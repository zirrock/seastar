#include <algorithm>
#include <iostream>
#include <queue>
#include <stdexcept>

#include "seastar/core/align.hh"
#include "seastar/core/app-template.hh"
#include "seastar/core/distributed.hh"
#include "seastar/core/fstream.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/util/conversions.hh"
#include "seastar/util/log.hh"

using namespace seastar;

static logger elogger("extsort");

static constexpr size_t merge_concurrency = 1; // 64
static constexpr size_t io_concurrency = 1; // 4

struct IoVecComp {
    static constexpr size_t length = 4096;

    bool operator()(const iovec &lhs, const iovec &rhs) const {
        return std::memcmp(lhs.iov_base, rhs.iov_base, length) < 0;
    }
};

struct HeapNode {
    static constexpr size_t length = IoVecComp::length;
    char *ptr;
    size_t file_id;

    bool operator<(const HeapNode& other) const {
        assert(ptr && other.ptr);
        return std::memcmp(ptr, other.ptr, length) < 0;
    }

    bool operator>(const HeapNode& other) const {
        assert(ptr && other.ptr);
        return std::memcmp(ptr, other.ptr, length) > 0;
    }
};

std::vector<iovec> sort_memory(char *mem, size_t size) {
    assert(size % IoVecComp::length == 0);
    std::vector<iovec> ret;
    ret.reserve(size / IoVecComp::length);
    for (char *ptr = mem; ptr != mem + size; ptr += IoVecComp::length) {
        ret.push_back({ptr, IoVecComp::length});
    }

    std::sort(ret.begin(), ret.end(), IoVecComp());
    return ret;
}

ssize_t get_file_size(const std::string& path) {
    struct stat stat_buf;
    int rc = stat(path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

class Splitter {
public:
    // _workdir      - working directory for output files
    // _path         - _path to input file
    // _file_size    - input file size
    // _max_mem_size - max memory to use at once for temporary buffers, in bytes
    Splitter(const sstring& workdir, const sstring& path, size_t file_size, size_t max_mem_size)
        : _workdir(workdir), _path(path), _file_size(file_size), _max_mem_size(max_mem_size) {
    }

    // Runs a splitting operation for single thread.
    // Expects to be run with invoke_on_all() on a distributed<Splitter>.
    future<> split() {
        _max_mem_size = std::min(_max_mem_size, _file_size) / io_concurrency;
        _part_size = align_up(_max_mem_size / smp::count, IoVecComp::length);
        size_t part_count = (_file_size / _part_size) + (_file_size % _part_size != 0);
        int cpu_id = engine().cpu_id();
        _parts_per_thread = part_count / smp::count;
        _parts_per_thread += (part_count % smp::count > 0) && (part_count % smp::count > unsigned(cpu_id));

        elogger.debug("Will open file");
        return open_file_dma(_path, open_flags::ro).then([this] (file f) {
            elogger.debug("opened file");
            auto begin_it = boost::counting_iterator<int>(0);
            auto end_it = boost::counting_iterator<int>(_parts_per_thread);
            auto read_descr = ::make_shared<file>(std::move(f));
            auto sem = ::make_shared<semaphore>(io_concurrency);
            return parallel_for_each(begin_it, end_it, [this, read_descr, sem] (int i) mutable {
                return with_semaphore(*sem, 1, [this, read_descr, i] {
                    auto read_descr_copy = ::make_shared<file>(read_descr->dup());
                    elogger.debug("inside-foreach: {}", i);
                    int block_offset = engine().cpu_id() + i * smp::count;
                    size_t to_read = std::min(_part_size, _file_size - block_offset * _part_size);
                    size_t read_offset = block_offset * _part_size;
                    elogger.debug("Will read");
                    return read_descr_copy->dma_read_exactly<char>(read_offset, to_read).then([this, block_offset] (temporary_buffer<char> buf) {
                        elogger.debug("Read {} bytes", buf.size());
                        _in_buf = std::make_unique<temporary_buffer<char>>(std::move(buf));
                        sstring out_path = get_output_path(block_offset);
                        return sort_and_write(out_path);
                    }).then([read_descr_copy] {
                        return read_descr_copy->close().then([read_descr_copy] {});
                    });
                });
            }).then([sem, read_descr] {
                return read_descr->close().then([read_descr] {});
            });
        });
    }

    future<> stop() {
        return make_ready_future<>();
    }

protected:
    inline future<> sort_and_write(const sstring& out_path) {
        elogger.debug("inside sort-and-write");
        open_flags flags = open_flags::rw | open_flags::create | open_flags::truncate;
        return open_file_dma(out_path, flags).then([this] (file f) mutable {
            auto write_descr = ::make_shared<file>(std::move(f));
            _sorted = sort_memory(_in_buf->get_write(), _in_buf->size());
            elogger.debug("Sorted!");
            auto begin_it = boost::counting_iterator<int>(0);
            auto end_it = boost::counting_iterator<int>((_sorted.size() + IOV_MAX - 1) / IOV_MAX);
            auto sem = ::make_shared<semaphore>(io_concurrency);
            return parallel_for_each(begin_it, end_it, [this, write_descr, sem] (int i) {
                return with_semaphore(*sem, 1, [this, write_descr, i] {
                    auto write_descr_copy = ::make_shared<file>(write_descr->dup());
                    size_t offset = i * IOV_MAX;
                    size_t to_write = std::min<size_t>(_sorted.size() - i * IOV_MAX, IOV_MAX);
                    auto batch = std::vector<iovec>(_sorted.begin() + offset, _sorted.begin() + offset + to_write);
                    return write_descr_copy->dma_write(offset * IoVecComp::length, std::move(batch)).then([write_descr_copy] (size_t) {
                        return write_descr_copy->close().then([write_descr_copy] {});
                    });
                });
            }).then([write_descr, sem] {
                return write_descr->close().then([write_descr] {});
            });
        });
    }

    // Returns a _path for output file for current thread id and given offset
    sstring get_output_path(size_t offset) {
        return _workdir + "/th" + std::to_string(engine().cpu_id()) + "/tmp." +
               std::to_string(offset);
    }

    sstring _workdir;
    sstring _path;
    size_t _file_size;
    size_t _max_mem_size;
    size_t _part_size = 0;
    size_t _parts_per_thread = 0;
    std::unique_ptr<temporary_buffer<char>> _in_buf;
    std::vector<iovec> _sorted;
};

class Merger {
public:
    // Helper structure representing one file part in the middle of being processed:
    // file_id          - file number
    // in_file_offset   - number of bytes processed from the input file
    // in_buffer offset - number of bytes processed in current temporary buffer
    // buf - temporary buffer, contains the part of input file that starts with in_file_offset
    struct MergeEntry {
        MergeEntry(size_t file_id)
        : file_id(file_id), in_file_offset(0), in_buffer_offset(0), buf() {}

        size_t file_id;
        size_t in_file_offset;
        size_t in_buffer_offset;
        temporary_buffer<char> buf;
    };

    Merger() {}
    Merger(const sstring &workdir, const sstring &out_name, size_t file_size, size_t max_mem_size)
    : workdir(workdir), out_name(out_name), file_size(file_size), max_mem_size(max_mem_size) {}

    // Starts a merge operation - first part of each input file is read into memory,
    // in order to be merged.
    future<> merge() {
        elogger.debug("Merging");
        max_mem_size = std::min<uint64_t>(std::min(max_mem_size, file_size) / io_concurrency, 1 * 1024 * 1024 * 1024);
        part_size = align_up(max_mem_size / smp::count, HeapNode::length);
        part_count = (file_size / part_size) + (file_size % part_size != 0);
        mem_per_part = align_up(max_mem_size / part_count, HeapNode::length);
        _entries.reserve(part_count);

        for (unsigned i = 0; i < part_count; ++i) {
            _entries.emplace_back(i);
        }

        return do_for_each(_entries.begin(), _entries.end(),
            [this] (MergeEntry &entry) mutable {
            sstring path = get_input_path(entry.file_id);
            return open_file_dma(path, open_flags::ro).then([this, &entry](file f) {
                _read_descr = std::make_unique<file>(std::move(f));
                size_t to_read = count_bytes_to_read(0, entry.file_id);
                return _read_descr->dma_read_exactly<char>(0, to_read)
                .then([this, &entry](temporary_buffer<char> buf) {
                    entry.buf = std::move(buf);
                });
            }).then([this] () {
                return _read_descr->close();
            });
        })
    .then([this]() {
        return init_min_queue();
    }).then([this] {
        elogger.debug("Return pending flush");
        return _pending_flush.then([this] {
            return _write_descr->close();
        });
    });
    }

protected:
    // Returns input file path for given file number
    sstring get_input_path(size_t file_id) const {
        return workdir + "/th" + std::to_string(file_id % smp::count) + "/tmp." +
               std::to_string(file_id);
    }

    // Returns the number of bytes that should be read from given offset and file
    size_t count_bytes_to_read(size_t start_offset, size_t file_id) const {
        size_t exact_part_size = part_size;
        // Special case for last file, which might be smaller than the rest
        if (file_id == _entries.size() - 1 && file_size % part_size > 0) {
            exact_part_size = file_size % part_size;
        }
        assert(start_offset <= exact_part_size);
        size_t to_read = std::min(mem_per_part, exact_part_size - start_offset);

        return to_read;
    }

    // Initializes min heap (a.k.a. priority queue), used to pick smallest elements
    // from currently loaded files.
    future<> init_min_queue() {
        for (MergeEntry &entry : _entries) {
            _min_queue.push({entry.buf.get_write(), entry.file_id});
            entry.in_buffer_offset += HeapNode::length;
        }
        _out_offset = 0;
        open_flags flags = open_flags::rw | open_flags::create | open_flags::truncate;
        sstring out_path = workdir + "/" + out_name;
        return open_file_dma(out_path, flags).then([this](file f) {
            _write_descr = std::make_unique<file>(std::move(f));
            return merge_some(std::vector<iovec>());
        });
    }

    // Performs a partial merge. Runs until:
    // a) whole file is processed
    // b) output file needs to be written (dma iovector is full)
    // c) next part of a file needs to be loaded
    future<> merge_some(std::vector<iovec>&& out_buf) {
        while (true) {
            if (_min_queue.empty()) {
                if (out_buf.empty()) {
                    return make_ready_future<>();
                }
                return flush_out_buf(std::move(out_buf));
            }
            if (out_buf.size() == IOV_MAX) {
                return flush_out_buf(std::move(out_buf)).then([this] {
                    return merge_some(std::vector<iovec>());
                });
            }
            HeapNode node = _min_queue.top();
            _min_queue.pop();
            out_buf.push_back({node.ptr, HeapNode::length});
            MergeEntry &entry = _entries[node.file_id];
            if (entry.in_buffer_offset >= entry.buf.size()) {
                sstring path = get_input_path(entry.file_id);
                size_t entry_buf_size = entry.buf.size();
                return flush_out_buf(std::move(out_buf), std::move(entry.buf)).then([this, entry_buf_size, &entry, path] {
                    return open_file_dma(path, open_flags::ro).then([this, entry_buf_size, &entry, path] (file f) {
                        size_t to_read = count_bytes_to_read(entry.in_file_offset + entry.buf.size(), entry.file_id);
                        if (to_read == 0) {
                            return merge_some(std::vector<iovec>());
                        }
                        _read_descr = std::make_unique<file>(std::move(f));
                        return _read_descr->dma_read_exactly<char>(entry.in_file_offset + entry.buf.size(), to_read).then([this, entry_buf_size, &entry] (temporary_buffer<char> buf) {
                            entry.in_file_offset += entry_buf_size;
                            entry.buf = std::move(buf);
                            _min_queue.push({entry.buf.get_write(), entry.file_id});
                            entry.in_buffer_offset = HeapNode::length;
                            return merge_some(std::vector<iovec>());
                        });
                    });
                });
            }
            _min_queue.push({entry.buf.get_write() + entry.in_buffer_offset, entry.file_id});
            entry.in_buffer_offset += HeapNode::length;
        }
    }

    future<> flush_out_buf(std::vector<iovec>&& out_buf, temporary_buffer<char>&& keep_alive = temporary_buffer<char>()) {
        return get_units(_write_sem, 1).then([this, out_buf = std::move(out_buf), keep_alive = std::move(keep_alive)] (auto sem_units) mutable {
            //elogger.debug("Enqueueing flush. Sem {}", _write_sem.current());
            _pending_flush = _pending_flush.then([this, out_buf = std::move(out_buf), keep_alive = std::move(keep_alive), sem_units = std::move(sem_units)]() mutable {
                return _write_descr->dma_write(_out_offset, std::move(out_buf)).then([this, keep_alive = std::move(keep_alive), sem_units = std::move(sem_units)] (size_t bytes_written) {
                    //elogger.debug("Flushed {}", _out_offset);
                    _out_offset += bytes_written;
                    return make_ready_future<>();
                });
            });
            return make_ready_future<>();
        });
    }

    sstring workdir;
    sstring out_name;
    size_t file_size;
    size_t part_size;
    size_t part_count;
    size_t mem_per_part;
    size_t max_mem_size;
    std::unique_ptr<file> _read_descr;
    std::unique_ptr<file> _write_descr;
    std::vector<MergeEntry> _entries;
    size_t _out_offset;
    std::priority_queue<HeapNode,
                        std::vector<HeapNode>,
                        std::greater<HeapNode>> _min_queue;
    future<> _pending_flush = make_ready_future<>();
    //FIXME: bad file descriptor for some reason
    semaphore _write_sem{merge_concurrency};
};

int main(int argc, char **argv) {
    app_template app;
    namespace bpo = boost::program_options;
    app.add_options()("max-split-mem", bpo::value<uint64_t>()->default_value(1024),
                      "max memory to use during split phase in Mebibytes (ex. 1024)");
    app.add_options()("input-file", bpo::value<sstring>()->default_value(""), "_path to input file");
    app.add_options()("working-dir", bpo::value<std::string>()->default_value("."),
                      "working directory for intermediate files");
    app.add_options()("out-name", bpo::value<sstring>()->default_value("merged.bin"),
                      "output file name to be created in working directory");
    static distributed<Splitter> splitter;
    static Merger merger;

    try {
        app.run(argc, argv, [&app] {
            engine().at_exit([&] { return splitter.stop(); });
            size_t max_split_mem = app.configuration()["max-split-mem"].as<size_t>() * 1024 * 1024;
            sstring path = app.configuration()["input-file"].as<sstring>();
            std::string workdir = app.configuration()["working-dir"].as<std::string>() + "/";
            sstring out_name = app.configuration()["out-name"].as<sstring>();
            if (path.empty()) {
                std::cerr << "Path cannot be empty" << std::endl;
                return make_ready_future<>();
            }
            size_t file_size = get_file_size(path);

            for (unsigned i = 0; i < smp::count; ++i) {
                std::string subdir_path = workdir + "/th" + std::to_string(i);
                int rc = mkdir(subdir_path.c_str(), 0755);
                if (rc < 0 && errno != EEXIST) {
                    std::cerr << "Cannot create working subdirectory " << subdir_path << std::endl;
                    return make_ready_future<>();
                }
            }

            return splitter.start(workdir, path, file_size, max_split_mem).then([] {
                return splitter.invoke_on_all(&Splitter::split);
            })
            .then([file_size, max_split_mem, out_name, workdir] {
                merger = Merger(workdir, out_name, file_size, max_split_mem);
                return merger.merge();
            });
        });
    } catch (...) {
        std::cerr << "Failed to start application: " << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
