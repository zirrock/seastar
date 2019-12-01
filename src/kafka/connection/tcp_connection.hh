/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/net/net.hh>
#include <string>

namespace seastar {

namespace kafka
{

class tcp_connection
{

public:

    tcp_connection(const ipv4_addr& addr, connected_socket&& fd) noexcept
            : _target_addr(addr)
            , _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output()) {};

    tcp_connection(tcp_connection&& other) noexcept
        : _target_addr(other._target_addr)
        , _fd(std::move(other._fd))
        , _read_buf(std::move(other._read_buf))
        , _write_buf(std::move(other._write_buf)) {};

    static future<lw_shared_ptr<tcp_connection>> connect(const std::string& address);

    future<> write(temporary_buffer<char> buff);
    future<temporary_buffer<char>> read(size_t bytes);
    future<> close();

private:

    ipv4_addr _target_addr;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;

};

}

}
