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

#include <iostream>
#include "tcp_connection.hh"

namespace seastar {

namespace kafka {

future<lw_shared_ptr<tcp_connection>> tcp_connection::connect(const std::string& address) {
    ipv4_addr target_addr = ipv4_addr{address};
    socket_address socket = socket_address(::sockaddr_in{AF_INET, INADDR_ANY, {0}});
    return engine().net().connect(make_ipv4_address(target_addr), socket, transport::TCP).then(
            [target_addr = std::move(target_addr)] (connected_socket fd) {
                return make_lw_shared<tcp_connection>(target_addr, std::move(fd));
            }
    );
}

future<temporary_buffer<char>> tcp_connection::read(size_t bytes) {
    return _read_buf.read_exactly(bytes);
}

future<> tcp_connection::write(temporary_buffer<char> buff) {
    return _write_buf.write(std::move(buff)).then([this] {
        return _write_buf.flush();
    });
}

future<> tcp_connection::close() {
    return _read_buf.close().then([this] {
        return _write_buf.close();
    });
}

}

}