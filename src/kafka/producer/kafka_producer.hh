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

#include <string>


#include "../connection/tcp_connection.hh"

#include <seastar/core/future.hh>
#include <seastar/net/net.hh>

namespace seastar {

namespace kafka {

class kafka_producer {
private:
    std::string _client_id;
    int32_t _correlation_id;
    connected_socket _socket;
    lw_shared_ptr<tcp_connection> _connection;

    seastar::future<int32_t> send_request(int16_t api_key, int16_t api_version,
            const char *payload, size_t payload_length);

    seastar::future<temporary_buffer<char>> receive_response();

    seastar::future<> refresh_metadata();

public:
    kafka_producer(std::string client_id);
    seastar::future<> init(std::string server_address, uint16_t port);
    seastar::future<> produce(std::string topic_name, std::string key, std::string value, int32_t partition_index);
};

}

}
