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

#include "../protocol/metadata_request.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/api_versions_request.hh"
#include "../protocol/api_versions_response.hh"
#include "../connection/tcp_connection.hh"

namespace seastar {

namespace kafka {

namespace producer {

kafka_producer::kafka_producer(string _client_id) {
  _correlation_id = 0;
}

seastar::future<> kafka_producer::start(server_address) {
  // establish connection
  api_request_api = 2;
  metadata_request_api = 2;
  header_length =
  char[18] api_request_serialized_tmp =
    "\x00\x00\x00\x0E\x00\x12\x00\x02\x00\x00\x00\x00\x00\x04\x74\x65\x73\x74";
  char[] metadata_request_header_serialized_tmp =
    "\x00\x00\x00\x00\x00\x03\x00\x02\x00\x00\x00\x01\x00\x04\x74\x65\x73\x74";
  api_versions_response api_response;

  connection::tcp_connection connection(server_address, &_socket);
  return connection.connect(server_address).then([]{
    temporary_buffer<char> buff(
      api_request_serilized_tmp,
      sizeof(api_request_serialized_tmp));
      return connection.write(&buff);
  }).then([] {
    return connection.read(4);
  }).then([temporary_buffer<char> message_len] {
    return connection.read(*static_cast<int32_t*>(message_len));
  })/*.then([temporary_buffer<char> header_message] {
    if (*static_cast<int32_t*>(header_message.get()) != _correlation_id) {
      // handle error
    }
    temporary_buffer<char> message = header_message.trim_front(sizeof(_correlation_id));
    api_versions_response response;
    response.deserialize(message, api_request_api);
    message.release();

  });*/

  // send metadata request
  // receive metedata response
}

} // producer

} // kafka

} // seastar
