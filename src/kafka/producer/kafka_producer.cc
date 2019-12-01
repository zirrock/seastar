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

#include <sstream>
#include <vector>

#include "../protocol/kafka_primitives.hh"
#include "../protocol/metadata_request.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/api_versions_request.hh"
#include "../protocol/api_versions_response.hh"
#include "../connection/tcp_connection.hh"

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

#include "kafka_producer.hh"

namespace seastar {

namespace kafka {

namespace producer {

char api_request_serialized_tmp[20] =
  "\x00\x00\x00\x0E\x00\x12\x00\x02\x00\x00\x00\x00\x00\x04\x74\x65\x73\x74";

char metadata_request_header_serialized_tmp[20] =
  "\x00\x00\x00\x00\x00\x03\x00\x02\x00\x00\x00\x01\x00\x04\x74\x65\x73\x74";

kafka_producer::kafka_producer(std::string client_id) {
  _correlation_id = 0;
  _client_id = client_id;
}

seastar::future<> kafka_producer::start(std::string server_address) {
  // establish connection
  //int16_t api_request_api = 2;
  //int16_t metadata_request_api = 2;
  future<lw_shared_ptr<tcp_connection> > connected =
    tcp_connection::connect(server_address);
  return connected.then([this](lw_shared_ptr<tcp_connection> conn) {
      _connection = conn;
      temporary_buffer<char> buff(
        api_request_serialized_tmp,
        sizeof(api_request_serialized_tmp));
      return conn->write(std::move(buff)).then([conn]() {
        return conn->read(4).then([conn](temporary_buffer<char> message_len) {
          return conn->read(std::stoi(std::string(message_len.get(), 4)))
            .then([conn](temporary_buffer<char> message){
            std::stringstream s;
            //int32_t correlation_id = std::stoi(std::string(message.get(), 4));
            api_versions_response api_response;
            s << (message.get() + 4);
            api_response.deserialize(s, 2); // TODO pass api
            // TODO save information
            metadata_request metadata_request;
            std::vector<char> new_message;
            boost::iostreams::back_insert_device<std::vector<char>> message_sink{new_message};
            boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> message_stream{message_sink};
            metadata_request_topic request_topic;
            kafka_string_t topic(kafka_string_t("Test"));
            request_topic.set_name(topic);
            kafka_array_t<metadata_request_topic> topics{std::vector<metadata_request_topic>()};
            topics->push_back(request_topic);
            metadata_request.set_topics(topics);
            metadata_request.set_allow_auto_topic_creation(kafka_bool_t(true));
            metadata_request.set_include_cluster_authorized_operations(kafka_bool_t(true));
            metadata_request.set_include_topic_authorized_operations(kafka_bool_t(true));
            message_stream.flush();
            int32_t request_size = sizeof(metadata_request_header_serialized_tmp + message.size());
            message_stream.write(reinterpret_cast<char*>(&request_size), 2);
            message_stream.write(metadata_request_header_serialized_tmp, sizeof(metadata_request_header_serialized_tmp));
            metadata_request.serialize(message_stream, 2);
            return conn->write(temporary_buffer<char> (new_message.data(), new_message.size()));
          });
        });
      });
    });
}

} // producer

} // kafka

} // seastar
