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
#include <iostream>

#include "../protocol/kafka_primitives.hh"
#include "../protocol/metadata_request.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/api_versions_request.hh"
#include "../protocol/api_versions_response.hh"
#include "../connection/tcp_connection.hh"
#include "../protocol/produce_request.hh"
#include "../protocol/produce_response.hh"

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

#include <seastar/core/print.hh>
#include <seastar/kafka/producer/kafka_producer.hh>

namespace seastar {

namespace kafka {

kafka_producer::kafka_producer(std::string client_id) {
    _correlation_id = 0;
    _client_id = client_id;
}

seastar::future<> kafka_producer::init(std::string server_address, uint16_t port) {
    auto connection_future = tcp_connection::connect(server_address, port).then([this] (auto connection) {
        _connection = connection;
    });

    // TODO ApiVersions

    return connection_future.then([this] {
        return refresh_metadata();
    });
}

seastar::future<int32_t>
kafka_producer::send_request(int16_t api_key, int16_t api_version, const char *payload, size_t payload_length) {
    std::vector<char> header;
    boost::iostreams::back_insert_device<std::vector<char>> header_sink{header};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> header_stream{header_sink};

    auto assigned_correlation_id = _correlation_id++;

    kafka::kafka_int16_t api_key_kafka(api_key);
    api_key_kafka.serialize(header_stream, 0);
    kafka::kafka_int16_t api_version_kafka(api_version);
    api_version_kafka.serialize(header_stream, 0);
    kafka::kafka_int32_t correlation_id(assigned_correlation_id);
    correlation_id.serialize(header_stream, 0);
    kafka::kafka_nullable_string_t client_id(_client_id);
    client_id.serialize(header_stream, 0);

    header_stream.flush();

    auto message_size = header.size() + payload_length;

    std::vector<char> message;
    boost::iostreams::back_insert_device<std::vector<char>> message_sink{message};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> message_stream{message_sink};

    kafka_int32_t size(message_size);
    size.serialize(message_stream, 0);
    message_stream.write(header.data(), header.size());
    message_stream.write(payload, payload_length);
    message_stream.flush();

    temporary_buffer<char> message_buffer{message.data(), message.size()};
    return _connection->write(message_buffer.clone())
    .then([assigned_correlation_id] { return assigned_correlation_id; });
}

future<temporary_buffer<char>> kafka_producer::receive_response() {
    return _connection->read(4).then([] (temporary_buffer<char> response) {
        boost::iostreams::stream<boost::iostreams::array_source> response_size_stream
                (response.get(), response.size());

        kafka::kafka_int32_t response_size;
        response_size.deserialize(response_size_stream, 0);
        return *response_size;
    }).then([this] (int32_t response_size) {
        return _connection->read(response_size);
    });
}

seastar::future<> kafka_producer::produce(std::string topic_name, std::string key, std::string value, int32_t partition_index) {
    kafka::produce_request req;
    req._acks = -1;
    req._timeout_ms = 30000;

    kafka::produce_request_topic_produce_data topic_data;
    topic_data._name = topic_name;

    kafka::produce_request_partition_produce_data partition_data;
    partition_data._partition_index = partition_index;

    kafka::kafka_records records;
    kafka::kafka_record_batch record_batch;

    record_batch._base_offset = 0;
    record_batch._partition_leader_epoch = -1;
    record_batch._magic = 2;
    record_batch._compression_type = kafka::kafka_record_compression_type::NO_COMPRESSION;
    record_batch._timestamp_type = kafka::kafka_record_timestamp_type::CREATE_TIME;
    record_batch._first_timestamp = 0x16e5b6eba2c; // TODO it should be a real time
    record_batch._producer_id = -1;
    record_batch._producer_epoch = -1;
    record_batch._base_sequence = -1;
    record_batch._is_transactional = false;
    record_batch._is_control_batch = false;

    kafka::kafka_record record;
    record._timestamp_delta = 0;
    record._offset_delta = 0;
    record._key = key;
    record._value = value;

    record_batch._records.push_back(record);
    records._record_batches.push_back(record_batch);

    partition_data._records = records;

    kafka::kafka_array_t<kafka::produce_request_partition_produce_data> partitions{
            std::vector<kafka::produce_request_partition_produce_data>()};
    partitions->push_back(partition_data);
    topic_data._partitions = partitions;

    kafka::kafka_array_t<kafka::produce_request_topic_produce_data> topics{
            std::vector<kafka::produce_request_topic_produce_data>()};
    topics->push_back(topic_data);

    req._topics = topics;

    std::vector<char> serialized;
    boost::iostreams::back_insert_device<std::vector<char>> serialized_sink{serialized};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> serialized_stream{serialized_sink};

    req.serialize(serialized_stream, 7); // todo hardcoded version 7
    serialized_stream.flush();

    auto send_future = send_request(0, 7, serialized.data(), serialized.size()).discard_result();

    auto response_future = send_future.then([this] {
        return receive_response();
    }).then([] (temporary_buffer<char> response) {
        boost::iostreams::stream<boost::iostreams::array_source> response_stream
                (response.get(), response.size());

        kafka::kafka_int32_t correlation_id;
        correlation_id.deserialize(response_stream, 0);

        kafka::produce_response produce_response;
        produce_response.deserialize(response_stream, 7);

        fprint(std::cout, "response has error code of: %d\n",
               *produce_response._responses[0]._partitions[0]._error_code);

        fprint(std::cout, "response has base offset of: %ld\n",
               *produce_response._responses[0]._partitions[0]._base_offset);
    });

    return response_future;
}

seastar::future<> kafka_producer::refresh_metadata() {
    kafka::metadata_request req;

    req._allow_auto_topic_creation = true;
    req._include_cluster_authorized_operations = true;
    req._include_topic_authorized_operations = true;

    std::vector<char> serialized;
    boost::iostreams::back_insert_device<std::vector<char>> serialized_sink{serialized};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> serialized_stream{serialized_sink};
    req.serialize(serialized_stream, 8); // todo hardcoded version 8
    serialized_stream.flush();

    auto send_future = send_request(3, 8, serialized.data(), serialized.size()).discard_result();

    auto response_future = send_future.then([this] {
        return receive_response();
    }).then([] (temporary_buffer<char> response) {
        boost::iostreams::stream<boost::iostreams::array_source> response_stream
                (response.get(), response.size());

        kafka::kafka_int32_t correlation_id;
        correlation_id.deserialize(response_stream, 0);

        kafka::metadata_response metadata_response;
        metadata_response.deserialize(response_stream, 8);

        for (const auto& broker : *metadata_response._brokers) {
            fprint(std::cout, "Broker: %s:%d\n", broker._host->c_str(), *broker._port);
        }

        for (const auto& topic : *metadata_response._topics) {
            fprint(std::cout, "Topic: %s\n", topic._name->c_str());
        }
    });

    return response_future;
}

}

}
