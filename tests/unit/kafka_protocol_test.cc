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
 * Copyright (C) 2019 ScyllaDB
 */

#define BOOST_TEST_MODULE kafka

#include <cstdint>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/test/included/unit_test.hpp>
#include "../../src/kafka/protocol/metadata_request.hh"
#include "../../src/kafka/protocol/metadata_response.hh"
#include "../../src/kafka/protocol/kafka_primitives.hh"
#include "../../src/kafka/protocol/api_versions_response.hh"

using namespace seastar;

BOOST_AUTO_TEST_CASE(kafka_primitives_number_test) {
    kafka::kafka_number_t<uint32_t> number(15);
    BOOST_REQUIRE_EQUAL(*number, 15);

    std::array<char, 4> data{0x12, 0x34, 0x56, 0x78};
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(data.begin(), data.end());

    number.deserialize(input_stream, 0);
    BOOST_REQUIRE_EQUAL(*number, 0x12345678);

    std::array<char, 4> output{};
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(output.begin(), output.end());
    number.serialize(output_stream, 0);
    BOOST_REQUIRE(!output_stream.bad());

    BOOST_TEST(output == data, boost::test_tools::per_element());

    std::array<char, 2> too_short_data{0x17, 0x27};
    boost::iostreams::stream<boost::iostreams::array_source> short_input_stream(too_short_data.begin(),
                                                                                too_short_data.end());

    BOOST_REQUIRE_THROW(number.deserialize(short_input_stream, 0), kafka::parsing_exception);

    BOOST_REQUIRE_EQUAL(*number, 0x12345678);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_string_test) {
    kafka::kafka_string_t string("321");
    BOOST_REQUIRE_EQUAL(*string, "321");

    std::array<char, 7> data{0, 5, 'a', 'b', 'c', 'd', 'e'};
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(data.begin(), data.end());

    string.deserialize(input_stream, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    std::array<char, 7> output{};
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(output.begin(), output.end());
    string.serialize(output_stream, 0);
    BOOST_REQUIRE(!output_stream.bad());

    BOOST_TEST(output == data, boost::test_tools::per_element());

    std::array<char, 5> too_short_data{0, 4, 'a', 'b', 'c'};
    boost::iostreams::stream<boost::iostreams::array_source> short_input_stream(too_short_data.begin(),
                                                                                too_short_data.end());

    BOOST_REQUIRE_THROW(string.deserialize(short_input_stream, 0), kafka::parsing_exception);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    std::array<char, 1> too_short_data2{0};
    boost::iostreams::stream<boost::iostreams::array_source> short_input_stream2(too_short_data2.begin(),
                                                                                 too_short_data2.end());

    BOOST_REQUIRE_THROW(string.deserialize(short_input_stream2, 0), kafka::parsing_exception);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_nullable_string_test) {
    kafka::kafka_nullable_string_t string;
    BOOST_REQUIRE(string.is_null());
    BOOST_REQUIRE_THROW((void) *string, std::exception);

    std::array<char, 7> data{0, 5, 'a', 'b', 'c', 'd', 'e'};
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(data.begin(), data.end());

    string.deserialize(input_stream, 0);
    BOOST_REQUIRE(!string.is_null());
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    std::array<char, 7> output{};
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(output.begin(), output.end());
    string.serialize(output_stream, 0);
    BOOST_REQUIRE(!output_stream.bad());

    BOOST_TEST(output == data, boost::test_tools::per_element());

    std::array<char, 2> null_data{-1, -1};
    boost::iostreams::stream<boost::iostreams::array_source> null_input_stream(null_data.begin(), null_data.end());
    string.deserialize(null_input_stream, 0);

    BOOST_REQUIRE(string.is_null());

    std::array<char, 2> null_output{};
    boost::iostreams::stream<boost::iostreams::array_sink> null_output_stream(null_output.begin(), null_output.end());
    string.serialize(null_output_stream, 0);
    BOOST_REQUIRE(!null_output_stream.bad());

    BOOST_TEST(null_output == null_data, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(kafka_primitives_bytes_test) {
    kafka::kafka_bytes_t bytes;

    std::array<char, 9> data{0, 0, 0, 5, 'a', 'b', 'c', 'd', 'e'};
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(data.begin(), data.end());

    bytes.deserialize(input_stream, 0);
    BOOST_REQUIRE_EQUAL(*bytes, "abcde");
    BOOST_REQUIRE_EQUAL(bytes->size(), 5);

    std::array<char, 9> output{};
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(output.begin(), output.end());
    bytes.serialize(output_stream, 0);

    BOOST_REQUIRE(!output_stream.bad());
    BOOST_TEST(output == data, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(kafka_primitives_array_test) {
    kafka::kafka_array_t<kafka::kafka_string_t> strings;

    std::array<char, 15> data{0, 0, 0, 2, 0, 5, 'a', 'b', 'c', 'd', 'e', 0, 2, 'f', 'g'};
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(data.begin(), data.end());

    strings.deserialize(input_stream, 0);
    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");

    std::array<char, 15> output{};
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(output.begin(), output.end());
    strings.serialize(output_stream, 0);

    BOOST_REQUIRE(!output_stream.bad());
    BOOST_TEST(output == data, boost::test_tools::per_element());

    std::array<char, 14> too_short_data{0, 0, 0, 2, 0, 5, 'A', 'B', 'C', 'D', 'E', 0, 2, 'F'};
    boost::iostreams::stream<boost::iostreams::array_source> short_input_stream(too_short_data.begin(),
                                                                                too_short_data.end());

    BOOST_REQUIRE_THROW(strings.deserialize(short_input_stream, 0), kafka::parsing_exception);
    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");
}

BOOST_AUTO_TEST_CASE(kafka_api_versions_reponse_deserialize_test) {
    std::array<char, 280> payload{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x0b, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x05,
            0x00, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x00, 0x0c,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x0f, 0x00, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x11, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x12, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x00, 0x00, 0x00, 0x03, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x15, 0x00, 0x00, 0x00, 0x01, 0x00, 0x16, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00, 0x18, 0x00, 0x00, 0x00, 0x01, 0x00, 0x19, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x1f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x21, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x22, 0x00, 0x00, 0x00, 0x01, 0x00, 0x23, 0x00, 0x00, 0x00, 0x01, 0x00, 0x24,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x25, 0x00, 0x00, 0x00, 0x01, 0x00, 0x26, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x28, 0x00, 0x00, 0x00, 0x01, 0x00, 0x29, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2c,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(payload.begin(), payload.end());
    kafka::api_versions_response response;
    response.deserialize(input_stream, 2);
    BOOST_REQUIRE_EQUAL(*response.get_throttle_time_ms(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_error_code(), 0);
    BOOST_REQUIRE_EQUAL(response.get_api_keys()->size(), 45);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[0].get_api_key(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[0].get_min_version(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[0].get_max_version(), 7);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[1].get_api_key(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[1].get_min_version(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_api_keys()[1].get_max_version(), 11);
}

BOOST_AUTO_TEST_CASE(kafka_metadata_request_deserialize_test) {
    std::array<char, 14> payload{
            0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x01, 0x00, 0x00
    };
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(payload.begin(), payload.end());
    kafka::metadata_request request;
    request.deserialize(input_stream, 8);

    BOOST_REQUIRE_EQUAL(request.get_topics()->size(), 1);
    BOOST_REQUIRE_EQUAL(*request.get_topics()[0].get_name(), "test5");
    BOOST_REQUIRE(*request.get_allow_auto_topic_creation());
    BOOST_REQUIRE(!*request.get_include_cluster_authorized_operations());
    BOOST_REQUIRE(!*request.get_include_topic_authorized_operations());
}

BOOST_AUTO_TEST_CASE(kafka_metadata_response_deserialize_test) {
    std::array<unsigned char, 118> payload{
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x0a, 0x31, 0x37,
            0x32, 0x2e, 0x31, 0x33, 0x2e, 0x30, 0x2e, 0x31, 0x00, 0x00, 0x23, 0x84, 0xff, 0xff, 0x00, 0x16,
            0x6b, 0x4c, 0x5a, 0x35, 0x6a, 0x50, 0x76, 0x44, 0x52, 0x30, 0x43, 0x77, 0x31, 0x79, 0x34, 0x31,
            0x41, 0x66, 0x35, 0x48, 0x55, 0x67, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(reinterpret_cast<char *>(payload.data()),
                                                                          payload.size());
    kafka::metadata_response response;
    response.deserialize(input_stream, 8);
    BOOST_REQUIRE_EQUAL(*response.get_throttle_time_ms(), 0);
    BOOST_REQUIRE_EQUAL(response.get_brokers()->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_brokers()[0].get_node_id(), 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.get_brokers()[0].get_host(), "172.13.0.1");
    BOOST_REQUIRE_EQUAL(*response.get_brokers()[0].get_port(), 0x2384);
    BOOST_REQUIRE(response.get_brokers()[0].get_rack().is_null());
    BOOST_REQUIRE_EQUAL(*response.get_cluster_id(), "kLZ5jPvDR0Cw1y41Af5HUg");
    BOOST_REQUIRE_EQUAL(*response.get_controller_id(), 0x3e9);
    BOOST_REQUIRE_EQUAL(response.get_topics()->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_error_code(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_name(), "test5");
    BOOST_REQUIRE(!*response.get_topics()[0].get_is_internal());
    BOOST_REQUIRE_EQUAL(response.get_topics()[0].get_partitions()->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_error_code(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_partition_index(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_leader_id(), 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_leader_epoch(), 0);
    BOOST_REQUIRE_EQUAL(response.get_topics()[0].get_partitions()[0].get_replica_nodes()->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_replica_nodes()[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(response.get_topics()[0].get_partitions()[0].get_isr_nodes()->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_partitions()[0].get_isr_nodes()[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.get_topics()[0].get_topic_authorized_operations(), 0);
    BOOST_REQUIRE_EQUAL(*response.get_cluster_authorized_operations(), 0);
}
