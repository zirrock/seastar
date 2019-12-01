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

#include "produce_request.hh"

namespace seastar {

namespace kafka {

const kafka_int32_t &produce_request_partition_produce_data::get_partition_index() const {
    return _partition_index;
}

void produce_request_partition_produce_data::set_partition_index(const kafka_int32_t &partition_index) {
    _partition_index = partition_index;
}

const kafka_records &produce_request_partition_produce_data::get_records() const {
    return _records;
}

void produce_request_partition_produce_data::set_records(const kafka_records &records) {
    _records = records;
}

void produce_request_partition_produce_data::serialize(std::ostream &os, int16_t api_version) const {
    _partition_index.serialize(os, api_version);
    _records.serialize(os, api_version);
}

void produce_request_partition_produce_data::deserialize(std::istream &is, int16_t api_version) {
    _partition_index.deserialize(is, api_version);
    _records.deserialize(is, api_version);
}

const kafka_string_t &produce_request_topic_produce_data::get_name() const {
    return _name;
}

void produce_request_topic_produce_data::set_name(const kafka_string_t &name) {
    _name = name;
}

const kafka_array_t<produce_request_partition_produce_data> &
produce_request_topic_produce_data::get_partitions() const {
    return _partitions;
}

void produce_request_topic_produce_data::set_partitions(
        const kafka_array_t<produce_request_partition_produce_data> &partitions) {
    _partitions = partitions;
}

void produce_request_topic_produce_data::serialize(std::ostream &os, int16_t api_version) const {
    _name.serialize(os, api_version);
    _partitions.serialize(os, api_version);
}

void produce_request_topic_produce_data::deserialize(std::istream &is, int16_t api_version) {
    _name.deserialize(is, api_version);
    _partitions.deserialize(is, api_version);
}

const kafka_nullable_string_t &produce_request::get_transactional_id() const {
    return _transactional_id;
}

void produce_request::set_transactional_id(const kafka_nullable_string_t &transactional_id) {
    _transactional_id = transactional_id;
}

const kafka_int16_t &produce_request::get_acks() const {
    return _acks;
}

void produce_request::set_acks(const kafka_int16_t &acks) {
    _acks = acks;
}

const kafka_int32_t &produce_request::get_timeout_ms() const {
    return _timeout_ms;
}

void produce_request::set_timeout_ms(const kafka_int32_t &timeout_ms) {
    _timeout_ms = timeout_ms;
}

const kafka_array_t<produce_request_topic_produce_data> &produce_request::get_topics() const {
    return _topics;
}

void produce_request::set_topics(const kafka_array_t<produce_request_topic_produce_data> &topics) {
    _topics = topics;
}

void produce_request::serialize(std::ostream &os, int16_t api_version) const {
    if (api_version >= 3) {
        _transactional_id.serialize(os, api_version);
    }
    _acks.serialize(os, api_version);
    _timeout_ms.serialize(os, api_version);
    _topics.serialize(os, api_version);
}

void produce_request::deserialize(std::istream &is, int16_t api_version) {
    if (api_version >= 3) {
        _transactional_id.deserialize(is, api_version);
    }
    _acks.deserialize(is, api_version);
    _timeout_ms.deserialize(is, api_version);
    _topics.deserialize(is, api_version);
}

}

}
