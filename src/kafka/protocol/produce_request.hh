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

#include "kafka_primitives.hh"
#include "kafka_records.hh"

namespace seastar {

namespace kafka {

class produce_request_partition_produce_data {
private:
    kafka_int32_t _partition_index;
    kafka_records _records;

public:
    [[nodiscard]] const kafka_int32_t &get_partition_index() const;

    void set_partition_index(const kafka_int32_t &partition_index);

    [[nodiscard]] const kafka_records &get_records() const;

    void set_records(const kafka_records &records);

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class produce_request_topic_produce_data {
private:
    kafka_string_t _name;
    kafka_array_t<produce_request_partition_produce_data> _partitions;

public:
    [[nodiscard]] const kafka_string_t &get_name() const;

    void set_name(const kafka_string_t &name);

    [[nodiscard]] const kafka_array_t<produce_request_partition_produce_data> &get_partitions() const;

    void set_partitions(const kafka_array_t<produce_request_partition_produce_data> &partitions);

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class produce_request {
private:
    kafka_nullable_string_t _transactional_id;
    kafka_int16_t _acks;
    kafka_int32_t _timeout_ms;
    kafka_array_t<produce_request_topic_produce_data> _topics;

public:
    [[nodiscard]] const kafka_nullable_string_t &get_transactional_id() const;

    void set_transactional_id(const kafka_nullable_string_t &transactional_id);

    [[nodiscard]] const kafka_int16_t &get_acks() const;

    void set_acks(const kafka_int16_t &acks);

    [[nodiscard]] const kafka_int32_t &get_timeout_ms() const;

    void set_timeout_ms(const kafka_int32_t &timeout_ms);

    [[nodiscard]] const kafka_array_t<produce_request_topic_produce_data> &get_topics() const;

    void set_topics(const kafka_array_t<produce_request_topic_produce_data> &topics);

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

}

}