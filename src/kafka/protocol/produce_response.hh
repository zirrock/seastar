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

namespace seastar {

namespace kafka {

class produce_response_batch_index_and_error_message {
private:
    kafka_int32_t _batch_index;
    kafka_nullable_string_t _batch_index_error_message;
public:
    [[nodiscard]] const kafka_int32_t &get_batch_index() const;

    [[nodiscard]] const kafka_nullable_string_t &get_batch_index_error_message() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class produce_response_partition_produce_response {
private:
    kafka_int32_t _partition_index;
    kafka_int16_t _error_code;
    kafka_int64_t _base_offset;
    kafka_int64_t _log_append_time_ms;
    kafka_int64_t _log_start_offset;
    kafka_array_t<produce_response_batch_index_and_error_message> _record_errors;
    kafka_nullable_string_t _error_message;

public:
    [[nodiscard]] const kafka_int32_t &get_partition_index() const;

    [[nodiscard]] const kafka_int16_t &get_error_code() const;

    [[nodiscard]] const kafka_int64_t &get_base_offset() const;

    [[nodiscard]] const kafka_int64_t &get_log_append_time_ms() const;

    [[nodiscard]] const kafka_int64_t &get_log_start_offset() const;

    [[nodiscard]] const kafka_array_t<produce_response_batch_index_and_error_message> &get_record_errors() const;

    [[nodiscard]] const kafka_nullable_string_t &get_error_message() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class produce_response_topic_produce_response {
private:
    kafka_string_t _name;
    kafka_array_t<produce_response_partition_produce_response> _partitions;

public:
    [[nodiscard]] const kafka_string_t &get_name() const;

    [[nodiscard]] const kafka_array_t<produce_response_partition_produce_response> &get_partitions() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class produce_response {
private:
    kafka_array_t<produce_response_topic_produce_response> _responses;
    kafka_int32_t _throttle_time_ms;
public:
    [[nodiscard]] const kafka_array_t<produce_response_topic_produce_response> &get_responses() const;

    [[nodiscard]] const kafka_int32_t &get_throttle_time_ms() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

}

}
