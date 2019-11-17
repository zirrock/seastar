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

class metadata_response_broker {
private:
    kafka_int32_t _node_id;
    kafka_string_t _host;
    kafka_int32_t _port;
    kafka_nullable_string_t _rack;

public:
    [[nodiscard]] const kafka_int32_t &get_node_id() const;

    [[nodiscard]] const kafka_string_t &get_host() const;

    [[nodiscard]] const kafka_int32_t &get_port() const;

    [[nodiscard]] const kafka_nullable_string_t &get_rack() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class metadata_response_partition {
private:
    kafka_int16_t _error_code;
    kafka_int32_t _partition_index;
    kafka_int32_t _leader_id;
    kafka_int32_t _leader_epoch;
    kafka_array_t<kafka_int32_t> _replica_nodes;
    kafka_array_t<kafka_int32_t> _isr_nodes;
    kafka_array_t<kafka_int32_t> _offline_replicas;
public:
    [[nodiscard]] const kafka_int16_t &get_error_code() const;

    [[nodiscard]] const kafka_int32_t &get_partition_index() const;

    [[nodiscard]] const kafka_int32_t &get_leader_id() const;

    [[nodiscard]] const kafka_int32_t &get_leader_epoch() const;

    [[nodiscard]] const kafka_array_t<kafka_int32_t> &get_replica_nodes() const;

    [[nodiscard]] const kafka_array_t<kafka_int32_t> &get_isr_nodes() const;

    [[nodiscard]] const kafka_array_t<kafka_int32_t> &get_offline_replicas() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class metadata_response_topic {
private:
    kafka_int16_t _error_code;
    kafka_string_t _name;
    kafka_bool_t _is_internal;
    kafka_array_t<metadata_response_partition> _partitions;
    kafka_int32_t _topic_authorized_operations;
public:
    [[nodiscard]] const kafka_int16_t &get_error_code() const;

    [[nodiscard]] const kafka_string_t &get_name() const;

    [[nodiscard]] const kafka_bool_t &get_is_internal() const;

    [[nodiscard]] const kafka_array_t<metadata_response_partition> &get_partitions() const;

    [[nodiscard]] const kafka_int32_t &get_topic_authorized_operations() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class metadata_response {
private:
    kafka_int32_t _throttle_time_ms;
    kafka_array_t<metadata_response_broker> _brokers;
    kafka_nullable_string_t _cluster_id;
    kafka_int32_t _controller_id;
    kafka_array_t<metadata_response_topic> _topics;
    kafka_int32_t _cluster_authorized_operations;

public:
    [[nodiscard]] const kafka_int32_t &get_throttle_time_ms() const;

    [[nodiscard]] const kafka_array_t<metadata_response_broker> &get_brokers() const;

    [[nodiscard]] const kafka_nullable_string_t &get_cluster_id() const;

    [[nodiscard]] const kafka_int32_t &get_controller_id() const;

    [[nodiscard]] const kafka_array_t<metadata_response_topic> &get_topics() const;

    [[nodiscard]] const kafka_int32_t &get_cluster_authorized_operations() const;

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

}

}