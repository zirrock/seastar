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

#include "metadata_response.hh"

namespace seastar {

namespace kafka {

const kafka_int32_t &metadata_response_broker::get_node_id() const {
    return _node_id;
}

const kafka_string_t &metadata_response_broker::get_host() const {
    return _host;
}

const kafka_int32_t &metadata_response_broker::get_port() const {
    return _port;
}

const kafka_nullable_string_t &metadata_response_broker::get_rack() const {
    return _rack;
}

void metadata_response_broker::serialize(std::ostream &os, int16_t api_version) const {
    _node_id.serialize(os, api_version);
    _host.serialize(os, api_version);
    _port.serialize(os, api_version);
    if (api_version >= 1) {
        _rack.serialize(os, api_version);
    }
}

void metadata_response_broker::deserialize(std::istream &is, int16_t api_version) {
    _node_id.deserialize(is, api_version);
    _host.deserialize(is, api_version);
    _port.deserialize(is, api_version);
    if (api_version >= 1) {
        _rack.deserialize(is, api_version);
    }
}

const kafka_int16_t &metadata_response_partition::get_error_code() const {
    return _error_code;
}

const kafka_int32_t &metadata_response_partition::get_partition_index() const {
    return _partition_index;
}

const kafka_int32_t &metadata_response_partition::get_leader_id() const {
    return _leader_id;
}

const kafka_int32_t &metadata_response_partition::get_leader_epoch() const {
    return _leader_epoch;
}

const kafka_array_t<kafka_int32_t> &metadata_response_partition::get_replica_nodes() const {
    return _replica_nodes;
}

const kafka_array_t<kafka_int32_t> &metadata_response_partition::get_isr_nodes() const {
    return _isr_nodes;
}

const kafka_array_t<kafka_int32_t> &metadata_response_partition::get_offline_replicas() const {
    return _offline_replicas;
}

void metadata_response_partition::serialize(std::ostream &os, int16_t api_version) const {
    _error_code.serialize(os, api_version);
    _partition_index.serialize(os, api_version);
    _leader_id.serialize(os, api_version);
    if (api_version >= 7) {
        _leader_epoch.serialize(os, api_version);
    }
    _replica_nodes.serialize(os, api_version);
    _isr_nodes.serialize(os, api_version);
    if (api_version >= 5) {
        _offline_replicas.serialize(os, api_version);
    }
}

void metadata_response_partition::deserialize(std::istream &is, int16_t api_version) {
    _error_code.deserialize(is, api_version);
    _partition_index.deserialize(is, api_version);
    _leader_id.deserialize(is, api_version);
    if (api_version >= 7) {
        _leader_epoch.deserialize(is, api_version);
    }
    _replica_nodes.deserialize(is, api_version);
    _isr_nodes.deserialize(is, api_version);
    if (api_version >= 5) {
        _offline_replicas.deserialize(is, api_version);
    }
}

const kafka_int16_t &metadata_response_topic::get_error_code() const {
    return _error_code;
}

const kafka_string_t &metadata_response_topic::get_name() const {
    return _name;
}

const kafka_bool_t &metadata_response_topic::get_is_internal() const {
    return _is_internal;
}

const kafka_array_t<metadata_response_partition> &metadata_response_topic::get_partitions() const {
    return _partitions;
}

const kafka_int32_t &metadata_response_topic::get_topic_authorized_operations() const {
    return _topic_authorized_operations;
}

void metadata_response_topic::serialize(std::ostream &os, int16_t api_version) const {
    _error_code.serialize(os, api_version);
    _name.serialize(os, api_version);
    if (api_version >= 1) {
        _is_internal.serialize(os, api_version);
    }
    _partitions.serialize(os, api_version);
    if (api_version >= 8) {
        _topic_authorized_operations.serialize(os, api_version);
    }
}

void metadata_response_topic::deserialize(std::istream &is, int16_t api_version) {
    _error_code.deserialize(is, api_version);
    _name.deserialize(is, api_version);
    if (api_version >= 1) {
        _is_internal.deserialize(is, api_version);
    }
    _partitions.deserialize(is, api_version);
    if (api_version >= 8) {
        _topic_authorized_operations.deserialize(is, api_version);
    }
}

const kafka_int32_t &metadata_response::get_throttle_time_ms() const {
    return _throttle_time_ms;
}

const kafka_array_t<metadata_response_broker> &metadata_response::get_brokers() const {
    return _brokers;
}

const kafka_nullable_string_t &metadata_response::get_cluster_id() const {
    return _cluster_id;
}

const kafka_int32_t &metadata_response::get_controller_id() const {
    return _controller_id;
}

const kafka_array_t<metadata_response_topic> &metadata_response::get_topics() const {
    return _topics;
}

const kafka_int32_t &metadata_response::get_cluster_authorized_operations() const {
    return _cluster_authorized_operations;
}

void metadata_response::serialize(std::ostream &os, int16_t api_version) const {
    if (api_version >= 3) {
        _throttle_time_ms.serialize(os, api_version);
    }
    _brokers.serialize(os, api_version);
    if (api_version >= 2) {
        _cluster_id.serialize(os, api_version);
    }
    if (api_version >= 1) {
        _controller_id.serialize(os, api_version);
    }
    _topics.serialize(os, api_version);
    if (api_version >= 8) {
        _cluster_authorized_operations.serialize(os, api_version);
    }
}

void metadata_response::deserialize(std::istream &is, int16_t api_version) {
    if (api_version >= 3) {
        _throttle_time_ms.deserialize(is, api_version);
    }
    _brokers.deserialize(is, api_version);
    if (api_version >= 2) {
        _cluster_id.deserialize(is, api_version);
    }
    if (api_version >= 1) {
        _controller_id.deserialize(is, api_version);
    }
    _topics.deserialize(is, api_version);
    if (api_version >= 8) {
        _cluster_authorized_operations.deserialize(is, api_version);
    }
}

}

}