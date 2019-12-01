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

class metadata_request_topic {
private:
    kafka_string_t _name;
public:
    [[nodiscard]] const kafka_string_t &get_name() const;

    void set_name(const kafka_string_t &name);

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

class metadata_request {
private:
    kafka_array_t<metadata_request_topic> _topics;
    kafka_bool_t _allow_auto_topic_creation;
    kafka_bool_t _include_cluster_authorized_operations;
    kafka_bool_t _include_topic_authorized_operations;
public:
    [[nodiscard]] const kafka_array_t<metadata_request_topic> &get_topics() const;

    void set_topics(const kafka_array_t<metadata_request_topic> &topics);

    [[nodiscard]] const kafka_bool_t &get_allow_auto_topic_creation() const;

    void set_allow_auto_topic_creation(const kafka_bool_t &allowAutoTopicCreation);

    [[nodiscard]] const kafka_bool_t &get_include_cluster_authorized_operations() const;

    void set_include_cluster_authorized_operations(const kafka_bool_t &includeClusterAuthorizedOperations);

    [[nodiscard]] const kafka_bool_t &get_include_topic_authorized_operations() const;

    void _set_include_topic_authorized_operations(const kafka_bool_t &includeTopicAuthorizedOperations);

    void serialize(std::ostream &os, int16_t api_version) const;

    void deserialize(std::istream &is, int16_t api_version);
};

}

}