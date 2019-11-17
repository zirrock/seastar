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

#include "api_versions_response.hh"

namespace seastar {

namespace kafka {

const kafka_int16_t &api_versions_response_key::get_api_key() const {
    return _api_key;
}

const kafka_int16_t &api_versions_response_key::get_min_version() const {
    return _min_version;
}

const kafka_int16_t &api_versions_response_key::get_max_version() const {
    return _max_version;
}

void api_versions_response_key::serialize(std::ostream &os, int16_t api_version) const {
    _api_key.serialize(os, api_version);
    _min_version.serialize(os, api_version);
    _max_version.serialize(os, api_version);
}

void api_versions_response_key::deserialize(std::istream &is, int16_t api_version) {
    _api_key.deserialize(is, api_version);
    _min_version.deserialize(is, api_version);
    _max_version.deserialize(is, api_version);
}

const kafka_int16_t &api_versions_response::get_error_code() const {
    return _error_code;
}

const kafka_array_t<api_versions_response_key> &
api_versions_response::get_api_keys() const {
    return _api_keys;
}

const kafka_int32_t &api_versions_response::get_throttle_time_ms() const {
    return _throttle_time_ms;
}

void api_versions_response::serialize(std::ostream &os, int16_t api_version) const {
    _error_code.serialize(os, api_version);
    _api_keys.serialize(os, api_version);
    if (api_version >= 1) {
        _throttle_time_ms.serialize(os, api_version);
    }
}

void api_versions_response::deserialize(std::istream &is, int16_t api_version) {
    _error_code.deserialize(is, api_version);
    _api_keys.deserialize(is, api_version);
    if (api_version >= 1) {
        _throttle_time_ms.deserialize(is, api_version);
    }
}

}

}