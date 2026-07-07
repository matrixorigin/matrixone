/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>
#include <vector>
#include <sstream>

namespace matrixone {

// -------------------------------------------------------------------------
// Lightweight JSON helpers — no external library, nvcc-safe.
// -------------------------------------------------------------------------

// Extract the raw value string for a top-level key.
// Handles both quoted strings and bare scalars (numbers, booleans, null).
inline std::string json_value(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos);
    if (pos == std::string::npos) return "";
    ++pos;
    while (pos < json.size() &&
           (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r'))
        ++pos;
    if (pos >= json.size()) return "";
    if (json[pos] == '"') {
        ++pos;
        size_t end = json.find('"', pos);
        if (end == std::string::npos) return "";
        return json.substr(pos, end - pos);
    }
    size_t end = pos;
    while (end < json.size() &&
           json[end] != ',' && json[end] != '}' && json[end] != '\n' && json[end] != '\r')
        ++end;
    std::string val = json.substr(pos, end - pos);
    while (!val.empty() && (val.back() == ' ' || val.back() == '\t')) val.pop_back();
    return val;
}

inline int64_t json_int(const std::string& json, const std::string& key, int64_t def_val = 0) {
    std::string v = json_value(json, key);
    if (v.empty()) return def_val;
    try { return std::stoll(v); } catch (...) { return def_val; }
}

inline bool json_bool(const std::string& json, const std::string& key, bool def_val = false) {
    std::string v = json_value(json, key);
    if (v.empty()) return def_val;
    return v == "true";
}

// Extract a JSON sub-object `{...}` for a given key (brace-balanced).
inline std::string json_object(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "{}";
    pos = json.find('{', pos);
    if (pos == std::string::npos) return "{}";
    int depth = 0;
    for (size_t i = pos; i < json.size(); ++i) {
        if (json[i] == '{')       ++depth;
        else if (json[i] == '}') {
            if (--depth == 0) return json.substr(pos, i - pos + 1);
        }
    }
    return "{}";
}

// Extract a JSON array of integers for a given key.
inline std::vector<int> json_int_array(const std::string& json, const std::string& key) {
    std::vector<int> result;
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return result;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return result;
    size_t end = json.find(']', pos);
    if (end == std::string::npos) return result;
    std::istringstream ss(json.substr(pos + 1, end - pos - 1));
    std::string item;
    while (std::getline(ss, item, ',')) {
        while (!item.empty() && (item.front() == ' ' || item.front() == '\n' ||
                                 item.front() == '\t' || item.front() == '\r'))
            item = item.substr(1);
        while (!item.empty() && (item.back() == ' ' || item.back() == '\n' ||
                                 item.back() == '\t' || item.back() == '\r'))
            item.pop_back();
        if (!item.empty()) {
            try { result.push_back(std::stoi(item)); } catch (...) {}
        }
    }
    return result;
}

// Extract a JSON array of strings for a given key.
inline std::vector<std::string> json_string_array(const std::string& json, const std::string& key) {
    std::vector<std::string> result;
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return result;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return result;
    size_t end_bracket = json.find(']', pos);
    if (end_bracket == std::string::npos) return result;
    size_t i = pos + 1;
    while (i < end_bracket) {
        size_t q1 = json.find('"', i);
        if (q1 == std::string::npos || q1 >= end_bracket) break;
        size_t q2 = json.find('"', q1 + 1);
        if (q2 == std::string::npos || q2 >= end_bracket) break;
        result.push_back(json.substr(q1 + 1, q2 - q1 - 1));
        i = q2 + 1;
    }
    return result;
}

} // namespace matrixone
