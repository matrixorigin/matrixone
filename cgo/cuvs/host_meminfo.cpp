// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "host_meminfo.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace matrixone {

// Outcome of the cgroup headroom walk. "no limit anywhere" and "a limit
// governs us but its usage is unreadable" both fail the walk, and they need
// opposite fallbacks -- the first should consult the node-wide figure, the
// second must not.
enum class headroom_status { no_limit, ok, unreadable };

namespace {

// Whitespace-delimited fields, the equivalent of Go's strings.Fields.
std::vector<std::string> fields(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream in(s);
    std::string tok;
    while (in >> tok) out.push_back(tok);
    return out;
}

std::string trim(const std::string& s) {
    const char* ws = " \t\n\r\f\v";
    const auto b = s.find_first_not_of(ws);
    if (b == std::string::npos) return std::string();
    const auto e = s.find_last_not_of(ws);
    return s.substr(b, e - b + 1);
}

bool read_file(const std::string& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::ostringstream buf;
    buf << in.rdbuf();
    if (!in && !in.eof()) return false;
    *out = buf.str();
    return true;
}

bool parse_uint(const std::string& s, uint64_t* out) {
    if (s.empty()) return false;
    errno = 0;
    char*              end = nullptr;
    unsigned long long v   = std::strtoull(s.c_str(), &end, 10);
    // Reject trailing junk and overflow. strtoull accepts a leading '-' and
    // wraps it, so a negative reading would otherwise arrive as a colossal
    // limit -- and a colossal limit is precisely the reading that would admit
    // a build this governor exists to refuse.
    if (errno != 0 || end == s.c_str() || *end != '\0' || s[0] == '-') return false;
    *out = static_cast<uint64_t>(v);
    return true;
}

// The cgroup directory for a process path, given one mountinfo entry. Empty
// when the process path is not under this mount's root.
bool cgroup_directory(const std::string& mount_point, const std::string& mount_root,
                      const std::string& process_path, std::string* out) {
    const std::filesystem::path root(std::filesystem::path(mount_root).lexically_normal());
    const std::filesystem::path proc(std::filesystem::path(process_path).lexically_normal());
    // Lexical, like Go's filepath.Rel: these paths are namespace-relative and
    // need not exist in this process's view of the filesystem.
    const std::filesystem::path rel = proc.lexically_relative(root);
    const std::string           r   = rel.generic_string();
    if (r.empty() || r == ".." || r.rfind("../", 0) == 0) return false;
    *out = (std::filesystem::path(mount_point) / rel).lexically_normal().generic_string();
    return true;
}

// Walks /proc/<pid>/cgroup and /proc/<pid>/mountinfo to find the process's
// memory cgroup, then hands the resolved directory to `fn`.
//
// Split out because the limit walk and the headroom walk resolve identically
// and only differ in what they compute -- the Go side carries this resolution
// twice and the copies have to be kept in step by hand.
template <typename Fn>
bool with_memory_cgroup(Fn&& fn) {
    std::string cgroup_data, mount_data;
    if (!read_file("/proc/self/cgroup", &cgroup_data)) return false;
    if (!read_file("/proc/self/mountinfo", &mount_data)) return false;

    std::string v2_path, v1_memory_path;
    {
        std::istringstream in(cgroup_data);
        std::string        line;
        while (std::getline(in, line)) {
            // hid:controllers:path -- the path may itself contain ':'.
            const auto c1 = line.find(':');
            if (c1 == std::string::npos) continue;
            const auto c2 = line.find(':', c1 + 1);
            if (c2 == std::string::npos) continue;
            const std::string hid         = line.substr(0, c1);
            const std::string controllers = line.substr(c1 + 1, c2 - c1 - 1);
            const std::string path        = line.substr(c2 + 1);
            if (hid == "0" && controllers.empty()) v2_path = path;
            size_t start = 0;
            while (start <= controllers.size()) {
                const auto comma = controllers.find(',', start);
                const auto len =
                    (comma == std::string::npos ? controllers.size() : comma) - start;
                if (controllers.compare(start, len, "memory") == 0) v1_memory_path = path;
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        }
    }

    std::istringstream in(mount_data);
    std::string        line;
    while (std::getline(in, line)) {
        const auto sep = line.find(" - ");
        if (sep == std::string::npos) continue;
        const std::vector<std::string> left  = fields(line.substr(0, sep));
        const std::vector<std::string> right = fields(line.substr(sep + 3));
        if (left.size() < 5 || right.size() < 3) continue;
        const std::string& mount_root  = left[3];
        const std::string& mount_point = left[4];
        const std::string& fs_type     = right[0];
        std::string        dir;
        if (fs_type == "cgroup2") {
            if (!v2_path.empty() && cgroup_directory(mount_point, mount_root, v2_path, &dir)) {
                return fn(dir, mount_point, "memory.max", "memory.current");
            }
        } else if (fs_type == "cgroup") {
            const std::string opts = "," + right[2] + ",";
            if (!v1_memory_path.empty() && opts.find(",memory,") != std::string::npos &&
                cgroup_directory(mount_point, mount_root, v1_memory_path, &dir)) {
                return fn(dir, mount_point, "memory.limit_in_bytes", "memory.usage_in_bytes");
            }
        }
    }
    return false;
}

}  // namespace

bool read_cgroup_uint(const std::string& path, uint64_t* out) {
    std::string data;
    if (!read_file(path, &data)) return false;
    const std::string value = trim(data);
    if (value.empty() || value == "max") return false;
    return parse_uint(value, out);
}

bool read_meminfo_bytes(const std::string& path, const char* key, uint64_t* out) {
    std::string data;
    if (!read_file(path, &data)) return false;
    const std::string want = std::string(key) + ":";
    std::istringstream in(data);
    std::string        line;
    while (std::getline(in, line)) {
        if (line.compare(0, want.size(), want) != 0) continue;
        const std::vector<std::string> f = fields(line.substr(want.size()));
        if (f.empty()) return false;
        uint64_t v = 0;
        if (!parse_uint(f[0], &v)) return false;
        // /proc/meminfo publishes kB for memory lines; a unitless line would be
        // bytes already. Anything else is a format this does not claim to read.
        if (f.size() >= 2) {
            if (f[1] == "kB" || f[1] == "KB") {
                if (v > UINT64_MAX / 1024) return false;
                v *= 1024;
            } else {
                return false;
            }
        }
        *out = v;
        return true;
    }
    return false;
}

headroom_status hierarchical_headroom(const std::string& dir_in, const std::string& mount_point_in,
                                      const char* limit_file, const char* usage_file,
                                      uint64_t* out) {
    namespace fs = std::filesystem;
    fs::path dir         = fs::path(dir_in).lexically_normal();
    fs::path mount_point = fs::path(mount_point_in).lexically_normal();

    uint64_t minimum = 0;
    bool     found   = false;
    for (;;) {
        uint64_t limit = 0;
        if (read_cgroup_uint((dir / limit_file).generic_string(), &limit) && limit > 0) {
            uint64_t usage = 0;
            if (!read_cgroup_uint((dir / usage_file).generic_string(), &usage)) {
                // A level caps us and we cannot read what it has already spent.
                // Distinct from "no limit anywhere": there IS a bound, we just
                // cannot size against it, and the node-wide fallback would
                // report the host's memory rather than this cgroup's.
                return headroom_status::unreadable;
            }
            const uint64_t headroom = limit > usage ? limit - usage : 0;
            if (!found || headroom < minimum) {
                minimum = headroom;
                found   = true;
            }
        }
        if (dir == mount_point) break;
        const fs::path parent = dir.parent_path();
        const std::string p   = parent.generic_string();
        const std::string mp  = mount_point.generic_string();
        if (parent == dir || (parent != mount_point && p.rfind(mp + "/", 0) != 0)) break;
        dir = parent;
    }
    if (!found) return headroom_status::no_limit;
    *out = minimum;
    return headroom_status::ok;
}

bool min_hierarchical_headroom(const std::string& dir, const std::string& mount_point,
                               const char* limit_file, const char* usage_file, uint64_t* out) {
    return hierarchical_headroom(dir, mount_point, limit_file, usage_file, out) ==
           headroom_status::ok;
}

host_available_t host_available_bytes() {
    host_available_t r;

#if defined(__linux__)
    // Prefer the hierarchy walk: it subtracts usage at each governing level, so
    // a constrained ancestor is reported at ITS headroom rather than the leaf's.
    uint64_t        headroom = 0;
    headroom_status status   = headroom_status::no_limit;
    with_memory_cgroup([&](const std::string& dir, const std::string& mount_point,
                           const char* limit_file, const char* usage_file) {
        status = hierarchical_headroom(dir, mount_point, limit_file, usage_file, &headroom);
        return true;  // the cgroup was resolved; `status` carries the outcome
    });
    if (status == headroom_status::ok) {
        r.bytes    = headroom;
        r.measured = true;
        return r;
    }
    if (status == headroom_status::unreadable) {
        // A cgroup bounds this process and its usage could not be read. Falling
        // through to /proc/meminfo here would size the build against the whole
        // node, so report the measurement as unavailable instead and let the
        // caller fall back to its device-side bound.
        return r;
    }
    // No memory limit anywhere in the chain, or no cgroup hierarchy to resolve:
    // the node-wide figure IS the right answer.
    uint64_t avail = 0;
    if (read_meminfo_bytes("/proc/meminfo", "MemAvailable", &avail)) {
        r.bytes    = avail;
        r.measured = true;
        return r;
    }
    return r;

#else
    // This translation unit is built only as part of the CUDA index, which is
    // Linux-only, so nothing compiles this branch today. It exists so that a
    // port degrades to an honest "cannot tell" -- which callers already handle
    // as "bound by device memory only" -- rather than silently reporting zero.
    return r;
#endif
}

}  // namespace matrixone
