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

#pragma once

#include <cstdint>
#include <string>

namespace matrixone {

// ---------------------------------------------------------------------------
// Host memory availability, read natively.
//
// This is the C++ twin of system.MemoryAvailableIncludingCache, and it exists
// because the host allocations it governs are made here, not in Go. The Go
// reader stays where it is and keeps serving the rest of mo-service; this one
// serves the index memory governor.
//
// The two MUST agree, because the CREATE path sizes capacity from the Go
// reading and this side admits against its own. They are kept in agreement by
// implementing the same rules in the same order, not by sharing code:
//
//   1. the tightest `limit - usage` across every governing cgroup level
//   2. else a single level's `limit - usage`
//   3. else the platform's "free including reclaimable" figure
//
// host_meminfo_test.cu covers 1 and 2 against temp directories, which is the
// part with branches; 3 is a single documented file/syscall per platform.
// ---------------------------------------------------------------------------

// host_available_t separates "nothing available" from "could not tell", which
// demand opposite responses -- refuse the build vs fall back to another bound.
// Collapsing them is a defect this subsystem has already been bitten by once,
// on the Go side: a cgroup at its limit read as unmeasured and DISABLED the
// bound that existed to stop the build.
struct host_available_t {
    uint64_t bytes    = 0;
    bool     measured = false;
};

// Memory that could be allocated on this node without evicting live pages.
//
// Linux only, because the CUDA index this governs is Linux only: cgroup
// headroom where a hierarchy is readable (v2 memory.max/current, v1
// memory.limit_in_bytes/usage_in_bytes), else /proc/meminfo MemAvailable.
// Anywhere else: measured=false, and the caller falls back to its other bounds.
host_available_t host_available_bytes();

// --- exposed for tests -----------------------------------------------------

// Reads a single-value cgroup file. False when missing, empty, or the literal
// "max" (v2's "unlimited"). Zero with a true return is a legitimate usage
// reading for an empty cgroup.
bool read_cgroup_uint(const std::string& path, uint64_t* out);

// Reads a cgroup LIMIT file. False when the file imposes no bound -- missing,
// empty, v2's "max", or v1's NUMERIC unlimited sentinel.
//
// v1 has no "max" string: with no limit set it writes PAGE_COUNTER_MAX, which
// parses as a valid integer. Read as a real limit it yields ~9.2 EB of headroom
// reported as MEASURED, which disables the host bound on exactly the hosts that
// still run cgroup v1.
bool read_cgroup_limit(const std::string& path, uint64_t* out);

// Reads one `Key:  N kB` line out of a /proc/meminfo-shaped file, returning
// BYTES. Callers pass the key including no colon, e.g. "MemAvailable".
bool read_meminfo_bytes(const std::string& path, const char* key, uint64_t* out);

// The tightest `limit - usage` from `dir` up to `mount_point`, and whether any
// level imposed a limit at all.
//
// Minimising the LIMIT and subtracting only the leaf's usage overstates
// headroom whenever an ancestor binds: a parent capped at 8 GiB with 7 GiB
// charged to siblings has 1 GiB left, but leaf arithmetic reports nearly 8.
// A level that publishes a limit but no readable usage makes the whole
// measurement unavailable rather than being skipped -- skipping a governing
// level is the overstatement this exists to prevent.
bool min_hierarchical_headroom(const std::string& dir, const std::string& mount_point,
                               const char* limit_file, const char* usage_file, uint64_t* out);

}  // namespace matrixone
