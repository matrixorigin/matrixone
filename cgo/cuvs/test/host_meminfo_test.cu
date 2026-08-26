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

// These mirror TestMinHierarchicalHeadroom in pkg/common/system/system_test.go
// case for case. The CREATE path sizes capacity from the Go reader and this
// side admits against the C++ one, so the two readings have to agree; the
// cheapest way to keep them agreeing is to hold both to the same table of
// cases, and to notice here when one of them changes.

#include "host_meminfo.h"
#include "test_framework.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

using matrixone::host_available_bytes;
using matrixone::min_hierarchical_headroom;
using matrixone::read_cgroup_uint;
using matrixone::read_meminfo_bytes;

namespace {

namespace fs = std::filesystem;

// A temp tree that removes itself, so a failing assertion cannot leave cgroup
// fixtures behind for the next run to trip over.
struct temp_tree {
    fs::path root;
    explicit temp_tree(const char* tag) {
        root = fs::temp_directory_path() /
               ("mo_host_meminfo_" + std::string(tag) + "_" + std::to_string(::getpid()));
        fs::remove_all(root);
        fs::create_directories(root);
    }
    ~temp_tree() {
        std::error_code ec;
        fs::remove_all(root, ec);
    }
};

void write_file(const fs::path& dir, const char* name, const std::string& val) {
    std::ofstream out(dir / name, std::ios::trunc);
    out << val << "\n";
}

}  // namespace

// Parent: 8 GiB cap with 7 GiB already charged (siblings) -> 1 GiB headroom.
// Child:  4 GiB cap with 1 GiB charged to us              -> 3 GiB headroom.
// The binding constraint is the PARENT's 1 GiB. Limit-minimum minus leaf-usage
// would instead report 4 GiB - 1 GiB = 3 GiB, i.e. 3x too much.
TEST(HostMeminfoTest, HeadroomReportsTightestGoverningLevel) {
    temp_tree t("tightest");
    const fs::path root   = t.root;
    const fs::path parent = root / "parent";
    const fs::path child  = parent / "child";
    fs::create_directories(child);

    write_file(root, "memory.max", "max");
    write_file(root, "memory.current", "0");
    write_file(parent, "memory.max", std::to_string(8ULL << 30));
    write_file(parent, "memory.current", std::to_string(7ULL << 30));
    write_file(child, "memory.max", std::to_string(4ULL << 30));
    write_file(child, "memory.current", std::to_string(1ULL << 30));

    uint64_t got = 0;
    ASSERT_TRUE(min_hierarchical_headroom(child.string(), root.string(), "memory.max",
                                          "memory.current", &got));
    ASSERT_EQ(got, (uint64_t)(1ULL << 30));
}

TEST(HostMeminfoTest, ExhaustedLevelReportsZeroStillMeasured) {
    temp_tree t("exhausted");
    const fs::path root   = t.root;
    const fs::path parent = root / "parent";
    const fs::path child  = parent / "child";
    fs::create_directories(child);

    write_file(root, "memory.max", "max");
    write_file(root, "memory.current", "0");
    write_file(parent, "memory.max", std::to_string(8ULL << 30));
    write_file(parent, "memory.current", std::to_string(9ULL << 30));  // over its cap
    write_file(child, "memory.max", std::to_string(4ULL << 30));
    write_file(child, "memory.current", std::to_string(1ULL << 30));

    uint64_t got = 1;
    ASSERT_TRUE(min_hierarchical_headroom(child.string(), root.string(), "memory.max",
                                          "memory.current", &got));
    ASSERT_EQ(got, (uint64_t)0);
}

TEST(HostMeminfoTest, LimitWithoutReadableUsageIsUnmeasured) {
    // Skipping such a level would resurrect the overstatement this prevents.
    temp_tree t("nousage");
    const fs::path root   = t.root;
    const fs::path parent = root / "parent";
    const fs::path child  = parent / "child";
    fs::create_directories(child);

    write_file(root, "memory.max", "max");
    write_file(root, "memory.current", "0");
    write_file(parent, "memory.max", std::to_string(8ULL << 30));  // capped, no memory.current
    write_file(child, "memory.max", std::to_string(4ULL << 30));
    write_file(child, "memory.current", std::to_string(1ULL << 30));

    uint64_t got = 0;
    ASSERT_FALSE(min_hierarchical_headroom(child.string(), root.string(), "memory.max",
                                           "memory.current", &got));
}

TEST(HostMeminfoTest, NoLimitAnywhereIsUnmeasured) {
    // An unlimited hierarchy must fall back to the host reading, not report 0.
    temp_tree t("nolimit");
    const fs::path root   = t.root;
    const fs::path parent = root / "parent";
    const fs::path child  = parent / "child";
    fs::create_directories(child);

    write_file(root, "memory.max", "max");
    write_file(root, "memory.current", "0");
    write_file(parent, "memory.max", "max");
    write_file(child, "memory.max", "max");
    write_file(child, "memory.current", std::to_string(1ULL << 30));

    uint64_t got = 0;
    ASSERT_FALSE(min_hierarchical_headroom(child.string(), root.string(), "memory.max",
                                           "memory.current", &got));
}

// v1 uses different filenames for the same two numbers; the walk is shared, so
// this only proves the names are wired through.
TEST(HostMeminfoTest, CgroupV1FileNames) {
    temp_tree t("v1");
    const fs::path root  = t.root;
    const fs::path child = root / "child";
    fs::create_directories(child);

    write_file(root, "memory.limit_in_bytes", std::to_string(8ULL << 30));
    write_file(root, "memory.usage_in_bytes", std::to_string(7ULL << 30));
    write_file(child, "memory.limit_in_bytes", std::to_string(4ULL << 30));
    write_file(child, "memory.usage_in_bytes", std::to_string(1ULL << 30));

    uint64_t got = 0;
    ASSERT_TRUE(min_hierarchical_headroom(child.string(), root.string(), "memory.limit_in_bytes",
                                          "memory.usage_in_bytes", &got));
    ASSERT_EQ(got, (uint64_t)(1ULL << 30));
}

TEST(HostMeminfoTest, ReadCgroupUintRejectsUnlimitedAndJunk) {
    temp_tree t("uint");
    uint64_t  v = 0;

    write_file(t.root, "max_v", "max");
    ASSERT_FALSE(read_cgroup_uint((t.root / "max_v").string(), &v));

    write_file(t.root, "empty_v", "");
    ASSERT_FALSE(read_cgroup_uint((t.root / "empty_v").string(), &v));

    write_file(t.root, "junk_v", "12x34");
    ASSERT_FALSE(read_cgroup_uint((t.root / "junk_v").string(), &v));

    // A negative reading must not wrap into a colossal limit -- that is exactly
    // the value that would admit a build this governor exists to refuse.
    write_file(t.root, "neg_v", "-1");
    ASSERT_FALSE(read_cgroup_uint((t.root / "neg_v").string(), &v));

    ASSERT_FALSE(read_cgroup_uint((t.root / "absent_v").string(), &v));

    // Zero is a legitimate usage reading for an empty cgroup, not a failure.
    write_file(t.root, "zero_v", "0");
    ASSERT_TRUE(read_cgroup_uint((t.root / "zero_v").string(), &v));
    ASSERT_EQ(v, (uint64_t)0);

    write_file(t.root, "ok_v", "  4096  ");
    ASSERT_TRUE(read_cgroup_uint((t.root / "ok_v").string(), &v));
    ASSERT_EQ(v, (uint64_t)4096);
}

TEST(HostMeminfoTest, ReadMeminfoConvertsKbToBytes) {
    temp_tree t("meminfo");
    write_file(t.root, "meminfo",
               "MemTotal:       32827108 kB\n"
               "MemFree:         1234567 kB\n"
               "MemAvailable:   21341428 kB\n"
               "Buffers:          123456 kB");

    uint64_t v = 0;
    ASSERT_TRUE(read_meminfo_bytes((t.root / "meminfo").string(), "MemAvailable", &v));
    ASSERT_EQ(v, (uint64_t)21341428 * 1024);

    ASSERT_TRUE(read_meminfo_bytes((t.root / "meminfo").string(), "MemFree", &v));
    ASSERT_EQ(v, (uint64_t)1234567 * 1024);

    // A key that is a prefix of no line, and a missing file.
    ASSERT_FALSE(read_meminfo_bytes((t.root / "meminfo").string(), "MemNotAThing", &v));
    ASSERT_FALSE(read_meminfo_bytes((t.root / "absent").string(), "MemAvailable", &v));
}

// The live reading. This asserts the CONTRACT, not a value: the machine's real
// availability is not knowable from here, and pinning it would make the test a
// weather report.
TEST(HostMeminfoTest, LiveReadingIsSelfConsistent) {
    const matrixone::host_available_t r = host_available_bytes();
#if defined(__linux__)
    // An unmeasured result here means every tier failed -- worth failing on,
    // because callers respond by dropping the host bound entirely.
    ASSERT_TRUE(r.measured);
#endif
    if (!r.measured) {
        ASSERT_EQ(r.bytes, (uint64_t)0);
    }
}
