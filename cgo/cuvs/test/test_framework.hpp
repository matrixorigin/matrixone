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

#include <iostream>
#include <vector>
#include <string>
#include <functional>
#include <chrono>
#include <numeric> // For std::iota
#include <future>  // For std::async
#include <atomic>
#include <map>
#include <stdexcept>
#include <sstream> // For building string messages
#include <algorithm> // For std::sort
#include <any> // For std::any comparisons in assertions

// --- Minimal Custom Test Framework (Stub for compilation) ---

// Logging - minimal versions
#define TEST_LOG(msg) std::cout << "[INFO    ] " << msg << std::endl
#define TEST_ERROR(msg) std::cerr << "[ERROR   ] " << msg << std::endl

// Global flag to indicate if the current test has failed (kept minimal)
extern thread_local bool current_test_failed;

// Helper to build string messages for assertions (handles various types)
template <typename T>
std::string to_string_for_assertion(const T& val) {
    std::ostringstream oss;
    oss << val;
    return oss.str();
}
inline std::string to_string_for_assertion(const std::any&) { return "std::any"; } // Simplified
inline std::string to_string_for_assertion(const char* val) { return std::string(val); }

// Helper to check if an exception_ptr holds a specific exception type (kept minimal)
template <typename E>
inline bool has_exception(const std::exception_ptr& ep) {
    if (!ep) return false;
    try {
        std::rethrow_exception(ep);
    } catch (const E& e) {
        return true;
    } catch (...) {
        return false;
    }
}

// Assertions - simplified to just return/log if condition is false
#define REPORT_FAILURE(msg_str) do { TEST_ERROR(msg_str); current_test_failed = true; return; } while (0)
#define ASSERT_TRUE(condition) do { if (!(condition)) { REPORT_FAILURE("ASSERT_TRUE failed: " #condition); } } while (0)
#define ASSERT_FALSE(condition) ASSERT_TRUE(!(condition))
#define ASSERT_EQ(val1, val2) do { \
    auto v1 = (val1); \
    auto v2 = (val2); \
    if (!(v1 == v2)) { \
        std::ostringstream oss; \
        oss << "ASSERT_EQ failed: " << #val1 << " (" << v1 << ") vs " << #val2 << " (" << v2 << ")"; \
        REPORT_FAILURE(oss.str()); \
    } \
} while (0)
#define ASSERT_NE(val1, val2) do { if (!((val1) != (val2))) { REPORT_FAILURE("ASSERT_NE failed: " #val1 " vs " #val2); } } while (0)
#define ASSERT_GE(val1, val2) do { if (!((val1) >= (val2))) { REPORT_FAILURE("ASSERT_GE failed: " #val1 " vs " #val2); } } while (0)
#define ASSERT_THROW(statement, expected_exception) do { bool caught = false; try { statement; } catch (const expected_exception&) { caught = true; } if (!caught) { REPORT_FAILURE("ASSERT_THROW failed"); } } while (0)
#define ASSERT_NO_THROW(statement) do { try { statement; } catch (...) { REPORT_FAILURE("ASSERT_NO_THROW failed"); } } while (0)

// Test registration
struct TestCase {
    std::string name;
    std::function<void()> func;
    bool failed = false;
};

inline std::vector<TestCase>& get_test_cases() {
    static std::vector<TestCase> test_cases;
    return test_cases;
}

// Simplified TEST macro for compilation
#define TEST(suite, name) \
    static void test_func_##suite##_##name(); \
    struct RegisterTest_##suite##_##name { \
        RegisterTest_##suite##_##name() { \
            get_test_cases().push_back({#suite "::" #name, test_func_##suite##_##name}); \
        } \
    }; \
    static RegisterTest_##suite##_##name register_test_##suite##_##name; \
    static void test_func_##suite##_##name()

inline int RUN_ALL_TESTS() {
    int passed_count = 0;
    int failed_count = 0;
    TEST_LOG("Running " << get_test_cases().size() << " tests (minimal framework)...");

    for (auto& test_case : get_test_cases()) {
        current_test_failed = false; // Reset for each test
        TEST_LOG("[ RUN      ] " << test_case.name);
        try {
            test_case.func();
        } catch (const std::exception& e) {
            TEST_ERROR("Test threw unhandled exception: " << e.what());
            current_test_failed = true;
        } catch (...) {
            TEST_ERROR("Test threw unhandled unknown exception.");
            current_test_failed = true;
        }

        if (current_test_failed) {
            test_case.failed = true;
            failed_count++;
            TEST_LOG("[  FAILED  ] " << test_case.name);
        } else {
            passed_count++;
            TEST_LOG("[       OK ] " << test_case.name);
        }
    }

    TEST_LOG("--------------------------------------------------");
    TEST_LOG("[==========] " << passed_count + failed_count << " tests ran.");
    TEST_LOG("[  PASSED  ] " << passed_count << " tests.");
    if (failed_count > 0) {
        TEST_ERROR("[  FAILED  ] " << failed_count << " tests, listed below:");
        for (const auto& test_case : get_test_cases()) {
            if (test_case.failed) {
                TEST_ERROR("    " << test_case.name);
            }
        }
    }
    TEST_LOG("--------------------------------------------------");

    return failed_count;
}

// --- End of Minimal Custom Test Framework (Stub for compilation) ---
