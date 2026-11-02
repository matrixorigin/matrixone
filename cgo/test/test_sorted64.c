// Copyright 2026 Matrix Origin
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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "../sorted64.h"

#define CHECK(cond, msg) if (!(cond)) { printf("Error: %s\n", msg); exit(1); }

// The payload holds the non-null keys sorted and deduplicated.
void test_build_sorts_and_dedups() {
    printf("Testing build sort, dedup and nulls...\n");
    int64_t k[] = {9, 3, 9, -1, 3, 7};
    uint64_t nulls[1] = {1ULL << 5};  // row 5 (key 7) is null
    uint64_t *p = mo_sorted64_build_fixed(k, sizeof(k), sizeof(int64_t), 6, nulls, sizeof(nulls));
    CHECK(p != NULL, "build");
    CHECK(p[0] == 3, "three distinct non-null keys");
    CHECK(p[1] == 3 && p[2] == 9 && p[3] == (uint64_t)-1, "sorted as uint64");
    CHECK(mo_sorted64_contains(p, 9) && mo_sorted64_contains(p, (uint64_t)-1), "contains");
    CHECK(!mo_sorted64_contains(p, 7), "null key absent");
    mo_sorted64_free(p);
}

// An elemsz other than 1, 2, 4 or 8 is rejected.
void test_build_rejects_elemsz() {
    printf("Testing build elemsz validation...\n");
    uint8_t k[6] = {1, 2, 3, 4, 5, 6};
    CHECK(mo_sorted64_build_fixed(k, sizeof(k), 0, 3, NULL, 0) == NULL, "elemsz 0");
    CHECK(mo_sorted64_build_fixed(k, sizeof(k), 3, 2, NULL, 0) == NULL, "elemsz 3");
    CHECK(mo_sorted64_build_fixed(k, sizeof(k), 16, 1, NULL, 0) == NULL, "elemsz 16");
    uint64_t *p = mo_sorted64_build_fixed(k, sizeof(k), 2, 3, NULL, 0);
    CHECK(p != NULL && p[0] == 3, "elemsz 2");
    mo_sorted64_free(p);
}

// An item whose bytes end past len is skipped.
void test_build_skips_truncated_item() {
    printf("Testing build truncated input...\n");
    uint32_t k[2] = {5, 6};
    uint64_t *p = mo_sorted64_build_fixed(k, sizeof(k) - 1, sizeof(uint32_t), 2, NULL, 0);
    CHECK(p != NULL && p[0] == 1 && p[1] == 5, "only the complete item");
    mo_sorted64_free(p);
}

int main() {
    test_build_sorts_and_dedups();
    test_build_rejects_elemsz();
    test_build_skips_truncated_item();
    printf("All sorted64 tests passed\n");
    return 0;
}
