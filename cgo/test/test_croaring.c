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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "../croaring.h"
#include "../bitmap.h"

#define CHECK(cond, msg) if (!(cond)) { printf("Error: %s\n", msg); exit(1); }

void test_create_add_contains() {
    printf("Testing create/add_fixed/contains...\n");
    void *r = mo_croaring_create();
    CHECK(r != NULL, "create ok");
    // Sparse, large values — the regime where a dense bitset is infeasible but
    // roaring stays compact.
    int64_t keys[] = {1, 100, 1LL << 40, 7};
    mo_croaring_add_fixed(r, keys, sizeof(keys), sizeof(int64_t), 4, NULL, 0);
    CHECK(mo_croaring_contains(r, 1), "1 present");
    CHECK(mo_croaring_contains(r, 7), "7 present");
    CHECK(mo_croaring_contains(r, 100), "100 present");
    CHECK(mo_croaring_contains(r, 1LL << 40), "1<<40 present");
    CHECK(!mo_croaring_contains(r, 2), "2 absent");
    CHECK(mo_croaring_cardinality(r) == 4, "cardinality 4");
    mo_croaring_free(r);
    printf("create/add_fixed/contains passed\n");
}

void test_nulls() {
    printf("Testing add_fixed with nullmap...\n");
    void *r = mo_croaring_create();
    uint64_t nullmap = 0;
    bitmap_set(&nullmap, 1); // index 1 is null
    uint32_t keys[] = {10, 20, 30};
    mo_croaring_add_fixed(r, keys, sizeof(keys), sizeof(uint32_t), 3,
                          &nullmap, sizeof(nullmap));
    CHECK(mo_croaring_contains(r, 10), "10 present");
    CHECK(!mo_croaring_contains(r, 20), "20 (null row) absent");
    CHECK(mo_croaring_contains(r, 30), "30 present");
    CHECK(mo_croaring_cardinality(r) == 2, "cardinality 2 (null skipped)");
    mo_croaring_free(r);
    printf("nullmap passed\n");
}

void test_test_fixed() {
    printf("Testing test_fixed...\n");
    void *r = mo_croaring_create();
    uint32_t keys[] = {100, 200};
    mo_croaring_add_fixed(r, keys, sizeof(keys), sizeof(uint32_t), 2, NULL, 0);

    uint32_t probe[] = {100, 300, 200, 400};
    uint8_t res[4];
    mo_croaring_test_fixed(r, probe, sizeof(probe), sizeof(uint32_t), 4, NULL, 0, res);
    CHECK(res[0] == 1, "100 found");
    CHECK(res[1] == 0, "300 not found");
    CHECK(res[2] == 1, "200 found");
    CHECK(res[3] == 0, "400 not found");

    // A null probe row is reported as 0.
    uint64_t pnull = 0;
    bitmap_set(&pnull, 2);
    mo_croaring_test_fixed(r, probe, sizeof(probe), sizeof(uint32_t), 4,
                           &pnull, sizeof(pnull), res);
    CHECK(res[0] == 1, "100 still found");
    CHECK(res[2] == 0, "null probe row -> 0");
    mo_croaring_free(r);
    printf("test_fixed passed\n");
}

void test_serialize_roundtrip() {
    printf("Testing serialize/deserialize...\n");
    void *r = mo_croaring_create();
    int64_t keys[] = {5, 500, 1LL << 50};
    mo_croaring_add_fixed(r, keys, sizeof(keys), sizeof(int64_t), 3, NULL, 0);

    size_t len = 0;
    uint8_t *buf = mo_croaring_serialize(r, &len);
    CHECK(buf != NULL && len > 0, "serialize ok");

    void *r2 = mo_croaring_deserialize(buf, len);
    CHECK(r2 != NULL, "deserialize ok");
    CHECK(mo_croaring_contains(r2, 5), "5 present after roundtrip");
    CHECK(mo_croaring_contains(r2, 500), "500 present after roundtrip");
    CHECK(mo_croaring_contains(r2, 1LL << 50), "1<<50 present after roundtrip");
    CHECK(!mo_croaring_contains(r2, 6), "6 absent");
    CHECK(mo_croaring_cardinality(r2) == 3, "cardinality preserved");

    mo_croaring_free_buf(buf);
    mo_croaring_free(r);
    mo_croaring_free(r2);
    printf("serialize/deserialize passed\n");
}

void test_u64_buffer() {
    printf("Testing uint64 buffer (8-byte elements)...\n");
    void *r = mo_croaring_create();
    uint64_t vals[] = {0, 42, 1ULL << 63};
    mo_croaring_add_fixed(r, vals, sizeof(vals), sizeof(uint64_t), 3, NULL, 0);
    CHECK(mo_croaring_contains(r, 0), "0 present");
    CHECK(mo_croaring_contains(r, 42), "42 present");
    CHECK(mo_croaring_contains(r, 1ULL << 63), "1<<63 present");
    CHECK(mo_croaring_cardinality(r) == 3, "cardinality 3");
    mo_croaring_free(r);
    printf("uint64 buffer passed\n");
}

void test_nil_safety() {
    printf("Testing NULL safety...\n");
    CHECK(!mo_croaring_contains(NULL, 5), "NULL contains nothing");
    CHECK(mo_croaring_cardinality(NULL) == 0, "NULL cardinality 0");
    mo_croaring_free(NULL); // must not crash
    printf("NULL safety passed\n");
}

int main() {
    test_create_add_contains();
    test_nulls();
    test_test_fixed();
    test_serialize_roundtrip();
    test_u64_buffer();
    test_nil_safety();
    printf("All CRoaring tests passed!\n");
    return 0;
}
