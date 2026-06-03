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
#include "../cbitmap.h"
#include "../bitmap.h"

#define CHECK(cond, msg) if (!(cond)) { printf("Error: %s\n", msg); exit(1); }

// A generous bit cap for the feasible tests (1M bits).
#define MAXBITS (1ULL << 20)

void test_build_contain() {
    printf("Testing build_fixed + contain...\n");
    uint32_t keys[] = {1, 2, 100, 4096};
    void *f = mo_cbitmap_build_fixed(keys, sizeof(keys), sizeof(uint32_t), 4,
                                     NULL, 0, MAXBITS, 0);
    CHECK(f != NULL, "build should succeed");
    CHECK(mo_cbitmap_contain(f, 1), "1 present");
    CHECK(mo_cbitmap_contain(f, 2), "2 present");
    CHECK(mo_cbitmap_contain(f, 100), "100 present");
    CHECK(mo_cbitmap_contain(f, 4096), "4096 present");
    CHECK(!mo_cbitmap_contain(f, 3), "3 absent");
    CHECK(!mo_cbitmap_contain(f, 5000), "5000 absent");
    CHECK(!mo_cbitmap_contain(f, 1ULL << 40), "out-of-range value absent");
    mo_cbitmap_free(f);
    printf("build_fixed + contain passed\n");
}

void test_test_fixed_and_nulls() {
    printf("Testing test_fixed + nullmap...\n");
    // Build with the 2nd row (index 1) null, so its value is not added.
    uint64_t nullmap = 0;
    bitmap_set(&nullmap, 1);
    int64_t keys[] = {10, 20, 30};
    void *f = mo_cbitmap_build_fixed(keys, sizeof(keys), sizeof(int64_t), 3,
                                     &nullmap, sizeof(nullmap), MAXBITS, 0);
    CHECK(f != NULL, "build ok");
    CHECK(mo_cbitmap_contain(f, 10), "10 present");
    CHECK(!mo_cbitmap_contain(f, 20), "20 (null row) absent");
    CHECK(mo_cbitmap_contain(f, 30), "30 present");

    // Probe via test_fixed (no nulls on the probe side).
    int64_t probe[] = {10, 20, 30, 40};
    uint8_t res[4];
    mo_cbitmap_test_fixed(f, probe, sizeof(probe), sizeof(int64_t), 4, NULL, 0, res);
    CHECK(res[0] == 1, "10 found");
    CHECK(res[1] == 0, "20 not found");
    CHECK(res[2] == 1, "30 found");
    CHECK(res[3] == 0, "40 not found");

    // A null probe row is reported as 0 regardless of membership.
    uint64_t pnull = 0;
    bitmap_set(&pnull, 0);
    mo_cbitmap_test_fixed(f, probe, sizeof(probe), sizeof(int64_t), 4,
                          &pnull, sizeof(pnull), res);
    CHECK(res[0] == 0, "null probe row -> 0");
    CHECK(res[2] == 1, "30 still found");

    mo_cbitmap_free(f);
    printf("test_fixed + nullmap passed\n");
}

void test_serialize_roundtrip() {
    printf("Testing serialize/deserialize...\n");
    uint32_t keys[] = {7, 64, 65, 1000};
    void *f = mo_cbitmap_build_fixed(keys, sizeof(keys), sizeof(uint32_t), 4,
                                     NULL, 0, MAXBITS, 0);
    CHECK(f != NULL, "build ok");

    size_t len = 0;
    uint8_t *buf = mo_cbitmap_serialize(f, &len);
    CHECK(buf != NULL && len > 0, "serialize ok");

    void *f2 = mo_cbitmap_deserialize(buf, len);
    CHECK(f2 != NULL, "deserialize ok");
    for (int i = 0; i < 4; i++) {
        CHECK(mo_cbitmap_contain(f2, keys[i]), "key present after roundtrip");
    }
    CHECK(!mo_cbitmap_contain(f2, 8), "8 absent after roundtrip");

    // A truncated buffer must fail cleanly.
    CHECK(mo_cbitmap_deserialize(buf, 4) == NULL, "short buffer -> NULL");

    mo_cbitmap_free_buf(buf);
    mo_cbitmap_free(f);
    mo_cbitmap_free(f2);
    printf("serialize/deserialize passed\n");
}

void test_feasibility_gate() {
    printf("Testing feasibility gate...\n");
    // Max value 999 with a 100-bit cap -> infeasible -> NULL (caller falls back).
    uint32_t big[] = {999};
    void *f = mo_cbitmap_build_fixed(big, sizeof(big), sizeof(uint32_t), 1,
                                     NULL, 0, 100, 0);
    CHECK(f == NULL, "max value exceeds cap -> NULL");

    // Max value 50 with a 100-bit cap -> feasible.
    uint32_t ok[] = {50};
    void *f2 = mo_cbitmap_build_fixed(ok, sizeof(ok), sizeof(uint32_t), 1,
                                      NULL, 0, 100, 0);
    CHECK(f2 != NULL, "within cap -> ok");
    CHECK(mo_cbitmap_contain(f2, 50), "50 present");
    mo_cbitmap_free(f2);
    printf("feasibility gate passed\n");
}

void test_empty_and_nil() {
    printf("Testing empty set + NULL safety...\n");
    // Empty input yields a valid filter that matches nothing.
    void *f = mo_cbitmap_build_fixed(NULL, 0, sizeof(uint32_t), 0, NULL, 0, MAXBITS, 0);
    CHECK(f != NULL, "empty build -> valid empty filter");
    CHECK(!mo_cbitmap_contain(f, 0), "empty contains nothing");
    CHECK(!mo_cbitmap_contain(f, 1), "empty contains nothing");

    size_t len = 0;
    uint8_t *buf = mo_cbitmap_serialize(f, &len);
    CHECK(buf != NULL, "serialize empty ok");
    void *f2 = mo_cbitmap_deserialize(buf, len);
    CHECK(f2 != NULL, "deserialize empty ok");
    CHECK(!mo_cbitmap_contain(f2, 0), "empty roundtrip contains nothing");

    mo_cbitmap_free_buf(buf);
    mo_cbitmap_free(f);
    mo_cbitmap_free(f2);

    // NULL handle is safe.
    CHECK(!mo_cbitmap_contain(NULL, 5), "NULL filter contains nothing");
    mo_cbitmap_free(NULL); // must not crash
    printf("empty set + NULL safety passed\n");
}

void test_width_compat() {
    printf("Testing width compatibility (little-endian decode)...\n");
    // Build from a 4-byte 42; probing the same value at other widths must hit
    // the same bit (identical LE zero-extension on both sides).
    uint32_t k32 = 42;
    void *f = mo_cbitmap_build_fixed(&k32, sizeof(k32), sizeof(uint32_t), 1,
                                     NULL, 0, MAXBITS, 0);
    CHECK(f != NULL, "build ok");
    CHECK(mo_cbitmap_contain(f, 42), "42 present");

    uint8_t k8 = 42, r;
    uint16_t k16 = 42;
    int64_t k64 = 42;
    mo_cbitmap_test_fixed(f, &k8, sizeof(k8), sizeof(uint8_t), 1, NULL, 0, &r);
    CHECK(r == 1, "uint8 42 hits");
    mo_cbitmap_test_fixed(f, &k16, sizeof(k16), sizeof(uint16_t), 1, NULL, 0, &r);
    CHECK(r == 1, "uint16 42 hits");
    mo_cbitmap_test_fixed(f, &k64, sizeof(k64), sizeof(int64_t), 1, NULL, 0, &r);
    CHECK(r == 1, "int64 42 hits");
    mo_cbitmap_free(f);
    printf("width compatibility passed\n");
}

void test_offset() {
    printf("Testing offset (base) layout...\n");
    // High but narrow span: values near 10M, span 3. Under a tiny 100-bit cap
    // the legacy value-indexed layout is infeasible (max ~10M), but the offset
    // layout (base=min, span=3) fits.
    int64_t keys[] = {10000000, 10000001, 10000002, 10000003};
    void *legacy = mo_cbitmap_build_fixed(keys, sizeof(keys), sizeof(int64_t), 4,
                                          NULL, 0, 100, 0);
    CHECK(legacy == NULL, "legacy layout infeasible for high values under tiny cap");

    void *f = mo_cbitmap_build_fixed(keys, sizeof(keys), sizeof(int64_t), 4,
                                     NULL, 0, 100, 1);
    CHECK(f != NULL, "offset layout feasible (span fits cap)");
    CHECK(mo_cbitmap_contain(f, 10000000), "min present");
    CHECK(mo_cbitmap_contain(f, 10000003), "max present");
    CHECK(!mo_cbitmap_contain(f, 9999999), "below base absent");
    CHECK(!mo_cbitmap_contain(f, 10000004), "above max absent");
    CHECK(!mo_cbitmap_contain(f, 5), "far below base absent");

    // serialize/deserialize preserves base + membership.
    size_t len = 0;
    uint8_t *buf = mo_cbitmap_serialize(f, &len);
    CHECK(buf != NULL, "serialize ok");
    void *f2 = mo_cbitmap_deserialize(buf, len);
    CHECK(f2 != NULL, "deserialize ok");
    CHECK(mo_cbitmap_contain(f2, 10000002), "present after roundtrip");
    CHECK(!mo_cbitmap_contain(f2, 9999999), "below base absent after roundtrip");

    // probe via test_fixed honors the base.
    int64_t probe[] = {10000000, 9999999, 10000003, 10000004};
    uint8_t res[4];
    mo_cbitmap_test_fixed(f, probe, sizeof(probe), sizeof(int64_t), 4, NULL, 0, res);
    CHECK(res[0] == 1 && res[1] == 0 && res[2] == 1 && res[3] == 0,
          "offset test_fixed honors base");

    mo_cbitmap_free_buf(buf);
    mo_cbitmap_free(f);
    mo_cbitmap_free(f2);
    printf("offset layout passed\n");
}

int main() {
    test_build_contain();
    test_test_fixed_and_nulls();
    test_serialize_roundtrip();
    test_feasibility_gate();
    test_empty_and_nil();
    test_width_compat();
    test_offset();
    printf("All cbitmap tests passed!\n");
    return 0;
}
