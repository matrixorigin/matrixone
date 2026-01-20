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
#include <assert.h>
#include <time.h>
#include "../bloom.h"
#include "../bitmap.h"

// Helper to check errors
#define CHECK(cond, msg) if (!(cond)) { printf("Error: %s\n", msg); exit(1); }

void test_basic_ops() {
    printf("Testing basic operations...\n");
    uint64_t nbits = 1000;
    uint32_t k = 3;
    bloomfilter_t *bf = bloomfilter_init(nbits, k);
    CHECK(bf != NULL, "Failed to allocate BloomFilter");

    const char *key1 = "hello";
    const char *key2 = "world";
    const char *key3 = "matrixone";
    const char *key4 = ""; // empty string

    bloomfilter_add(bf, key1, strlen(key1));
    bloomfilter_add(bf, key2, strlen(key2));
    bloomfilter_add(bf, key4, strlen(key4));

    CHECK(bloomfilter_test(bf, key1, strlen(key1)), "key1 should be present");
    CHECK(bloomfilter_test(bf, key2, strlen(key2)), "key2 should be present");
    CHECK(bloomfilter_test(bf, key4, strlen(key4)), "key4 should be present");
    if (bloomfilter_test(bf, key3, strlen(key3))) {
        printf("Warning: key3 might be a false positive (or error)\n");
    } else {
        printf("key3 is correctly identified as not present\n");
    }
    bloomfilter_free(bf);
    printf("Basic operations passed\n");
}

void test_marshal_unmarshal() {
    printf("Testing marshal/unmarshal...\n");
    uint64_t nbits = 1000;
    uint32_t k = 3;
    bloomfilter_t *bf = bloomfilter_init(nbits, k);
    
    bloomfilter_add(bf, "hello", 5);

    size_t buf_size = 0;
    uint8_t *data = bloomfilter_marshal(bf, &buf_size);
    CHECK(data != NULL && buf_size == (sizeof(bloomfilter_t) + bitmap_nbyte(nbits)), "Marshal failed");

    uint8_t *buf_copy = (uint8_t *)malloc(buf_size);
    memcpy(buf_copy, data, buf_size);

    bloomfilter_t *bf2 = bloomfilter_unmarshal(buf_copy, buf_size);
    CHECK(bf2 != NULL, "Unmarshal failed");
    CHECK(bloomfilter_test(bf2, "hello", 5), "key should be present in restored BF");
    
    bloomfilter_free(bf2); // This frees buf_copy
    bloomfilter_free(bf);
    printf("Marshal/unmarshal passed\n");
}

void test_test_and_add() {
    printf("Testing test_and_add...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    const char *key = "new_key";
    
    CHECK(!bloomfilter_test_and_add(bf, key, strlen(key)), "key should NOT be present initially");
    CHECK(bloomfilter_test(bf, key, strlen(key)), "key should BE present after test_and_add");
    CHECK(bloomfilter_test_and_add(bf, key, strlen(key)), "key should BE present on second call");
    
    bloomfilter_free(bf);
    printf("test_and_add passed\n");
}

void test_add_fixed() {
    printf("Testing add_fixed...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    
    uint64_t nullmap = 0;
    bitmap_set(&nullmap, 1); // 2nd item (index 1) is null

    uint32_t keys[] = {100, 200, 300};
    bloomfilter_add_fixed(bf, (void *)keys, sizeof(keys), sizeof(uint32_t), 3, &nullmap, sizeof(nullmap));
    
    CHECK(bloomfilter_test(bf, &keys[0], sizeof(uint32_t)), "key 100 should be present");
    CHECK(!bloomfilter_test(bf, &keys[1], sizeof(uint32_t)), "key 200 (null) should NOT be present");
    CHECK(bloomfilter_test(bf, &keys[2], sizeof(uint32_t)), "key 300 should be present");

    bloomfilter_free(bf);
    printf("add_fixed passed\n");
}

void test_test_fixed() {
    printf("Testing test_fixed...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    uint32_t k1=100, k2=200;
    bloomfilter_add(bf, &k1, 4);
    bloomfilter_add(bf, &k2, 4);
    
    uint32_t keys[] = {100, 300, 200}; // 100(Y), 300(N), 200(Y)
    bool results[3];
    
    bloomfilter_test_fixed(bf, (void *)keys, sizeof(keys), sizeof(uint32_t), 3, NULL, 0, results);
    
    CHECK(results[0], "100 should be found");
    CHECK(!results[1], "300 should not be found");
    CHECK(results[2], "200 should be found");

    bloomfilter_free(bf);
    printf("test_fixed passed\n");
}

void test_test_and_add_fixed() {
    printf("Testing test_and_add_fixed...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    uint32_t k1=100;
    bloomfilter_add(bf, &k1, 4);
    
    uint32_t keys[] = {500, 100, 600}; // 500(N->Y), 100(Y), 600(N->Y)
    bool results[3];
    
    bloomfilter_test_and_add_fixed(bf, (void *)keys, sizeof(keys), sizeof(uint32_t), 3, NULL, 0, results);
    
    CHECK(!results[0], "500 was not present");
    CHECK(results[1], "100 was present");
    CHECK(!results[2], "600 was not present");
    
    CHECK(bloomfilter_test(bf, &keys[0], 4), "500 should now be present");
    CHECK(bloomfilter_test(bf, &keys[2], 4), "600 should now be present");

    bloomfilter_free(bf);
    printf("test_and_add_fixed passed\n");
}

void test_varlena_ops() {
    printf("Testing varlena operations...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    
    // Construct buffer: [len:4][data][len:4][data][len:4][data]
    // "one" (3), "two" (3), "three" (5)
    size_t buf_size = sizeof(uint32_t)*3 + 3 + 3 + 5;
    uint8_t *buf = (uint8_t*)malloc(buf_size);
    uint8_t *ptr = buf;
    
    *(uint32_t*)ptr = 3; ptr += 4; memcpy(ptr, "one", 3); ptr += 3;
    *(uint32_t*)ptr = 3; ptr += 4; memcpy(ptr, "two", 3); ptr += 3;
    *(uint32_t*)ptr = 5; ptr += 4; memcpy(ptr, "three", 5); ptr += 5;

    // Test add_varlena with nullmap
    // Skip "two" (index 1)
    uint64_t nullmap = 0;
    bitmap_set(&nullmap, 1);
    
    bloomfilter_add_varlena_4b(bf, buf, buf_size, 3, &nullmap, sizeof(nullmap));
    
    CHECK(bloomfilter_test(bf, "one", 3), "'one' should be present");
    CHECK(!bloomfilter_test(bf, "two", 3), "'two' should NOT be present");
    CHECK(bloomfilter_test(bf, "three", 5), "'three' should be present");
    
    // Test test_varlena
    // Use same buffer
    bool results[3];
    bloomfilter_test_varlena_4b(bf, buf, buf_size, 3, NULL, 0, results);
    CHECK(results[0], "'one' found");
    CHECK(!results[1], "'two' not found");
    CHECK(results[2], "'three' found");

    // Test test_and_add_varlena
    // Reuse buffer, but this time add everything (no nullmap)
    bool taa_results[3];
    bloomfilter_test_and_add_varlena_4b(bf, buf, buf_size, 3, NULL, 0, taa_results);
    
    CHECK(taa_results[0], "'one' already there");
    CHECK(!taa_results[1], "'two' not there (was null)");
    CHECK(taa_results[2], "'three' already there");
    
    CHECK(bloomfilter_test(bf, "two", 3), "'two' should now be present");

    free(buf);
    bloomfilter_free(bf);
    printf("varlena operations passed\n");
}

void test_integer_compatibility() {
    printf("Testing integer compatibility...\n");
    bloomfilter_t *bf = bloomfilter_init(1000, 3);

    int8_t v8 = 42;
    int16_t v16 = 42;
    int32_t v32 = 42;
    int64_t v64 = 42;

    bloomfilter_add(bf, &v8, sizeof(v8));
    CHECK(bloomfilter_test(bf, &v8, sizeof(v8)), "int8 should be found");
    CHECK(bloomfilter_test(bf, &v16, sizeof(v16)), "int16 should be found");
    CHECK(bloomfilter_test(bf, &v32, sizeof(v32)), "int32 should be found");
    CHECK(bloomfilter_test(bf, &v64, sizeof(v64)), "int64 should be found");

    int64_t v64_2 = 123456789;
    bloomfilter_add(bf, &v64_2, sizeof(v64_2));
    CHECK(bloomfilter_test(bf, &v64_2, sizeof(v64_2)), "int64_2 should be found");

    // Test with another value
    int16_t v16_3 = 1000;
    bloomfilter_add(bf, &v16_3, sizeof(v16_3));
    int32_t v32_3 = 1000;
    CHECK(bloomfilter_test(bf, &v32_3, sizeof(v32_3)), "int32_3 should be found if int16_3 was added");

    bloomfilter_free(bf);
    printf("Integer compatibility passed\n");
}

int main() {
    srand(time(NULL));
    
    test_basic_ops();
    test_marshal_unmarshal();
    test_test_and_add();
    test_add_fixed();
    test_test_fixed();
    test_test_and_add_fixed();
    test_varlena_ops();
    test_integer_compatibility();

    printf("All BloomFilter tests passed!\n");
    return 0;
}
