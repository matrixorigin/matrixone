#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "../bloom.h"
#include "../bitmap.h"

int main() {
    srand(time(NULL));
    uint64_t nbits = 1000;
    uint32_t k = 3;
    
    // Test the new Init which allocates everything in one go
    bloomfilter_t *bf = bloomfilter_init(nbits, k);
    if (!bf) {
        printf("Failed to allocate BloomFilter\n");
        return 1;
    }

    const char *key1 = "hello";
    const char *key2 = "world";
    const char *key3 = "matrixone";

    bloomfilter_add(bf, key1, strlen(key1));
    bloomfilter_add(bf, key2, strlen(key2));

    if (!bloomfilter_test(bf, key1, strlen(key1))) {
        printf("Error: key1 should be present\n");
        return 1;
    }
    if (!bloomfilter_test(bf, key2, strlen(key2))) {
        printf("Error: key2 should be present\n");
        return 1;
    }
    if (bloomfilter_test(bf, key3, strlen(key3))) {
        printf("Warning: key3 might be a false positive (or error)\n");
    } else {
        printf("key3 is correctly identified as not present\n");
    }

    // Test Marshal/Unmarshal
    size_t buf_size = 0;
    uint8_t *data = bloomfilter_marshal(bf, &buf_size);
    if (!data || buf_size != (sizeof(bloomfilter_t) + bitmap_nbyte(nbits))) {
        printf("Failed to marshal BloomFilter\n");
        return 1;
    }

    // Create a copy to simulate a real-world scenario (e.g. data from network/disk)
    uint8_t *buf_copy = (uint8_t *)malloc(buf_size);
    memcpy(buf_copy, data, buf_size);

    bloomfilter_t *bf2 = bloomfilter_unmarshal(buf_copy, buf_size);
    if (!bf2) {
        printf("Failed to unmarshal BloomFilter\n");
        free(buf_copy);
        return 1;
    }

    if (!bloomfilter_test(bf2, key1, strlen(key1))) {
        printf("Error: key1 should be present in restored bitmap\n");
        return 1;
    }
    if (bloomfilter_test(bf2, key3, strlen(key3))) {
        printf("Warning: key3 might be a false positive in restored bitmap\n");
    } else {
        printf("key3 is correctly identified as not present in restored bitmap\n");
    }

    // Test test_and_add
    const char *key4 = "new_key";
    if (bloomfilter_test_and_add(bf2, key4, strlen(key4))) {
        printf("Error: key4 should NOT be present initially\n");
        return 1;
    }
    if (!bloomfilter_test(bf2, key4, strlen(key4))) {
        printf("Error: key4 should BE present after test_and_add\n");
        return 1;
    }
    if (!bloomfilter_test_and_add(bf2, key4, strlen(key4))) {
        printf("Error: key4 should BE present when calling test_and_add again\n");
        return 1;
    }
    printf("test_and_add passed\n");

    // Test add_multi
    uint32_t data_multi[] = {100, 200, 300};
    bloomfilter_add_multi(bf2, (void *)data_multi, sizeof(data_multi), sizeof(uint32_t), 3);
    for (int i = 0; i < 3; i++) {
        if (!bloomfilter_test(bf2, &data_multi[i], sizeof(uint32_t))) {
            printf("Error: data_multi[%d] should be present after add_multi\n", i);
            return 1;
        }
    }
    printf("add_multi passed\n");

    // Test test_multi
    uint32_t test_multi_keys[] = {100, 400, 300}; // 100 and 300 added, 400 not
    bool test_results[3];
    bloomfilter_test_multi(bf2, (void *)test_multi_keys, sizeof(test_multi_keys), sizeof(uint32_t), 3, test_results);
    if (!test_results[0] || test_results[1] || !test_results[2]) {
        printf("Error: test_multi results incorrect: %d %d %d\n", test_results[0], test_results[1], test_results[2]);
        return 1;
    }
    printf("test_multi passed\n");

    // Test test_and_add_multi
    uint32_t taa_multi_keys[] = {500, 100, 600}; // 500, 600 new, 100 exists
    bool taa_results[3];
    bloomfilter_test_and_add_multi(bf2, (void *)taa_multi_keys, sizeof(taa_multi_keys), sizeof(uint32_t), 3, taa_results);
    
    // 500: was not there (false), 100: was there (true), 600: was not there (false)
    if (taa_results[0] || !taa_results[1] || taa_results[2]) {
        printf("Error: test_and_add_multi results incorrect: %d %d %d\n", taa_results[0], taa_results[1], taa_results[2]);
        return 1;
    }
    
    // Verify they are all there now
    for (int i = 0; i < 3; i++) {
        if (!bloomfilter_test(bf2, &taa_multi_keys[i], sizeof(uint32_t))) {
            printf("Error: taa_multi_keys[%d] should be present after test_and_add_multi\n", i);
            return 1;
        }
    }
    printf("test_and_add_multi passed\n");

    bloomfilter_free(bf2); // This will free(buf_copy)
    bloomfilter_free(bf);
    
    printf("Bloom filter single-malloc test passed\n");
    return 0;
}
