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

    bloomfilter_free(bf2); // This will free(buf_copy)
    bloomfilter_free(bf);
    
    printf("Bloom filter single-malloc test passed\n");
    return 0;
}
