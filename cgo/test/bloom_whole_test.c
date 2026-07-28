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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "../bloom.h"
#include "../varlena.h"

// Helper to create a packed buffer of varlenas
int create_test_buffer(uint8_t *buffer, uint8_t *area) {
    uint8_t *ptr = buffer;
    int nitem = 0;

    // --- Element 1: small ---
    const char *str1 = "apple";
    uint8_t len1 = strlen(str1);
    ptr[0] = len1;
    memcpy(ptr + 1, str1, len1);
    ptr += VARLENA_SIZE;
    nitem++;

    // --- Element 2: big ---
    const char *str2 = "banana_long_string_to_test_big_varlena";
    uint32_t len2 = strlen(str2);
    uint32_t offset2 = 50; 
    memcpy(area + offset2, str2, len2);
    
    varlena_set_big_offset_len(ptr, offset2, len2);
    ptr += VARLENA_SIZE;
    nitem++;

    // --- Element 3: small ---
    const char *str3 = "cherry";
    uint8_t len3 = strlen(str3);
    ptr[0] = len3;
    memcpy(ptr + 1, str3, len3);
    ptr += VARLENA_SIZE;
    nitem++;
    
    return nitem;
}

void test_add_and_test_varlena() {
    printf("--- Running test_add_and_test_varlena ---\n");
    
    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    assert(bf != NULL);

    uint8_t buffer[200];
    uint8_t area[200];
    int nitem = create_test_buffer(buffer, area);

    // Add all items from the buffer
    bloomfilter_add_varlena(bf, buffer, sizeof(buffer), VARLENA_SIZE, nitem, area, sizeof(area), NULL, 0);

    // Test if all added items exist
    bool results[nitem];
    bloomfilter_test_varlena(bf, buffer, sizeof(buffer), VARLENA_SIZE, nitem, area, sizeof(area), NULL, 0, results);
    
    for (int i = 0; i < nitem; i++) {
        assert(results[i]);
    }

    // Test for a non-existent item
    const char *str_not_exist = "grape";
    assert(!bloomfilter_test(bf, str_not_exist, strlen(str_not_exist)));

    bloomfilter_free(bf);
    printf("test_add_and_test_whole passed.\n\n");
}

void test_test_and_add_varlena() {
    printf("--- Running test_test_and_add_varlena ---\n");

    bloomfilter_t *bf = bloomfilter_init(1000, 3);
    assert(bf != NULL);

    uint8_t buffer[200];
    uint8_t area[200];
    int nitem = create_test_buffer(buffer, area);
    
    bool results1[nitem];
    bool results2[nitem];

    // First call: should report all items as non-existent and add them
    bloomfilter_test_and_add_varlena(bf, buffer, sizeof(buffer), VARLENA_SIZE, nitem, area, sizeof(area), NULL, 0, results2);
    for (int i = 0; i < nitem; i++) {
        assert(!results1[i]);
    }

    // Second call: should report all items as existent
    bloomfilter_test_and_add_varlena(bf, buffer, sizeof(buffer), VARLENA_SIZE, nitem, area, sizeof(area), NULL, 0, results2);
    for (int i = 0; i < nitem; i++) {
        assert(results2[i]);
    }

    bloomfilter_free(bf);
    printf("test_test_and_add_whole passed.\n\n");
}

int main() {
    test_add_and_test_varlena();
    test_test_and_add_varlena();
    printf("All bloom_varlena_test passed!\n");
    return 0;
}
