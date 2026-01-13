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

#include "bloom.h"
#include "bitmap.h"
#include <stdlib.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

static void bloomfilter_setup(bloomfilter_t *bf, uint64_t nbits) {
    memcpy(bf->magic, BLOOM_MAGIC, 4);
    bf->nbits = nbits;
    for (int i = 0; i < 3; i++) {
        bf->seeds[i] = 0;
        for (int j = 0; j < 4; j++) {
            bf->seeds[i] = (bf->seeds[i] << 16) | (rand() & 0xFFFF);
        }
    }
}

bloomfilter_t* bloomfilter_init(uint64_t nbits) {
    uint64_t nbytes = bitmap_nbyte(nbits);
    bloomfilter_t *bf = (bloomfilter_t *)malloc(sizeof(bloomfilter_t) + nbytes);
    if (!bf) return NULL;
    
    memset(bf->bitmap, 0, nbytes);
    bloomfilter_setup(bf, nbits);
    return bf;
}

void bloomfilter_free(bloomfilter_t *bf) {
    if (bf) free(bf);
}

void bloomfilter_add(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return;

    uint64_t h1 = XXH3_64bits_withSeed(key, len, bf->seeds[0]);
    uint64_t h2 = XXH3_64bits_withSeed(key, len, bf->seeds[1]);
    uint64_t h3 = XXH3_64bits_withSeed(key, len, bf->seeds[2]);

    for (int i = 0; i < 3; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2 + (uint64_t)i * i * h3) % bf->nbits;
        bitmap_set((uint64_t *) bf->bitmap, pos);
    }
}

bool bloomfilter_test(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return false;

    uint64_t h1 = XXH3_64bits_withSeed(key, len, bf->seeds[0]);
    uint64_t h2 = XXH3_64bits_withSeed(key, len, bf->seeds[1]);
    uint64_t h3 = XXH3_64bits_withSeed(key, len, bf->seeds[2]);

    for (int i = 0; i < 3; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2 + (uint64_t)i * i * h3) % bf->nbits;
        if (!bitmap_test((uint64_t*)bf->bitmap, pos)) {
            return false;
        }
    }
    return true;
}

uint8_t* bloomfilter_marshal(const bloomfilter_t *bf, size_t *len) {
    if (memcmp(bf->magic, BLOOM_MAGIC, 4) != 0) {
        *len = 0;
        return NULL;
    }
    *len = sizeof(bloomfilter_t) + bitmap_nbyte(bf->nbits);
    return (uint8_t*)bf;
}

bloomfilter_t* bloomfilter_unmarshal(const uint8_t *buf, size_t len) {
    if (len < sizeof(bloomfilter_t)) {
	    return NULL;
    }
    bloomfilter_t *bf = (bloomfilter_t*) buf;
    if (memcmp(bf->magic, BLOOM_MAGIC, 4) != 0) {
        return NULL;
    }
    return bf;
}
