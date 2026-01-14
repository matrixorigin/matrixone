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

#define MAX_STACK_K 64

typedef struct {
    uint64_t h1;
    uint64_t h2;
    uint64_t h3;
} bloom_hash_t;

static inline bloom_hash_t bloom_calculate_hash(const void *key, size_t len, uint64_t seed) {
    bloom_hash_t h;
    h.h1 = XXH3_64bits_withSeed(key, len, seed);
    h.h2 = XXH3_64bits_withSeed(key, len, seed << 32);
    h.h3 = XXH3_64bits_withSeed(key, len, seed >> 32);
    return h;
}

static inline uint64_t bloom_calculate_pos(bloom_hash_t h, int i, uint64_t nbits) {
    return (h.h1 + (uint64_t)i * h.h2 + (uint64_t)i * i * h.h3) % nbits;
}

static void bloomfilter_setup(bloomfilter_t *bf, uint64_t nbits, uint32_t k) {
    memcpy(bf->magic, BLOOM_MAGIC, 4);
    bf->nbits = nbits;
    bf->k = k;
    for (int i = 0; i < k; i++) {
        bf->seeds[i] = 0;
        for (int j = 0; j < 4; j++) {
            bf->seeds[i] = (bf->seeds[i] << 16) | (rand() & 0xFFFF);
        }
    }
}

bloomfilter_t* bloomfilter_init(uint64_t nbits, uint32_t k) {
    uint64_t nbytes = bitmap_nbyte(nbits);
    bloomfilter_t *bf = (bloomfilter_t *)malloc(sizeof(bloomfilter_t) + nbytes);
    if (!bf) return NULL;
    
    memset(bf->bitmap, 0, nbytes);
    bloomfilter_setup(bf, nbits, k);
    pthread_mutex_init(&bf->mutex, NULL);
    return bf;
}

void bloomfilter_free(bloomfilter_t *bf) {
    if (bf) {
        pthread_mutex_destroy(&bf->mutex);
        free(bf);
    }
}

void bloomfilter_add(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return;

    uint64_t stack_pos[MAX_STACK_K];
    uint64_t *pos = stack_pos;
    if (bf->k > MAX_STACK_K) {
        pos = (uint64_t*)malloc(sizeof(uint64_t) * bf->k);
        if (!pos) return;
    }

    for (int i = 0; i < bf->k; i++) {
        bloom_hash_t h = bloom_calculate_hash(key, len, bf->seeds[i]);
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
    }

    pthread_mutex_lock((pthread_mutex_t*)&bf->mutex);
    for (int i = 0; i < bf->k; i++) {
        bitmap_set((uint64_t *) bf->bitmap, pos[i]);
    }
    pthread_mutex_unlock((pthread_mutex_t*)&bf->mutex);

    if (pos != stack_pos) free(pos);
}

bool bloomfilter_test(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return false;

    uint64_t stack_pos[MAX_STACK_K];
    uint64_t *pos = stack_pos;
    if (bf->k > MAX_STACK_K) {
        pos = (uint64_t*)malloc(sizeof(uint64_t) * bf->k);
        if (!pos) return false;
    }

    for (int i = 0; i < bf->k; i++) {
        bloom_hash_t h = bloom_calculate_hash(key, len, bf->seeds[i]);
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
    }

    bool result = true;
    pthread_mutex_lock((pthread_mutex_t*)&bf->mutex);
    for (int i = 0; i < bf->k; i++) {
        if (!bitmap_test((uint64_t*)bf->bitmap, pos[i])) {
            result = false;
            break;
        }
    }
    pthread_mutex_unlock((pthread_mutex_t*)&bf->mutex);

    if (pos != stack_pos) free(pos);
    return result;
}

bool bloomfilter_test_and_add(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return false;

    uint64_t stack_pos[MAX_STACK_K];
    uint64_t *pos = stack_pos;
    if (bf->k > MAX_STACK_K) {
        pos = (uint64_t*)malloc(sizeof(uint64_t) * bf->k);
        if (!pos) return false;
    }

    for (int i = 0; i < bf->k; i++) {
        bloom_hash_t h = bloom_calculate_hash(key, len, bf->seeds[i]);
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
    }

    bool all_set = true;
    pthread_mutex_lock((pthread_mutex_t*)&bf->mutex);
    for (int i = 0; i < bf->k; i++) {
        if (!bitmap_test((uint64_t*)bf->bitmap, pos[i])) {
            all_set = false;
            bitmap_set((uint64_t*)bf->bitmap, pos[i]);
        }
    }
    pthread_mutex_unlock((pthread_mutex_t*)&bf->mutex);

    if (pos != stack_pos) free(pos);
    return all_set;
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
    pthread_mutex_init(&bf->mutex, NULL);
    return bf;
}
