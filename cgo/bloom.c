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
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

typedef struct {
    uint64_t h1;
    uint64_t h2;
} bloom_hash_t;

#include "varlena.h"

static inline bloom_hash_t bloom_calculate_hash64(uint64_t key, uint64_t seed) {
    bloom_hash_t h;
    XXH128_hash_t xh1 = XXH3_128bits_withSeed(&key, sizeof(uint64_t), seed);
    h.h1 = xh1.low64;
    h.h2 = xh1.high64;
    return h;
}

/*
 * Calculates a 128-bit hash (split into two 64-bit halves) for a given key and seed using XXH3.
 */
static inline bloom_hash_t bloom_calculate_hash(const void *key, size_t len, uint64_t seed) {

    // force cast byte, int16, int32 into int64 so that same value share the same hash value
    switch (len) {
        case 1: // int8
            return bloom_calculate_hash64((int64_t)(*(int8_t*)key), seed);
        case 2: // int16
            return bloom_calculate_hash64((int64_t)(*(int16_t*)key), seed);
        case 4: // int32
            return bloom_calculate_hash64((int64_t)(*(int32_t*)key), seed);
        case 8: // int64 / uint64 / double
            return bloom_calculate_hash64(*(uint64_t*)key, seed);
        default:
            {
                bloom_hash_t h;
                XXH128_hash_t xh1 = XXH3_128bits_withSeed(key, len, seed);
                h.h1 = xh1.low64;
                h.h2 = xh1.high64;
                return h;
            }
            break;
    }

}

/*
 * Calculates the bit position in the bloom filter for a given hash and iteration index.
 * Uses the double hashing technique: (h1 + i * h2) % nbits.
 */
static inline uint64_t bloom_calculate_pos(bloom_hash_t h, int i, uint64_t nbits) {
    return (h.h1 + (uint64_t)i * h.h2) & (nbits-1);
}

/*
 * Sets up the bloom filter structure with the given bit count and number of hash functions.
 */
static void bloomfilter_setup(bloomfilter_t *bf, uint64_t nbits, uint32_t k, uint64_t seed) {
    memcpy(bf->magic, BLOOM_MAGIC, 4);
    bf->nbits = nbits;
    bf->k = k;
    bf->seed = seed;
}

static uint64_t next_pow2_64(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

bloomfilter_t* bloomfilter_init(uint64_t nbits, uint32_t k) {
    uint64_t new_nbits = next_pow2_64(nbits);
    uint64_t nbytes = bitmap_nbyte(new_nbits);

    if (k > MAX_K_SEED) return NULL;

    bloomfilter_t *bf = (bloomfilter_t *)malloc(sizeof(bloomfilter_t) + nbytes);
    if (!bf) return NULL;

    memset(bf->bitmap, 0, nbytes);
    uint64_t seed = 0;
    for (int j = 0; j < 4; j++) {
        seed = (seed << 16) | (rand() & 0xFFFF);
    }
    bloomfilter_setup(bf, new_nbits, k, seed);
    return bf;
}

bloomfilter_t* bloomfilter_init_with_seed(uint64_t nbits, uint32_t k, uint64_t seed) {
    uint64_t new_nbits = next_pow2_64(nbits);
    uint64_t nbytes = bitmap_nbyte(new_nbits);

    if (k > MAX_K_SEED) return NULL;

    bloomfilter_t *bf = (bloomfilter_t *)malloc(sizeof(bloomfilter_t) + nbytes);
    if (!bf) return NULL;

    memset(bf->bitmap, 0, nbytes);
    bloomfilter_setup(bf, new_nbits, k, seed);
    return bf;
}

void bloomfilter_free(bloomfilter_t *bf) {
    if (bf) {
        free(bf);
    }
}

void bloomfilter_add(bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return;

    uint64_t stack_pos[MAX_K_SEED];
    uint64_t *pos = stack_pos;

    bloom_hash_t h = bloom_calculate_hash(key, len, bf->seed);
    for (int i = 0; i < bf->k; i++) {
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
        bitmap_set((uint64_t *) bf->bitmap, pos[i]);
    }
}

void bloomfilter_add_fixed(bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem, const void *nullmap, size_t nullmaplen) {
    char *k = (char *) key;
    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz, k += elemsz) {
        if (!nullmap || !bitmap_test((uint64_t *) nullmap, i)) {
            bloomfilter_add(bf, k, elemsz);
        }
    }
}

void bloomfilter_add_varlena_4b(bloomfilter_t *bf, const void *key, size_t len, size_t nitem, const void *nullmap, size_t nullmaplen) {
    char *k = (char *) key;
    char *start = k;

    for (int i = 0; i < nitem; i++) {
        if ((size_t)(k - start) + sizeof(uint32_t) > len) break;
        uint32_t elemsz = *((uint32_t*)k);
        k += sizeof(uint32_t);

        if ((size_t)(k - start) + elemsz > len) break;

        if (!nullmap || !bitmap_test((uint64_t *) nullmap, i)) {
             bloomfilter_add(bf, k, elemsz);
        }
        k += elemsz;
    }
}

bool bloomfilter_test(const bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return false;

    uint64_t stack_pos[MAX_K_SEED];
    uint64_t *pos = stack_pos;

    bloom_hash_t h = bloom_calculate_hash(key, len, bf->seed);
    for (int i = 0; i < bf->k; i++) {
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
    }

    bool result = true;
    for (int i = 0; i < bf->k; i++) {
        if (!bitmap_test((uint64_t*)bf->bitmap, pos[i])) {
            result = false;
            break;
        }
    }

    return result;
}

void bloomfilter_test_fixed(const bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem, const void *nullmap, size_t nullmaplen, void *result) {
    char *k = (char *) key;
    bool *br = (bool *) result;

    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz, k += elemsz) {
        if (nullmap && bitmap_test((uint64_t*)nullmap, i)) {
            // null
            br[i] = false;
        } else {
            br[i] = bloomfilter_test(bf, k, elemsz);
        }
    }
}

/*
 * key contain the lists of varlena items.
 * first 4 byte (uint32) contains the size of the content
 * and then follow with the content
 * format of the keys look likes [size0] [data with size0] [size1] [data with size1]...
 */
void bloomfilter_test_varlena_4b(const bloomfilter_t *bf, const void *key, size_t len, size_t nitem, const void *nullmap, size_t nullmaplen, void *result) {
    char *k = (char *) key;
    char *start = k;
    bool *br = (bool *) result;

    for (int i = 0; i < nitem; i++) {
        if ((size_t)(k - start) + sizeof(uint32_t) > len) break;
	    uint32_t elemsz = *((uint32_t*)k);
	    k += sizeof(uint32_t);
        
        if ((size_t)(k - start) + elemsz > len) break;

        if (nullmap && bitmap_test((uint64_t*)nullmap, i)) {
            // null
            br[i] = false;
        } else {
            br[i] = bloomfilter_test(bf, k, elemsz);
        }
        k += elemsz;
    }
}

bool bloomfilter_test_and_add(bloomfilter_t *bf, const void *key, size_t len) {
    if (bf->nbits == 0) return false;

    uint64_t stack_pos[MAX_K_SEED];
    uint64_t *pos = stack_pos;

    bloom_hash_t h = bloom_calculate_hash(key, len, bf->seed);
    for (int i = 0; i < bf->k; i++) {
        pos[i] = bloom_calculate_pos(h, i, bf->nbits);
    }

    bool all_set = true;
    for (int i = 0; i < bf->k; i++) {
        if (!bitmap_test((uint64_t*)bf->bitmap, pos[i])) {
            all_set = false;
            bitmap_set((uint64_t*)bf->bitmap, pos[i]);
        }
    }

    return all_set;
}

void bloomfilter_test_and_add_fixed(bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem,  const void *nullmap, size_t nullmaplen, void *result) {
    char *k = (char *) key;
    bool *br = (bool *) result;
    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz, k += elemsz) {
        if (nullmap && bitmap_test((uint64_t*)nullmap, i)) {
            // null
            br[i] = false;
        } else {
            br[i] = bloomfilter_test_and_add(bf, k, elemsz);
        }
    }
}

void bloomfilter_test_and_add_varlena_4b(bloomfilter_t *bf, const void *key, size_t len, size_t nitem,  const void *nullmap, size_t nullmaplen, void *result) {
    char *k = (char *) key;
    char *start = k;
    bool *br = (bool *) result;
    for (int i = 0; i < nitem; i++) {
        if ((size_t)(k - start) + sizeof(uint32_t) > len) break;
        uint32_t elemsz = *((uint32_t*)k);
        k += sizeof(uint32_t);

        if ((size_t)(k - start) + elemsz > len) break;

        if (nullmap && bitmap_test((uint64_t*)nullmap, i)) {
            // null
            br[i] = false;
        } else {
            br[i] = bloomfilter_test_and_add(bf, k, elemsz);
        }
        k += elemsz;
    }
}

void bloomfilter_add_varlena(bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen) {
    const uint8_t *v = (const uint8_t *)keys;

    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
        uint32_t vlen;
        const uint8_t *data = varlena_get_byte_slice(v, area, &vlen);
        if (!nullmap || !bitmap_test((uint64_t *) nullmap, i)) {
            bloomfilter_add(bf, data, vlen);
        }
        v += elemsz;
    }
}

void bloomfilter_test_varlena(const bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen, void *result) {
    const uint8_t *v = (const uint8_t *)keys;
    bool *br = (bool *) result;

    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
        uint32_t vlen;
        const uint8_t *data = varlena_get_byte_slice(v, area, &vlen);
        if (nullmap && bitmap_test((uint64_t *) nullmap, i)) {
            br[i] = false;
        } else {
            br[i] = bloomfilter_test(bf, data, vlen);
        }
        v += elemsz;
    }
}

void bloomfilter_test_and_add_varlena(bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen, void *result) {
    const uint8_t *v = (const uint8_t *)keys;
    bool *br = (bool *) result;

    for (int i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
        uint32_t vlen;
        const uint8_t *data = varlena_get_byte_slice(v, area, &vlen);
        if (nullmap && bitmap_test((uint64_t *) nullmap, i)) {
            br[i] = false;
        } else {
            br[i] = bloomfilter_test_and_add(bf, data, vlen);
        }
        v += elemsz;
    }
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

int bloomfilter_or(bloomfilter_t *dst, const bloomfilter_t *a, const bloomfilter_t *b) {

    if (!(a->nbits == b->nbits && a->nbits == dst->nbits)) {
        return 1;
    }

    if (!(a->seed == b->seed && a->seed == dst->seed)) {
        return 2;
    }

    if (!(a->k == b->k && a->k == dst->k)) {
        return 3;
    }

    bitmap_or((uint64_t*)dst->bitmap, (uint64_t*)a->bitmap, (uint64_t*)b->bitmap, dst->nbits);
    return 0;
}

