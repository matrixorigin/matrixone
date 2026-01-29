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

#ifndef _BLOOM_H_
#define _BLOOM_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define BLOOM_MAGIC "XXBF"
#define MAX_K_SEED 64

/*
 * Bloom filter structure.
 * magic: magic number "XXBF"
 * nbits: total number of bits in the bitmap
 * seeds: seeds for 3 hash functions
 * bitmap: flexible array member for the bitmap data
 */

typedef struct {
    uint8_t magic[4];
    uint32_t k;
    uint64_t nbits;
    uint64_t seed;
    uint64_t bitmap[1];
} bloomfilter_t;

/*
 * Initializes a new bloom filter.
 * nbits: the number of bits in the bloom filter bitmap.
 * k: the number of hash functions to use.
 * Returns a pointer to the newly allocated bloomfilter_t, or NULL if it fails.
 */
bloomfilter_t* bloomfilter_init(uint64_t nbits, uint32_t k);

/*
 * Initializes a new bloom filter with a given seed.
 * nbits: the number of bits in the bloom filter bitmap.
 * k: the number of hash functions to use.
 * seed: the seed for hash functions.
 * Returns a pointer to the newly allocated bloomfilter_t, or NULL if it fails.
 */
bloomfilter_t* bloomfilter_init_with_seed(uint64_t nbits, uint32_t k, uint64_t seed);

/*
 * Frees the memory allocated for the bloom filter.
 */
void bloomfilter_free(bloomfilter_t *bf);

/*
 * Adds a single key of given length to the bloom filter.
 */
void bloomfilter_add(const bloomfilter_t *bf, const void *key, size_t len);

/*
 * Adds multiple fixed-length keys to the bloom filter.
 * key: pointer to the start of keys.
 * len: total length of the key buffer.
 * elemsz: size of each element.
 * nitem: number of items to add.
 * nullmap: optional bitmap indicating which items are NULL.
 * nullmaplen: length of the nullmap.
 */
void bloomfilter_add_fixed(const bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem, const void *nullmap, size_t nullmaplen);

/*
 * Tests if a key might be in the bloom filter.
 * Returns true if the key is potentially in the filter, false if it is definitely not.
 */
bool bloomfilter_test(const bloomfilter_t *bf, const void *key, size_t len);

/*
 * Tests multiple fixed-length keys and stores boolean results in the result buffer.
 */
void bloomfilter_test_fixed(const bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem, const void *nullmap, size_t nullmaplen, void *result);

/*
 * Tests multiple variable-length keys and stores boolean results in the result buffer.
 * Keys are expected to be prefixed with a 4-byte length.
 */
void bloomfilter_test_varlena_4b(const bloomfilter_t *bf, const void *key, size_t len, size_t nitem, const void *nullmap, size_t nullmaplen, void *result);

/*
 * Adds multiple variable-length keys to the bloom filter.
 * Keys are expected to be prefixed with a 4-byte length.
 */
void bloomfilter_add_varlena_4b(const bloomfilter_t *bf, const void *key, size_t len, size_t nitem, const void *nullmap, size_t nullmaplen);

/*
 * Tests if a key is in the bloom filter and then adds it.
 * Returns true if the key was already potentially in the filter.
 */
bool bloomfilter_test_and_add(const bloomfilter_t *bf, const void *key, size_t len);

/*
 * Tests and adds multiple fixed-length keys, storing results in the result buffer.
 */
void bloomfilter_test_and_add_fixed(const bloomfilter_t *bf, const void *key, size_t len, size_t elemsz, size_t nitem, const void *nullmap, size_t nullmaplen,void *result);

/*
 * Tests and adds multiple variable-length keys, storing results in the result buffer.
 */
void bloomfilter_test_and_add_varlena_4b(const bloomfilter_t *bf, const void *key, size_t len, size_t nitem, const void *nullmap, size_t nullmaplen,void *result);

/*
 * Returns a pointer to the raw bytes of the bloom filter for serialization.
 * len: will be set to the total length of the marshaled data.
 */
uint8_t* bloomfilter_marshal(const bloomfilter_t *bf, size_t *len);

/*
 * Returns a pointer to a bloom filter from a raw byte buffer.
 * No copy is performed.
 */
bloomfilter_t* bloomfilter_unmarshal(const uint8_t *buf, size_t len);

/*
 * Adds multiple variable-length keys to the bloom filter.
 * keys: pointer to the start of Varlena array.
 * area: pointer to the start of area buffer.
 */
void bloomfilter_add_varlena(const bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen);

/*
 * Tests multiple variable-length keys and stores boolean results in the result buffer.
 * keys: pointer to the start of Varlena array.
 * area: pointer to the start of area buffer.
 */
void bloomfilter_test_varlena(const bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen, void *result);

/*
 * Tests and adds multiple variable-length keys, storing results in the result buffer.
 * keys: pointer to the start of Varlena array.
 * area: pointer to the start of area buffer.
 */
void bloomfilter_test_and_add_varlena(const bloomfilter_t *bf, const void *keys, size_t len, size_t elemsz, size_t nitem, const void *area, size_t area_len, const void *nullmap, size_t nullmaplen, void *result);

/*
 * Merge two bloomfilter into dst. nbits, seed and k MUST be same
 */
int bloomfilter_or(bloomfilter_t *dst, const bloomfilter_t *a, const bloomfilter_t *b);

/*
 * Returns the number of bits in the bloom filter.
 */
static inline uint64_t bloomfilter_get_nbits(const bloomfilter_t *bf) {
    return bf->nbits;
}

/*
 * Returns the seed of the bloom filter.
 */
static inline uint64_t bloomfilter_get_seed(const bloomfilter_t *bf) {
    return bf->seed;
}

/*
 * Returns the number of hash functions used by the bloom filter.
 */
static inline uint32_t bloomfilter_get_k(const bloomfilter_t *bf) {
    return bf->k;
}

#endif /* _BLOOM_H_ */
