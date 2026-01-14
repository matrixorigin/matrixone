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
    uint64_t seeds[3];
    uint64_t bitmap[1];
} bloomfilter_t;

bloomfilter_t* bloomfilter_init(uint64_t nbits, uint32_t k);
void bloomfilter_free(bloomfilter_t *bf);
void bloomfilter_add(const bloomfilter_t *bf, const void *key, size_t len);
bool bloomfilter_test(const bloomfilter_t *bf, const void *key, size_t len);
uint8_t* bloomfilter_marshal(const bloomfilter_t *bf, size_t *len);
bloomfilter_t* bloomfilter_unmarshal(const uint8_t *buf, size_t len);

#endif /* _BLOOM_H_ */
