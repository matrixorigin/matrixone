// Copyright 2021 - 2022 Matrix Origin
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

// Thin C wrapper over the CRoaring roaring64 API for the doc_id membership
// filter. The *_fixed entry points read a fixed-width integer column buffer
// directly in C (one cgo call per vector), mirroring bloomfilter_*_fixed, so
// there is no per-row Go<->C crossing or Go-side value extraction.
#ifndef MO_CROARING_H
#define MO_CROARING_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

void *mo_croaring_create(void);
void mo_croaring_free(void *r);

// Add a fixed-width integer column read directly from the vector's data buffer.
//   key:        pointer to the fixed column data
//   len:        total bytes of the data buffer
//   elemsz:     bytes per element (1/2/4/8)
//   nitem:      number of rows
//   nullmap:    optional MO null bitmap (uint64 words) or NULL
//   nullmaplen: bytes of nullmap
void mo_croaring_add_fixed(void *r, const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap, size_t nullmaplen);

// Single membership test (value already decoded to uint64).
bool mo_croaring_contains(void *r, uint64_t val);

// Test a fixed-width integer column; result[i] = 1 if present, 0 if absent/null.
void mo_croaring_test_fixed(void *r, const void *key, size_t len, size_t elemsz,
                            size_t nitem, const void *nullmap,
                            size_t nullmaplen, void *result);

// Portable-serialize into a freshly malloc'd buffer; *len gets the length.
// Caller frees with mo_croaring_free_buf. Returns NULL on OOM.
uint8_t *mo_croaring_serialize(void *r, size_t *len);
void mo_croaring_free_buf(uint8_t *buf);

// Portable-deserialize; returns NULL on error.
void *mo_croaring_deserialize(const uint8_t *buf, size_t len);

uint64_t mo_croaring_cardinality(void *r);

// Run-length-optimize eligible containers (helps clustered/consecutive id sets).
// Returns true if the representation changed.
bool mo_croaring_run_optimize(void *r);

#endif  // MO_CROARING_H
