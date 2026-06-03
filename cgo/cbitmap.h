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

// Dense doc_id membership bitset, indexed by the doc_id VALUE (one bit per
// possible id), the integer-PK counterpart to cgo/croaring. The *_fixed entry
// points read a fixed-width integer column buffer directly in C (one cgo call
// per vector), mirroring mo_croaring_add_fixed / mo_croaring_test_fixed, so
// there is no per-row Go<->C crossing or Go-side value extraction.
//
// A dense bitset is sized to (max id + 1) bits, so it is only viable when the
// max id is bounded (see max_bits below); for sparse/large id ranges the caller
// falls back to the compact CRoaring filter.
#ifndef MO_CBITMAP_H
#define MO_CBITMAP_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// Build a dense bitset from a fixed-width integer column read directly from the
// vector's data buffer (one cgo call).
//   key:        pointer to the fixed column data (may be NULL when nitem == 0)
//   len:        total bytes of the data buffer
//   elemsz:     bytes per element (1/2/4/8)
//   nitem:      number of rows
//   nullmap:    optional MO null bitmap (uint64 words) or NULL
//   nullmaplen: bytes of nullmap
//   max_bits:   dense-bitset size cap in bits
//   use_offset: when nonzero, base the bitset at min(values) so its size is the
//               value SPAN (max-min) rather than the max value; 0 keeps the
//               legacy value-indexed layout (base 0).
// Returns NULL when the bit count would exceed max_bits (caller falls back to
// CRoaring) or on allocation failure. An empty input yields a valid empty
// bitset that matches nothing (not NULL).
void *mo_cbitmap_build_fixed(const void *key, size_t len, size_t elemsz,
                             size_t nitem, const void *nullmap,
                             size_t nullmaplen, uint64_t max_bits,
                             int use_offset);
void mo_cbitmap_free(void *f);

// Single membership test (value already decoded to uint64).
bool mo_cbitmap_contain(void *f, uint64_t val);

// Test a fixed-width integer column; result[i] = 1 if present, 0 if absent/null.
void mo_cbitmap_test_fixed(void *f, const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap,
                           size_t nullmaplen, void *result);

// Serialize into a freshly malloc'd buffer ([base u64][nbits u64][bitmap
// words]); *len gets the length. Caller frees with mo_cbitmap_free_buf. Returns
// NULL on OOM.
// The format is host-endian and only exchanged between same-architecture MO
// nodes (same as the build/probe data path).
uint8_t *mo_cbitmap_serialize(void *f, size_t *len);
void mo_cbitmap_free_buf(uint8_t *buf);

// Deserialize a buffer produced by mo_cbitmap_serialize; returns NULL on error.
void *mo_cbitmap_deserialize(const uint8_t *buf, size_t len);

#endif  // MO_CBITMAP_H
