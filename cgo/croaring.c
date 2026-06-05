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

#include "croaring.h"
#include "bitmap.h"
#include "roaring.h"
#include <stdlib.h>

// Include the real CRoaring header (the amalgamated roaring.h) so the roaring64
// prototypes we call are the single source of truth: any upstream signature
// change is a compile error here, not a silent ABI mismatch at link. roaring.h
// requires C11 (it uses <stdatomic.h>), which is why the cgo C build is C11.
// No version assert by design: a CRoaring upgrade compiles cleanly as long as
// the signatures we use are unchanged, so bumps stay frictionless and a real
// incompatibility still surfaces as a compile error.

// Decode elemsz little-endian bytes (1/2/4/8) of a fixed integer into uint64
// by zero-extension. Identical decode on build and probe keeps mapping stable.
static inline uint64_t mo_decode_uint(const unsigned char *p, size_t elemsz) {
  uint64_t x = 0;
  for (size_t b = 0; b < elemsz && b < 8; b++) {
    x |= ((uint64_t)p[b]) << (8 * b);
  }
  return x;
}

void *mo_croaring_create(void) { return (void *)roaring64_bitmap_create(); }

void mo_croaring_free(void *r) {
  if (r) roaring64_bitmap_free((roaring64_bitmap_t *)r);
}

// Convert eligible containers to run-length encoding (helps clustered/
// consecutive id sets). Returns true if the representation changed.
bool mo_croaring_run_optimize(void *r) {
  if (!r) return false;
  return roaring64_bitmap_run_optimize((roaring64_bitmap_t *)r);
}

bool mo_croaring_add_fixed(void *r, const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap,
                           size_t nullmaplen) {
  (void)nullmaplen;
  if (!r) return false;                        // no bitmap -> caller must error
  if (elemsz == 0 || nitem == 0) return true;  // nothing to add
  const unsigned char *p = (const unsigned char *)key;
  // Bulk-insert in fixed-size batches via a stack buffer: roaring sorts +
  // inserts each batch in one pass (far faster than per-value add) WITHOUT the
  // previous O(nitem) heap temp, whose silent NULL-on-OOM could leave the bitmap
  // empty and then serialize as a valid-looking but membership-empty filter
  // (dropping all matching rows). The stack buffer cannot fail to allocate.
  enum { CHUNK = 2048 };
  uint64_t buf[CHUNK];
  size_t cnt = 0;
  for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
    if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
    buf[cnt++] = mo_decode_uint(p + j, elemsz);
    if (cnt == CHUNK) {
      roaring64_bitmap_add_many((roaring64_bitmap_t *)r, cnt, buf);
      cnt = 0;
    }
  }
  if (cnt) roaring64_bitmap_add_many((roaring64_bitmap_t *)r, cnt, buf);
  return true;
}

bool mo_croaring_contains(void *r, uint64_t val) {
  if (!r) return false;
  return roaring64_bitmap_contains((const roaring64_bitmap_t *)r, val);
}

void mo_croaring_test_fixed(void *r, const void *key, size_t len, size_t elemsz,
                            size_t nitem, const void *nullmap,
                            size_t nullmaplen, void *result) {
  (void)nullmaplen;
  const roaring64_bitmap_t *b = (const roaring64_bitmap_t *)r;
  const unsigned char *p = (const unsigned char *)key;
  uint8_t *out = (uint8_t *)result;
  for (size_t i = 0, j = 0; i < nitem; i++, j += elemsz) {
    if ((nullmap && bitmap_test((uint64_t *)nullmap, i)) || j >= len) {
      out[i] = 0;
      continue;
    }
    out[i] = roaring64_bitmap_contains(b, mo_decode_uint(p + j, elemsz)) ? 1 : 0;
  }
}

uint8_t *mo_croaring_serialize(void *r, size_t *len) {
  const roaring64_bitmap_t *b = (const roaring64_bitmap_t *)r;
  size_t sz = roaring64_bitmap_portable_size_in_bytes(b);
  uint8_t *buf = (uint8_t *)malloc(sz);
  if (!buf) {
    *len = 0;
    return NULL;
  }
  roaring64_bitmap_portable_serialize(b, (char *)buf);
  *len = sz;
  return buf;
}

void mo_croaring_free_buf(uint8_t *buf) { free(buf); }

void *mo_croaring_deserialize(const uint8_t *buf, size_t len) {
  return (void *)roaring64_bitmap_portable_deserialize_safe((const char *)buf,
                                                           len);
}

uint64_t mo_croaring_cardinality(void *r) {
  if (!r) return 0;
  return roaring64_bitmap_get_cardinality((const roaring64_bitmap_t *)r);
}
