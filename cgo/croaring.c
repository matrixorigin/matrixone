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
#include <stdlib.h>

// Forward declarations of the CRoaring roaring64 C API we call. Declared here
// (instead of including the large amalgamated roaring.h, which requires C11) so
// this wrapper compiles cleanly under the cgo C99 toolchain; symbols resolve
// from libroaring.a at link. Pinned to CRoaring 4.7.0.
typedef struct roaring64_bitmap_s roaring64_bitmap_t;
roaring64_bitmap_t *roaring64_bitmap_create(void);
void roaring64_bitmap_free(roaring64_bitmap_t *r);
void roaring64_bitmap_add_many(roaring64_bitmap_t *r, size_t n_args,
                               const uint64_t *vals);
bool roaring64_bitmap_contains(const roaring64_bitmap_t *r, uint64_t val);
size_t roaring64_bitmap_portable_size_in_bytes(const roaring64_bitmap_t *r);
size_t roaring64_bitmap_portable_serialize(const roaring64_bitmap_t *r,
                                           char *buf);
roaring64_bitmap_t *roaring64_bitmap_portable_deserialize_safe(const char *buf,
                                                               size_t maxbytes);
uint64_t roaring64_bitmap_get_cardinality(const roaring64_bitmap_t *r);

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

void mo_croaring_add_fixed(void *r, const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap,
                           size_t nullmaplen) {
  (void)nullmaplen;
  if (!r || elemsz == 0 || nitem == 0) return;
  const unsigned char *p = (const unsigned char *)key;
  // Collect into a temp array and bulk-insert (roaring sorts + inserts in one
  // pass, which is much faster than per-value add).
  uint64_t *tmp = (uint64_t *)malloc(nitem * sizeof(uint64_t));
  if (!tmp) return;
  size_t cnt = 0;
  for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
    if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
    tmp[cnt++] = mo_decode_uint(p + j, elemsz);
  }
  roaring64_bitmap_add_many((roaring64_bitmap_t *)r, cnt, tmp);
  free(tmp);
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
