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

#include "cbitmap.h"
#include "bitmap.h"
#include <stdlib.h>
#include <string.h>

// A dense bitset sized to nbits bits (words holds bitmap_size(nbits) uint64s).
// words is NULL when nbits == 0 (an empty filter that matches nothing).
typedef struct {
  uint64_t nbits;
  uint64_t *words;
} mo_cbitmap_t;

// Decode elemsz little-endian bytes (1/2/4/8) of a fixed integer into uint64 by
// zero-extension. MUST match mo_decode_uint in croaring.c and rawIntToUint64 in
// Go so the build and probe sides map a value to the same bit.
static inline uint64_t mo_cbm_decode(const unsigned char *p, size_t elemsz) {
  uint64_t x = 0;
  for (size_t b = 0; b < elemsz && b < 8; b++) {
    x |= ((uint64_t)p[b]) << (8 * b);
  }
  return x;
}

void *mo_cbitmap_build_fixed(const void *key, size_t len, size_t elemsz,
                             size_t nitem, const void *nullmap,
                             size_t nullmaplen, uint64_t max_bits) {
  (void)nullmaplen;
  const unsigned char *p = (const unsigned char *)key;

  // Pass 1: find the max non-null value (a dense bitset is sized by value).
  uint64_t maxv = 0;
  int any = 0;
  if (elemsz != 0) {
    for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
      if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
      uint64_t v = mo_cbm_decode(p + j, elemsz);
      if (!any || v > maxv) maxv = v;
      any = 1;
    }
  }
  // Feasibility gate: a dense bitset needs maxv+1 bits; bail (caller falls back
  // to the compact CRoaring filter) if that exceeds the cap.
  if (any && maxv + 1 > max_bits) return NULL;

  mo_cbitmap_t *f = (mo_cbitmap_t *)malloc(sizeof(mo_cbitmap_t));
  if (!f) return NULL;
  f->nbits = any ? (maxv + 1) : 0;
  f->words = NULL;
  uint64_t nwords = bitmap_size(f->nbits);  // (nbits+63)>>6; 0 when nbits == 0
  if (nwords) {
    f->words = (uint64_t *)calloc(nwords, sizeof(uint64_t));
    if (!f->words) {
      free(f);
      return NULL;
    }
  }

  // Pass 2: set a bit per non-null value.
  if (elemsz != 0 && f->words) {
    for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
      if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
      bitmap_set(f->words, mo_cbm_decode(p + j, elemsz));
    }
  }
  return f;
}

void mo_cbitmap_free(void *f) {
  if (!f) return;
  mo_cbitmap_t *b = (mo_cbitmap_t *)f;
  free(b->words);
  free(b);
}

bool mo_cbitmap_contain(void *f, uint64_t val) {
  if (!f) return false;
  mo_cbitmap_t *b = (mo_cbitmap_t *)f;
  if (val >= b->nbits) return false;
  return bitmap_test(b->words, val);
}

void mo_cbitmap_test_fixed(void *f, const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap,
                           size_t nullmaplen, void *result) {
  (void)nullmaplen;
  mo_cbitmap_t *b = (mo_cbitmap_t *)f;
  const unsigned char *p = (const unsigned char *)key;
  uint8_t *out = (uint8_t *)result;
  for (size_t i = 0, j = 0; i < nitem; i++, j += elemsz) {
    if (!b || (nullmap && bitmap_test((uint64_t *)nullmap, i)) || j >= len) {
      out[i] = 0;
      continue;
    }
    uint64_t v = mo_cbm_decode(p + j, elemsz);
    out[i] = (v < b->nbits && bitmap_test(b->words, v)) ? 1 : 0;
  }
}

uint8_t *mo_cbitmap_serialize(void *f, size_t *len) {
  mo_cbitmap_t *b = (mo_cbitmap_t *)f;
  uint64_t nwords = bitmap_size(b->nbits);
  size_t sz = sizeof(uint64_t) + (size_t)nwords * sizeof(uint64_t);
  uint8_t *buf = (uint8_t *)malloc(sz);
  if (!buf) {
    *len = 0;
    return NULL;
  }
  memcpy(buf, &b->nbits, sizeof(uint64_t));
  if (nwords) {
    memcpy(buf + sizeof(uint64_t), b->words, (size_t)nwords * sizeof(uint64_t));
  }
  *len = sz;
  return buf;
}

void mo_cbitmap_free_buf(uint8_t *buf) { free(buf); }

void *mo_cbitmap_deserialize(const uint8_t *buf, size_t len) {
  if (!buf || len < sizeof(uint64_t)) return NULL;
  uint64_t nbits = 0;
  memcpy(&nbits, buf, sizeof(uint64_t));
  uint64_t nwords = bitmap_size(nbits);
  if (len < sizeof(uint64_t) + (size_t)nwords * sizeof(uint64_t)) return NULL;

  mo_cbitmap_t *f = (mo_cbitmap_t *)malloc(sizeof(mo_cbitmap_t));
  if (!f) return NULL;
  f->nbits = nbits;
  f->words = NULL;
  if (nwords) {
    f->words = (uint64_t *)malloc((size_t)nwords * sizeof(uint64_t));
    if (!f->words) {
      free(f);
      return NULL;
    }
    memcpy(f->words, buf + sizeof(uint64_t), (size_t)nwords * sizeof(uint64_t));
  }
  return f;
}
