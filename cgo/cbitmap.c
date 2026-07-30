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

// WARNING: HOST-ENDIAN serialization (deliberate, for speed). mo_cbitmap_serialize
// memcpy's the [base][nbits][raw words] payload in the machine's native byte
// order, so a payload is only valid when exchanged between same-endianness MO
// nodes. This differs from the CRoaring filter, which uses CRoaring's PORTABLE
// serialization. All current MO targets are little-endian; this assert turns a
// big-endian build into a loud compile error rather than letting it silently
// emit payloads other nodes would misread. If MO ever runs on a big-endian or
// mixed-endian deployment, port mo_cbitmap_serialize/deserialize to explicit
// little-endian (de)serialization first.
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "cbitmap serialization is host-endian and assumes little-endian; port to portable (de)serialization before building for a big-endian target"
#endif

// A dense bitset over [base, base+nbits). bit i represents value base+i, so the
// structure is sized to the value SPAN, not the max value. When the builder is
// told not to offset, base is 0 and bit i == value i (legacy layout). words
// holds bitmap_size(nbits) uint64s; NULL when nbits == 0 (matches nothing).
typedef struct {
  uint64_t base;
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

int mo_cbitmap_build_fixed(const void *key, size_t len, size_t elemsz,
                           size_t nitem, const void *nullmap,
                           size_t nullmaplen, uint64_t max_bits,
                           int use_offset, void **out) {
  (void)nullmaplen;
  if (!out) return MO_CBITMAP_INVALID_INPUT;
  *out = NULL;
  // A non-empty column must have a supported integer width; reject anything
  // else rather than silently mis-decoding it.
  if (nitem != 0 &&
      elemsz != 1 && elemsz != 2 && elemsz != 4 && elemsz != 8) {
    return MO_CBITMAP_INVALID_INPUT;
  }
  const unsigned char *p = (const unsigned char *)key;

  // Pass 1: find the min and max non-null value. With use_offset the bitset is
  // based at min, so its size is the value SPAN (max-min); otherwise base is 0
  // and it is sized by max (legacy layout).
  uint64_t minv = 0, maxv = 0;
  int any = 0;
  if (elemsz != 0) {
    for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
      if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
      uint64_t v = mo_cbm_decode(p + j, elemsz);
      if (!any) {
        minv = maxv = v;
        any = 1;
      } else {
        if (v < minv) minv = v;
        if (v > maxv) maxv = v;
      }
    }
  }

  uint64_t base = use_offset ? minv : 0;
  // span = maxv - base (never overflows: maxv >= base); span+1 is the bit count.
  uint64_t span = any ? (maxv - base) : 0;
  // Feasibility gate: bail (caller falls back to the compact CRoaring filter)
  // if the bit count would exceed the cap. Checking span >= max_bits also
  // avoids the span+1 overflow for pathological ranges.
  if (any && span >= max_bits) return MO_CBITMAP_RANGE_TOO_LARGE;

  mo_cbitmap_t *f = (mo_cbitmap_t *)malloc(sizeof(mo_cbitmap_t));
  if (!f) return MO_CBITMAP_OOM;
  f->base = base;
  f->nbits = any ? (span + 1) : 0;
  f->words = NULL;
  uint64_t nwords = bitmap_size(f->nbits);  // (nbits+63)>>6; 0 when nbits == 0
  if (nwords) {
    f->words = (uint64_t *)calloc(nwords, sizeof(uint64_t));
    if (!f->words) {
      free(f);
      return MO_CBITMAP_OOM;
    }
  }

  // Pass 2: set a bit per non-null value, offset by base.
  if (elemsz != 0 && f->words) {
    for (size_t i = 0, j = 0; i < nitem && j < len; i++, j += elemsz) {
      if (nullmap && bitmap_test((uint64_t *)nullmap, i)) continue;
      bitmap_set(f->words, mo_cbm_decode(p + j, elemsz) - base);
    }
  }
  *out = f;
  return MO_CBITMAP_OK;
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
  if (val < b->base) return false;  // below base also guards the subtraction
  uint64_t idx = val - b->base;
  if (idx >= b->nbits) return false;
  return bitmap_test(b->words, idx);
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
    if (v < b->base) {
      out[i] = 0;
      continue;
    }
    uint64_t idx = v - b->base;
    out[i] = (idx < b->nbits && bitmap_test(b->words, idx)) ? 1 : 0;
  }
}

uint8_t *mo_cbitmap_serialize(void *f, size_t *len) {
  mo_cbitmap_t *b = (mo_cbitmap_t *)f;
  uint64_t nwords = bitmap_size(b->nbits);
  // Header: [base u64][nbits u64], then the bitmap words.
  size_t sz = 2 * sizeof(uint64_t) + (size_t)nwords * sizeof(uint64_t);
  uint8_t *buf = (uint8_t *)malloc(sz);
  if (!buf) {
    *len = 0;
    return NULL;
  }
  memcpy(buf, &b->base, sizeof(uint64_t));
  memcpy(buf + sizeof(uint64_t), &b->nbits, sizeof(uint64_t));
  if (nwords) {
    memcpy(buf + 2 * sizeof(uint64_t), b->words,
           (size_t)nwords * sizeof(uint64_t));
  }
  *len = sz;
  return buf;
}

void mo_cbitmap_free_buf(uint8_t *buf) { free(buf); }

void *mo_cbitmap_deserialize(const uint8_t *buf, size_t len) {
  if (!buf || len < 2 * sizeof(uint64_t)) return NULL;
  uint64_t base = 0, nbits = 0;
  memcpy(&base, buf, sizeof(uint64_t));
  memcpy(&nbits, buf + sizeof(uint64_t), sizeof(uint64_t));

  // Derive the word count from the ACTUAL payload length and require nbits to be
  // exactly consistent with it, rather than trusting nbits to size the read.
  // Computing bitmap_size(nbits) = (nbits+63)>>6 from an untrusted nbits
  // OVERFLOWS for large nbits (e.g. nbits=MaxUint64 -> nwords=0), so a forged
  // 16-byte payload would otherwise be accepted as a "valid" filter that then
  // crashes / matches nothing on probe. Reject any header that does not match.
  size_t payload = len - 2 * sizeof(uint64_t);
  if (payload % sizeof(uint64_t) != 0) return NULL;  // not a whole number of words
  uint64_t nwords = (uint64_t)(payload / sizeof(uint64_t));
  // ceil(nbits/64) must equal nwords, checked WITHOUT the nbits+63 overflow: for
  // nwords words, nbits must lie in ((nwords-1)*64, nwords*64]; nbits==0 iff
  // nwords==0. nwords comes from len, so nwords*64 cannot overflow here.
  if (nbits == 0) {
    if (nwords != 0) return NULL;
  } else {
    if (nwords == 0) return NULL;
    if (nbits > nwords * 64) return NULL;
    if (nbits <= (nwords - 1) * 64) return NULL;
  }

  mo_cbitmap_t *f = (mo_cbitmap_t *)malloc(sizeof(mo_cbitmap_t));
  if (!f) return NULL;
  f->base = base;
  f->nbits = nbits;
  f->words = NULL;
  if (nwords) {
    f->words = (uint64_t *)malloc((size_t)nwords * sizeof(uint64_t));
    if (!f->words) {
      free(f);
      return NULL;
    }
    memcpy(f->words, buf + 2 * sizeof(uint64_t),
           (size_t)nwords * sizeof(uint64_t));
  }
  return f;
}
