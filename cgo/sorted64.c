// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sorted64.h"
#include "bitmap.h"

#include <stdlib.h>

static inline uint64_t mo_sorted64_decode_le(const uint8_t *p, size_t len) {
  uint64_t value = 0;
  for (size_t i = 0; i < len && i < sizeof(value); i++) {
    value |= ((uint64_t)p[i]) << (8 * i);
  }
  return value;
}

bool mo_sorted64_contains(const void *filter, uint64_t key) {
  if (!filter) return false;
  const uint8_t *data = (const uint8_t *)filter;
  uint64_t count64 = mo_sorted64_decode_le(data, sizeof(uint64_t));
  if (count64 > SIZE_MAX) return false;
  size_t lo = 0;
  size_t hi = (size_t)count64;
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    uint64_t value = mo_sorted64_decode_le(
        data + sizeof(uint64_t) * (mid + 1), sizeof(uint64_t));
    if (value < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo >= (size_t)count64) return false;
  uint64_t value = mo_sorted64_decode_le(
      data + sizeof(uint64_t) * (lo + 1), sizeof(uint64_t));
  return value == key;
}

void mo_sorted64_test_fixed(const void *filter, const void *key, size_t len,
                            size_t elemsz, size_t nitem, const void *nullmap,
                            size_t nullmaplen, void *result) {
  (void)nullmaplen;
  const uint8_t *data = (const uint8_t *)key;
  uint8_t *out = (uint8_t *)result;
  for (size_t i = 0, offset = 0; i < nitem; i++, offset += elemsz) {
    if (!filter || offset >= len ||
        (nullmap && bitmap_test((uint64_t *)nullmap, i))) {
      out[i] = 0;
      continue;
    }
    uint64_t value = mo_sorted64_decode_le(data + offset, elemsz);
    out[i] = mo_sorted64_contains(filter, value) ? 1 : 0;
  }
}

static int mo_sorted64_cmp(const void *a, const void *b) {
  uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
  return (x > y) - (x < y);
}

uint64_t *mo_sorted64_build_fixed(const void *key, size_t len, size_t elemsz,
                                  size_t nitem, const void *nullmap,
                                  size_t nullmaplen) {
  (void)nullmaplen;
  if (elemsz != 1 && elemsz != 2 && elemsz != 4 && elemsz != 8) return NULL;
  if (nitem > (SIZE_MAX / sizeof(uint64_t)) - 1) return NULL;
  uint64_t *payload = (uint64_t *)malloc(sizeof(uint64_t) * (nitem + 1));
  if (!payload) return NULL;
  const uint8_t *data = (const uint8_t *)key;
  uint64_t *values = payload + 1;
  size_t n = 0;
  for (size_t i = 0, offset = 0; i < nitem; i++, offset += elemsz) {
    if (offset + elemsz > len || (nullmap && bitmap_test((uint64_t *)nullmap, i))) continue;
    values[n++] = mo_sorted64_decode_le(data + offset, elemsz);
  }
  qsort(values, n, sizeof(uint64_t), mo_sorted64_cmp);
  size_t unique = 0;
  for (size_t i = 0; i < n; i++) {
    if (unique == 0 || values[unique - 1] != values[i]) values[unique++] = values[i];
  }
  payload[0] = unique;
  return payload;
}

void mo_sorted64_free(uint64_t *payload) { free(payload); }
