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

#ifndef MO_SORTED64_H
#define MO_SORTED64_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// filter is the validated little-endian [count][sorted uint64 values] payload.
bool mo_sorted64_contains(const void *filter, uint64_t key);

// Probe a fixed-width integer vector in one C call. Integer bytes are decoded
// by the same zero-extending little-endian contract as cbitmap / CRoaring.
void mo_sorted64_test_fixed(const void *filter, const void *key, size_t len,
                            size_t elemsz, size_t nitem, const void *nullmap,
                            size_t nullmaplen, void *result);

#endif  // MO_SORTED64_H
