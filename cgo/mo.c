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

#include "mo_impl.h"

void Bitmap_Add(uint64_t *p, uint64_t pos) {
    bitmap_set(p, pos);
}
void Bitmap_Remove(uint64_t *p, uint64_t pos) {
    bitmap_clear(p, pos);
}
bool Bitmap_Contains(uint64_t *p, uint64_t pos) {
    return bitmap_test(p, pos);
}

bool Bitmap_IsEmpty(uint64_t *p, uint64_t nbits) {
    return bitmap_empty(p, nbits);
}
uint64_t Bitmap_Count(uint64_t *p, uint64_t nbits) {
    return bitmap_count(p, nbits);
}

void Bitmap_And(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits) {
    bitmap_and(dst, a, b, nbits);
}
void Bitmap_Or(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits) {
    bitmap_or(dst, a, b, nbits);
}
void Bitmap_Not(uint64_t *dst, uint64_t *a, uint64_t nbits) {
    bitmap_not(dst, a, nbits);
}
