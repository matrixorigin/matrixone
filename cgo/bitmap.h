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

#ifndef _BITMAP_H_
#define _BITMAP_H_

#include "mo.h"
#include <string.h>

/*
 * Null bitmap operations.    Bitmap is an array of unit64_t and nbits of bits, nbits > 0.
 * Each of the nbits is a bit from position pos 0 to pos nbits - 1
 *
 * MUST MATCH GO IMPLEMENTATION.
 */

/* bitmap in number of bytes. */
static inline uint64_t bitmap_nbyte(uint64_t nbits) {
    return (nbits + 7) >> 3; 
}
/* return number of uint64 that can holds nbits */
static inline uint64_t bitmap_size(uint64_t nbits) {
    return (nbits + 63) >> 6;
}
/* return index into array of the bit position */
static inline uint64_t bitmap_pos2idx(uint64_t pos) {
    return pos >> 6;
}
/* return mask of the bit position */
static inline uint64_t bitmap_pos2mask(uint64_t pos) {
    uint64_t mask = 1;
    return mask << (pos & 63);
}

static inline bool bitmap_test(uint64_t *p, uint64_t pos) {
    return (p[bitmap_pos2idx(pos)] & bitmap_pos2mask(pos)) != 0;
}

static inline void bitmap_set(uint64_t *p, uint64_t pos) {
    p[bitmap_pos2idx(pos)] |= bitmap_pos2mask(pos);
}

static inline void bitmap_clear(uint64_t *p, uint64_t pos) {
    p[bitmap_pos2idx(pos)] &= ~bitmap_pos2mask(pos);
}

static inline void bitmap_zeros(uint64_t *p, uint64_t nbits) {
    memset(p, 0, bitmap_nbyte(nbits));
}

static inline void bitmap_ones(uint64_t *p, uint64_t nbits) {
    memset(p, 0xFF, bitmap_nbyte(nbits));
}

static inline void bitmap_copy(uint64_t *dst, uint64_t *src, uint64_t nbits) {
    memcpy(dst, src, bitmap_nbyte(nbits));
}

static inline void bitmap_and(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits) {
    uint64_t sz = bitmap_size(nbits);
    for (uint64_t i = 0; i < sz; ++i) {
        dst[i] = a[i] & b[i];
    }
}
static inline void bitmap_or(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits) {
    uint64_t sz = bitmap_size(nbits);
    for (uint64_t i = 0; i < sz; ++i) {
        dst[i] = a[i] | b[i];
    }
}

static inline void bitmap_not(uint64_t *dst, uint64_t *a, uint64_t nbits) {
    uint64_t sz = bitmap_size(nbits);
    for (uint64_t i = 0; i < sz; ++i) {
        dst[i] = ~a[i]; 
    }
}

static inline uint64_t bitmap_lastmask(uint64_t nbits) {
    uint64_t mask = bitmap_pos2mask(nbits - 1);
    /* 
     * For nbits = 0, 64, ...   mask will be 1 << 63. 
     * the following overflowed expression is the correct answer.
     */ 
    return (mask << 1) - 1;
}

static inline uint64_t bitmap_count(uint64_t *p, uint64_t nbits) {
    if (nbits == 0) {
        return 0;
    } else {
        uint64_t sz = bitmap_size(nbits);
        uint64_t mask = bitmap_lastmask(nbits);
        uint64_t cnt = __builtin_popcountll(p[sz - 1] & mask);
        for (uint64_t i = 0; (i + 1) < sz; ++i) {
            cnt += __builtin_popcountll(p[i]);
        }
        return cnt;
    }
}

static inline bool bitmap_empty(uint64_t *p, uint64_t nbits) {
    if (nbits == 0) {
        return true;
    } else {
        uint64_t sz = bitmap_size(nbits);
        uint64_t mask = bitmap_lastmask(nbits);
        if ((p[sz-1] & mask) != 0) {
            return false;
        }
        for (uint64_t i = 0; (i + 1) < sz; ++i) {
            if (p[i] != 0) {
                return false;
            }
        }
        return true;
    }
}

#endif /* _BITMAP_H_ */
