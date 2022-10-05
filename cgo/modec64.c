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

#include "modecimal.h"
#include <endian.h>
#include <assert.h>

// first bit is sign bit
#define SIGN_BIT (1ULL << 63)
// next 5 bits is for how many dec digits.
#define DEC_DIGITS_SHIFT 58
#define DEC_DIGITS_BITS (31ULL << DEC_DIGITS_SHIFT)

// bits need to encode n decimal digits.
#define LOWER30_BITS (0x3FFFFFFF)
static const uint32_t digits_to_bits[10] = {0, 4, 7, 10, 14, 17, 20, 24, 27, 30};
static const uint32_t digits_bitmask[10] = {
    0,
    (1 << 4) - 1,
    (1 << 7) - 1,
    (1 << 10) - 1,
    (1 << 14) - 1,
    (1 << 17) - 1,
    (1 << 20) - 1,
    (1 << 24) - 1,
    (1 << 27) - 1,
    (1 << 30) - 1,
};

#define DEC_NEED(D, B)  \
    } else if (d < (D)) { return (B)

static inline uint32_t dec_need_bits(uint32_t d) {
    if (d == 0) { return 0;  
    DEC_NEED(10, 4);
    DEC_NEED(100, 7);
    DEC_NEED(1000, 10);
    DEC_NEED(10000, 14);
    DEC_NEED(100000, 17);
    DEC_NEED(1000000, 20);
    DEC_NEED(10000000, 24);
    DEC_NEED(100000000, 27);
    DEC_NEED(1000000000, 30);
    } else { assert(0); return 0; }
}

static inline uint32_t dec_need_digits(uint32_t d) {
    if (d == 0) { return 0;  
    DEC_NEED(10, 1);
    DEC_NEED(100, 2);
    DEC_NEED(1000, 3);
    DEC_NEED(10000, 4);
    DEC_NEED(100000, 5);
    DEC_NEED(1000000, 6);
    DEC_NEED(10000000, 7);
    DEC_NEED(100000000, 8);
    DEC_NEED(1000000000, 9);
    } else { assert(0); return 0; }
}

static inline uint32_t mul_pow10(uint32_t u, uint32_t pow) {
    switch (pow) {
        case 0:
            return u;
        case 1:
            return u * 10;
        case 2:
            return u * 100;
        case 3:
            return u * 1000;
        case 4: 
            return u * 10000;
        case 5: 
            return u * 100000;
        case 6:
            return u * 1000000;
        case 7:
            return u * 10000000;
        case 8:
            return u * 100000000;
        case 9:
            return u * 1000000000;
        default:
            // should never be here.
            assert(0);
            return 0;
    }
}

/* unpack d into p.  *d is a little endian uint64 */
static inline void unpack64(modec_ctxt_t *ctxt, 
                            modec64_unpacked_t *p, const modec64_t *d) {
    // convert to host representation
    uint64_t u64 = le64toh(*d);

    // sign -- note that sign bit 0 means neg, 1 means pos.  
    // Unpack sign bit and revert bits for neg*/
    if ((u64 & SIGN_BIT) == 0) {
        p->sign = -1;
        u64 = ~u64;
    } else {
        p->sign = 1;
        u64 &= ~SIGN_BIT;
    }

    // number of dec digits.
    p->ndigits = (u64 & DEC_DIGITS_BITS) >> DEC_DIGITS_SHIFT;
    u64 &= ~DEC_DIGITS_BITS;
    // should never happend, all dec64 are built/packed by us.
    assert(p->ndigits <= 17);

    int intpbits = 0;
    // unpack intp 
    if (p->ndigits > 9) {
        // bits needed to encode intp[0]
        int32_t nbits = digits_to_bits[p->ndigits - 9];
        // total bits for int parts
        intpbits = nbits + 30;

        p->intp[0] = u64 >> (DEC_DIGITS_SHIFT - nbits);
        p->intp[1] = (u64 >> (DEC_DIGITS_SHIFT - 30 - nbits)) & LOWER30_BITS;
    } else {
        intpbits = digits_to_bits[p->ndigits]; 
        p->intp[0] = 0; 
        p->intp[1] = u64 >> (DEC_DIGITS_SHIFT - intpbits);
    }

    int32_t fracshift = DEC_DIGITS_SHIFT - intpbits;
    int32_t fracdigits = 17 - p->ndigits;
    if (fracdigits > 9) {
        p->frac[0] = (u64 >> fracshift) & LOWER30_BITS;
        fracdigits -= 9;
        fracshift += 30;
    }
    // fracdigits now between 0-9
    uint32_t remaining_bits = fracshift - digits_to_bits[fracdigits];
    assert(remaining_bits == 0 || remaining_bits == 1);

    p->frac[1] = (u64 >> (fracshift + remaining_bits));
    p->frac[1] = mul_pow10(p->frac[1], 9-fracdigits);
}

/* pack p into d */
static inline void pack64(modec_ctxt_t *ctxt, modec64_t *d, const modec64_unpacked_t *p) {
    // first pack int part.   intp[0] (high digits) can have at most 8 digits.
    assert(p->intp[0] <= 99999999);
    assert(p->intp[1] <= 999999999);
    assert(p->ndigits <= 17);

    uint64_t u64 = p->ndigits;
    u64 <<= DEC_DIGITS_SHIFT;

    uint32_t intp_shift = 0;

    // intp
    if (p->ndigits > 9) { 
        uint32_t need_bits = dec_need_bits(p->ndigits - 9);
        uint64_t tmp = p->intp[0];
        tmp <<= (DEC_DIGITS_SHIFT - need_bits); 
        u64 |= tmp;
        tmp = p->intp[1];
        tmp <<= (DEC_DIGITS_SHIFT - need_bits - 30);
        u64 |= tmp;
        intp_shift = DEC_DIGITS_SHIFT - need_bits - 30;
    } else {
        uint32_t need_bits = dec_need_bits(p->ndigits - 9);
        intp_shift = DEC_DIGITS_SHIFT - need_bits;
        uint64_t tmp = p->intp[1];
        tmp <<= intp_shift;
        u64 |= tmp;
    }

    // fracp






}
