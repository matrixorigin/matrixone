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

#ifndef _MODECIMAL_H_
#define _MODECIMAL_H_

#include "mo_impl.h"

/*
 * Mo's own impl of decimal64/128.  Only supports +,-,*,/,% and round.
 * dec64 has precision of 17.
 * dec128 has precision of 36.
 *
 * Binary format.
 *      bit 1:     sign bit, 0 for neg, 1 for zero and pos.
 *      bit 5/6:   number of integeral dec digits. 
 *      intparts:  bits to store integral part, from above.
 *                 intpart will be decimal digits, base 1000000000.
 *                 The left most intpart could be short.
 *      fracparts: bits to store fractional part, after intparts.
 *
 * If the number is negative (bit 1), the rest of the bits are
 * reversed.
 *
 * Example:   123456789012345.6789 in decimal128 format.
 *      1 bit    :      1       -- pos
 *      6 bits   :      15      -- int part has 15 decimal digits
 *      20 bits  :     123456   -- need 20 bits to store 6 digits
 *      30 bits  :  789012345   -- need 30 bits to store 9 digits
 *      30 bits  :  678900000   -- 30 bits to store 9 digits.  
 *                              -- Even though 14 bits are enough.
 *                              -- we fill to 30 bits (base 1000000000)
 *      rest     :      0       -- zero fill.
 *
 * Another example:  -123456789012345.678901234567890123456
 *      1 bit    :      0       -- neg
 *      6 bits   :      15      -- int part has 15 decimal digits
 *      20 bits  :     123456   -- need 20 bits to store 6 digits
 *      30 bits  :  789012345   -- need 30 bits to store 9 digits
 *      30 bits  :  678901234   -- 30 bits to store 9 digits.  
 *      30 bits  :  567890123   -- 30 bits
 *      10 bits  :  456         -- 10 bits to store 3 digits
 *      1 (rest) :      0       -- unused. 
 *
 * The format is designed with the following in mind,
 *   0. decimal to binary is injection (no cohort).  It is not
 *      a bijection.
 *   1. binary format is precision/scale independt.
 *   2. you can compare two decimal with memcmp.
 *
 * Bits manipulation is still much easier in C.   However, there
 * is no reason this cannot be in go.
 */

typedef struct modec_ctxt_t { 
    int32_t ec;         /* error code */
} modec_ctxt_t;

/* Decimal execution context info */
#define DECL_INIT_CTXT          \
    modec_ctxt_t decCtxt;       \
    decCtxt.ec = 0;             \
    (void)0

#define DEC64_NDIGITS 17
#define DEC128_NDIGITS 36

typedef uint64_t modec64_t;
typedef struct modec128_t {
    uint64_t u64[2];
} modec128_t; 

/* 
 * string read/write buf.  
 *
 * we only support optional +/- sign, digits, . decimal point, then more digits, zero terminator
 * 20 is enough to hold dec64.
 * 40 is enough to hold dec128.
 */
#define MODEC64_STRBUF_LEN 20
#define MODEC128_STRBUF_LEN 40

typedef struct modec64_upacked_t {
    int8_t sign;        // sign, +/- 1
    uint8_t ndigits;     // number of decimal digits, 0-17
    uint32_t intp[2];   // after unpack, each base 1B digits can be
                        // stored in 30 bits, we use 32.
    uint32_t frac[2];   // base 1b of frac part.
                        // note that if there are digits in frac[1] 
                        // it is zero filled.   In the another example 
                        // above, the last digits are 456, will extend
                        // to 456000000
} modec64_unpacked_t;

typedef struct modec128_upacked_t {
    int8_t sign;        // sign, +/- 1
    uint8_t ndigits;     // number of decimal digits 0-36
    uint32_t intp[4];   // after unpack, each base 1B digits can be
                        // stored in 30 bits, we use 32.
    uint32_t frac[4];   // base 1b of frac part.
                        // note that if there are digits in frac[1] 
                        // it is zero filled.   In the another example 
                        // above, the last digits are 456, will extend
                        // to 456000000
} modec128_unpacked_t;

#endif /* _MODECIMAL_H_ */
