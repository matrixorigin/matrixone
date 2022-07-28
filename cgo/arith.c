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

/* 
 * Signed int add with overflow check.
 *
 * The test checks if rt[i] and tmpx have different sign, and rt[i]
 * and tmpy have different sign, therefore, tmpx and tmpy have same 
 * sign but the result has different sign, therefore, it is an overflow.
 *
 * DO NOT use __builtin_add_overflow, which gcc cannot vectorize with SIMD
 * and is actually much much slower.
 */
#define ADD_SIGNED_OVFLAG(TGT, A, B)                 \
    TGT = (A) + (B);                                 \
    opflag |= ((TGT) ^ (A)) & ((TGT) ^ (B))

#define ADD_SIGNED_OVFLAG_CHECK                      \
    if (opflag < 0)    {                             \
        return RC_OUT_OF_RANGE;                      \
    } else return RC_SUCCESS


/*
 * Unsigned int add with overflow check
 *
 * If result is less, we know it wrapped around.
 */
#define ADD_UNSIGNED_OVFLAG(TGT, A, B)              \
    TGT = (A) + (B);                                \
    if ((TGT) < (A)) {                              \
        opflag = 1;                                 \
    } else (void) 0

#define ADD_UNSIGNED_OVFLAG_CHECK                   \
    if (opflag != 0) {                              \
        return RC_OUT_OF_RANGE;                     \
    } else return RC_SUCCESS

/*
 * Float/Double overflow check.
 *
 * At this moment we don't do anything.
 */
#define ADD_FLOAT_OVFLAG(TGT, A, B)                 \
    TGT = (A) + (B)    

#define ADD_FLOAT_OVFLAG_CHECK                      \
    (void) opflag;                                  \
    return RC_SUCCESS


const int32_t LEFT_IS_SCALAR = 1;
const int32_t RIGHT_IS_SCALAR = 2;

#define MO_ARITH_T(OP, ZT)                                    \
    ZT *rt = (ZT *) r;                                        \
    ZT *at = (ZT *) a;                                        \
    ZT *bt = (ZT *) b;                                        \
    ZT opflag = 0;                                            \
    if ((flag & LEFT_IS_SCALAR) != 0) {                       \
        if (nulls != NULL) {                                  \
            for (uint64_t i = 0; i < n; i++) {                \
                if (!bitmap_test(nulls, i)) {                 \
                    OP(rt[i], at[0], bt[i]);                  \
                }                                             \
            }                                                 \
        } else {                                              \
            for (uint64_t i = 0; i < n; i++) {                \
                OP(rt[i], at[0], bt[i]);                      \
            }                                                 \
        }                                                     \
    } else if ((flag & RIGHT_IS_SCALAR) != 0) {               \
        if (nulls != NULL) {                                  \
            for (uint64_t i = 0; i < n; i++) {                \
                if (!bitmap_test(nulls, i)) {                 \
                    OP(rt[i], at[i], bt[0]);                  \
                }                                             \
            }                                                 \
        } else {                                              \
            for (uint64_t i = 0; i < n; i++) {                \
                OP(rt[i], at[i], bt[0]);                      \
            }                                                 \
        }                                                     \
    } else {                                                  \
        if (nulls != NULL) {                                  \
            for (uint64_t i = 0; i < n; i++) {                \
                if (!bitmap_test(nulls, i)) {                 \
                    OP(rt[i], at[i], bt[i]);                  \
                }                                             \
            }                                                 \
        } else {                                              \
            for (uint64_t i = 0; i < n; i++) {                \
                OP(rt[i], at[i], bt[i]);                      \
            }                                                 \
        }                                                     \
    }                                                         \
    OP ## _CHECK 

int32_t SignedInt_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(ADD_SIGNED_OVFLAG, int8_t);
    } else if (szof == 2) {
        MO_ARITH_T(ADD_SIGNED_OVFLAG, int16_t);
    } else if (szof == 4) {
        MO_ARITH_T(ADD_SIGNED_OVFLAG, int32_t);
    } else if (szof == 8) {
        MO_ARITH_T(ADD_SIGNED_OVFLAG, int64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t UnsignedInt_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(ADD_UNSIGNED_OVFLAG, uint8_t);
    } else if (szof == 2) {
        MO_ARITH_T(ADD_UNSIGNED_OVFLAG, uint16_t);
    } else if (szof == 4) {
        MO_ARITH_T(ADD_UNSIGNED_OVFLAG, uint32_t);
    } else if (szof == 8) {
        MO_ARITH_T(ADD_UNSIGNED_OVFLAG, uint64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t Float_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 4) {
        MO_ARITH_T(ADD_FLOAT_OVFLAG, float);
    } else if (szof == 8) {
        MO_ARITH_T(ADD_FLOAT_OVFLAG, double);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}




