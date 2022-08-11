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

/*
 * Signed int sub with overflow check.
 */
#define SUB_SIGNED_OVFLAG(TGT, A, B)                 \
    TGT = (A) - (B);                                 \
    opflag |= ((A) ^ (B)) & ((TGT) ^ (A))

#define SUB_SIGNED_OVFLAG_CHECK                      \
    if (opflag < 0)    {                             \
        return RC_OUT_OF_RANGE;                      \
    }else return RC_SUCCESS


/*
 * Unsigned int sub with overflow check
 *
 * If A is less than B,, we know it wrapped around.
 */
#define SUB_UNSIGNED_OVFLAG(TGT, A, B)              \
    TGT = (A) - (B);                                \
    if ((A) < (B)) {                                \
        opflag = 1;                                 \
    } else (void) 0

#define SUB_UNSIGNED_OVFLAG_CHECK                   \
    if (opflag != 0) {                              \
        return RC_OUT_OF_RANGE;                     \
    } else return RC_SUCCESS


/*
 * Float/Double overflow check.
 *
 * At this moment we don't do anything.
 */
#define SUB_FLOAT_OVFLAG(TGT, A, B)                 \
    TGT = (A) - (B)

#define SUB_FLOAT_OVFLAG_CHECK                      \
    (void) opflag;                                  \
    return RC_SUCCESS

/*
 * Signed int mul with overflow check.
 */
#define MUL_SIGNED_OVFLAG(TGT, A, B, MAXVAL, MINVAL, ZT ,UPTYPE)               \
    temp = (UPTYPE)(A) * (UPTYPE)(B);                                          \
    TGT = (ZT)temp;                                                            \
    opflag = ((A ^ B) > 0 && temp > MAXVAL) || ((A ^ B) < 0 && temp < MINVAL)

#define MUL_SIGNED_OVFLAG_CHECK                                                \
    if (opflag != 0)    {                                                      \
        return RC_OUT_OF_RANGE;                                                \
    }else return RC_SUCCESS


/*
 * Unsigned int mul with overflow check
 */
#define MUL_UNSIGNED_OVFLAG(TGT, A, B, MAXVAL, MINVAL, ZT, UPTYPE)             \
    temp = (UPTYPE)(A) * (UPTYPE)(B);                                          \
    TGT = (ZT)temp;                                                            \
    opflag = (temp > MAXVAL)

#define MUL_UNSIGNED_OVFLAG_CHECK                                              \
    if (opflag != 0) {                                                         \
        return RC_OUT_OF_RANGE;                                                \
    } else return RC_SUCCESS


/*
 * Float/Double mul overflow check.
 *
 * At this moment we don't do anything.
 */
#define MUL_FLOAT_OVFLAG(TGT, A, B)                 \
    TGT = (A) * (B)

#define MUL_FLOAT_OVFLAG_CHECK                      \
    (void) opflag;                                  \
    return RC_SUCCESS


/*
 * Float/Double div overflow check.
 *
 * At this moment we don't do anything.
 */
#define DIV_FLOAT_OVFLAG(TGT, A, B)                 \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = (A) / (B)

#define DIV_FLOAT_OVFLAG_CHECK                      \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS

/*
 * Signed int mod with overflow check.
 */
#define MOD_SIGNED_OVFLAG(TGT, A, B)                \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = (A) % (B)


#define MOD_SIGNED_OVFLAG_CHECK                     \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS


/*
 * Unsigned int mod with overflow check
 */
#define MOD_UNSIGNED_OVFLAG(TGT, A, B)              \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = (A) % (B)

#define MOD_UNSIGNED_OVFLAG_CHECK                   \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS


/*
 * Float mod overflow check.
 */
#define MOD_FLOAT_OVFLAG(TGT, A, B)                 \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = fmodf((A), (B))

#define MOD_FLOAT_OVFLAG_CHECK                      \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS


/*
 * Double mod overflow check.
 */
#define MOD_DOUBLE_OVFLAG(TGT, A, B)                \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = fmod((A), (B))

#define MOD_DOUBLE_OVFLAG_CHECK                     \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS

// MO_ARITH_T: Handle general arithmetic operations
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


// MO_MUL_T: Handle signed and unsigned integer multiplication
#define MO_MUL_T(OP, ZT, MAXVAL, MINVAL, UPTYPE)                               \
    ZT *rt = (ZT *) r;                                                         \
    ZT *at = (ZT *) a;                                                         \
    ZT *bt = (ZT *) b;                                                         \
    UPTYPE temp = 0;                                                           \
    ZT opflag = 0;                                                             \
    if ((flag & LEFT_IS_SCALAR) != 0) {                                        \
        if (nulls != NULL) {                                                   \
            for (uint64_t i = 0; i < n; i++) {                                 \
                if (!bitmap_test(nulls, i)) {                                  \
                    OP(rt[i], at[0], bt[i], MAXVAL, MINVAL, ZT, UPTYPE);       \
                }                                                              \
            }                                                                  \
        } else {                                                               \
            for (uint64_t i = 0; i < n; i++) {                                 \
                OP(rt[i], at[0], bt[i], MAXVAL, MINVAL, ZT, UPTYPE);           \
            }                                                                  \
        }                                                                      \
    } else if ((flag & RIGHT_IS_SCALAR) != 0) {                                \
        if (nulls != NULL) {                                                   \
            for (uint64_t i = 0; i < n; i++) {                                 \
                if (!bitmap_test(nulls, i)) {                                  \
                    OP(rt[i], at[i], bt[0], MAXVAL, MINVAL, ZT, UPTYPE);       \
                }                                                              \
            }                                                                  \
        } else {                                                               \
            for (uint64_t i = 0; i < n; i++) {                                 \
                OP(rt[i], at[i], bt[0], MAXVAL, MINVAL, ZT, UPTYPE);           \
            }                                                                  \
        }                                                                      \
    } else {                                                                   \
        if (nulls != NULL) {                                                   \
            for (uint64_t i = 0; i < n; i++) {                                 \
                if (!bitmap_test(nulls, i)) {                                  \
                    OP(rt[i], at[i], bt[i], MAXVAL, MINVAL, ZT, UPTYPE);       \
                }                                                              \
            }                                                                  \
        } else {                                                               \
            for (uint64_t i = 0; i < n; i++) {                                 \
                OP(rt[i], at[i], bt[i], MAXVAL, MINVAL, ZT, UPTYPE);           \
            }                                                                  \
        }                                                                      \
    }                                                                          \
    OP ## _CHECK



// Addition operation
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

// Subtraction operation
int32_t SignedInt_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(SUB_SIGNED_OVFLAG, int8_t);
    } else if (szof == 2) {
        MO_ARITH_T(SUB_SIGNED_OVFLAG, int16_t);
    } else if (szof == 4) {
        MO_ARITH_T(SUB_SIGNED_OVFLAG, int32_t);
    } else if (szof == 8) {
        MO_ARITH_T(SUB_SIGNED_OVFLAG, int64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t UnsignedInt_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(SUB_UNSIGNED_OVFLAG, uint8_t);
    } else if (szof == 2) {
        MO_ARITH_T(SUB_UNSIGNED_OVFLAG, uint16_t);
    } else if (szof == 4) {
        MO_ARITH_T(SUB_UNSIGNED_OVFLAG, uint32_t);
    } else if (szof == 8) {
        MO_ARITH_T(SUB_UNSIGNED_OVFLAG, uint64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t Float_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 4) {
        MO_ARITH_T(SUB_FLOAT_OVFLAG, float);
    } else if (szof == 8) {
        MO_ARITH_T(SUB_FLOAT_OVFLAG, double);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

// Multiplication operation
int32_t SignedInt_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_MUL_T(MUL_SIGNED_OVFLAG, int8_t, INT8_MAX, INT8_MIN, int16_t);
    } else if (szof == 2) {
        MO_MUL_T(MUL_SIGNED_OVFLAG, int16_t, INT16_MAX, INT16_MIN, int16_t);
    } else if (szof == 4) {
        MO_MUL_T(MUL_SIGNED_OVFLAG, int32_t, INT32_MAX, INT32_MIN, int64_t);
    } else if (szof == 8) {
        MO_MUL_T(MUL_SIGNED_OVFLAG, int64_t, INT64_MAX, INT64_MIN, __int128);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


int32_t UnsignedInt_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_MUL_T(MUL_UNSIGNED_OVFLAG, uint8_t, UINT8_MAX, 0, uint16_t);
    } else if (szof == 2) {
        MO_MUL_T(MUL_UNSIGNED_OVFLAG, uint16_t, UINT16_MAX, 0, uint32_t);
    } else if (szof == 4) {
        MO_MUL_T(MUL_UNSIGNED_OVFLAG, uint32_t, UINT32_MAX, 0, uint64_t);
    } else if (szof == 8) {
        MO_MUL_T(MUL_UNSIGNED_OVFLAG, uint64_t, UINT64_MAX, 0, __int128);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


int32_t Float_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 4) {
        MO_ARITH_T(MUL_FLOAT_OVFLAG, float);
    } else if (szof == 8) {
        MO_ARITH_T(MUL_FLOAT_OVFLAG, double);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

// Division operation
int32_t Float_VecDiv(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof) {
    if (szof == 4) {
        MO_ARITH_T(DIV_FLOAT_OVFLAG, float);
    } else if (szof == 8) {
        MO_ARITH_T(DIV_FLOAT_OVFLAG, double);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

// Mod operation
int32_t SignedInt_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(MOD_SIGNED_OVFLAG, int8_t);
    } else if (szof == 2) {
        MO_ARITH_T(MOD_SIGNED_OVFLAG, int16_t);
    } else if (szof == 4) {
        MO_ARITH_T(MOD_SIGNED_OVFLAG, int32_t);
    } else if (szof == 8) {
        MO_ARITH_T(MOD_SIGNED_OVFLAG, int64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t UnsignedInt_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 1) {
        MO_ARITH_T(MOD_UNSIGNED_OVFLAG, uint8_t);
    } else if (szof == 2) {
        MO_ARITH_T(MOD_UNSIGNED_OVFLAG, uint16_t);
    } else if (szof == 4) {
        MO_ARITH_T(MOD_UNSIGNED_OVFLAG, uint32_t);
    } else if (szof == 8) {
        MO_ARITH_T(MOD_UNSIGNED_OVFLAG, uint64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t Float_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 4) {
        MO_ARITH_T(MOD_FLOAT_OVFLAG, float);
    } else if (szof == 8) {
        MO_ARITH_T(MOD_DOUBLE_OVFLAG, double);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


/*
 * Float/Double integer div overflow check.
 *
 * At this moment we don't do anything.
 */
#define INTDIV_FLOAT_OVFLAG(TGT, A, B)              \
    if ((B) == 0) {                                 \
        opflag = 1;                                 \
    } else TGT = (int64_t)((A) / (B))

#define INTDIV_FLOAT_OVFLAG_CHECK                   \
    if (opflag == 1) {                              \
        return RC_DIVISION_BY_ZERO;                 \
    } else return RC_SUCCESS

// MO_INT_DIV : Handle floating-point integer division
#define MO_INT_DIV(OP, ZT, RT)                                \
    RT *rt = (RT *) r;                                        \
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


int32_t Float_VecIntegerDiv(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof)
{
    if (szof == 4) {
        MO_INT_DIV(INTDIV_FLOAT_OVFLAG, float, int64_t);
    } else if (szof == 8) {
        MO_INT_DIV(INTDIV_FLOAT_OVFLAG, double, int64_t);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}