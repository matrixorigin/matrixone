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


#define	Type_BOOL     10
#define	Type_INT8     20
#define	Type_INT16    21
#define	Type_INT32    22
#define	Type_INT64    23
#define	Type_INT128   24
#define	Type_UINT8    25
#define	Type_UINT16   26
#define	Type_UINT32   27
#define	Type_UINT64   28
#define	Type_UINT128  29
#define	Type_FLOAT32  30
#define	Type_FLOAT64  31

// Time
#define Type_DATE       50
#define Type_TIME       51
#define Type_DATETIME   52
#define Type_TIMESTAMP  53



/*
 * Equal operator (=)
 */
#define COMPARE_EQ(TGT, A, B)                                 \
    TGT = ((A) == (B))


/*
 * Not Equal operator (<>)
 */
#define COMPARE_NE(TGT, A, B)                                 \
    TGT = ((A) != (B))


/*
 * great than operator (>)
 */
#define COMPARE_GT(TGT, A, B)                                 \
    TGT = ((A) > (B))


/*
 * great equal operator (>=)
 */
#define COMPARE_GE(TGT, A, B)                                 \
    TGT = ((A) >= (B))


/*
 * less than operator (<)
 */
#define COMPARE_LT(TGT, A, B)                                 \
    TGT = ((A) < (B))


/*
 * less equal operator (<=)
 */
#define COMPARE_LE(TGT, A, B)                                 \
    TGT = ((A) <= (B))


/*
 * bool compare operator
 */
#define COMPARE_BOOL_EQ(TGT, A, B)                                     \
    TGT = (((A) && (B)) || (!(A) && !(B)))

#define COMPARE_BOOL_NE(TGT, A, B)                                     \
    TGT = ((!(A) && (B)) || ((A) && !(B)))

#define COMPARE_BOOL_LE(TGT, A, B)                                     \
    TGT = (!(A) || (B))

#define COMPARE_BOOL_LT(TGT, A, B)                                     \
    TGT = (!(A) && (B))

#define COMPARE_BOOL_GE(TGT, A, B)                                     \
    TGT = ((A) || !(B))

#define COMPARE_BOOL_GT(TGT, A, B)                                     \
    TGT = ((A) && !(B))





#define MO_COMPARE_T(OP, ZT)                                  \
    bool *rt = (bool *) r;                                    \
    ZT *at = (ZT *) a;                                        \
    ZT *bt = (ZT *) b;                                        \
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
    }



int32_t Numeric_VecEq(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_EQ, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_EQ, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_EQ, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_EQ, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_EQ, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_EQ, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_EQ, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_EQ, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_EQ, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_EQ, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_EQ, int32_t);
    } else if (type == Type_TIME) {
        MO_COMPARE_T(COMPARE_EQ, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_EQ, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_EQ, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_EQ, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


int32_t Numeric_VecNe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_NE, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_NE, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_NE, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_NE, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_NE, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_NE, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_NE, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_NE, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_NE, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_NE, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_NE, int32_t);
    } else if (type == Type_TIME){
        MO_COMPARE_T(COMPARE_NE, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_NE, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_NE, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_NE, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


int32_t Numeric_VecGt(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_GT, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_GT, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_GT, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_GT, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_GT, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_GT, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_GT, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_GT, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_GT, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_GT, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_GT, int32_t);
    } else if (type == Type_TIME) {
        MO_COMPARE_T(COMPARE_GT, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_GT, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_GT, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_GT, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}

int32_t Numeric_VecGe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_GE, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_GE, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_GE, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_GE, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_GE, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_GE, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_GE, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_GE, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_GE, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_GE, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_GE, int32_t);
    } else if (type == Type_TIME) {
        MO_COMPARE_T(COMPARE_GE, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_GE, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_GE, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_GE, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }

    return RC_SUCCESS;
}


int32_t Numeric_VecLt(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_LT, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_LT, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_LT, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_LT, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_LT, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_LT, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_LT, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_LT, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_LT, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_LT, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_LT, int32_t);
    } else if (type == Type_TIME) {
        MO_COMPARE_T(COMPARE_LT, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_LT, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_LT, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_LT, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}


int32_t Numeric_VecLe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type)
{
    if (type == Type_INT8) {
        MO_COMPARE_T(COMPARE_LE, int8_t);
    } else if (type == Type_INT16) {
        MO_COMPARE_T(COMPARE_LE, int16_t);
    } else if (type == Type_INT32) {
        MO_COMPARE_T(COMPARE_LE, int32_t);
    } else if (type == Type_INT64) {
        MO_COMPARE_T(COMPARE_LE, int64_t);
    } else if (type == Type_UINT8) {
        MO_COMPARE_T(COMPARE_LE, uint8_t);
    } else if (type == Type_UINT16) {
        MO_COMPARE_T(COMPARE_LE, uint16_t);
    } else if (type == Type_UINT32) {
        MO_COMPARE_T(COMPARE_LE, uint32_t);
    } else if (type == Type_UINT64) {
        MO_COMPARE_T(COMPARE_LE, uint64_t);
    } else if (type == Type_FLOAT32) {
        MO_COMPARE_T(COMPARE_LE, float);
    } else if (type == Type_FLOAT64) {
        MO_COMPARE_T(COMPARE_LE, double);
    } else if (type == Type_DATE) {
        MO_COMPARE_T(COMPARE_LE, int32_t);
    } else if (type == Type_TIME) {
        MO_COMPARE_T(COMPARE_LE, int64_t);
    } else if (type == Type_DATETIME) {
        MO_COMPARE_T(COMPARE_LE, int64_t);
    } else if (type == Type_TIMESTAMP) {
        MO_COMPARE_T(COMPARE_LE, int64_t);
    } else if (type == Type_BOOL) {
        MO_COMPARE_T(COMPARE_BOOL_LE, bool);
    } else {
        return RC_INVALID_ARGUMENT;
    }
    return RC_SUCCESS;
}