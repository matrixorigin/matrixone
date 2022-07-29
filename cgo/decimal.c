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

#include "decDouble.h"
#include "decQuad.h"

#include <inttypes.h>
#include <errno.h>

#define DecDoublePtr(X) ((decDouble*)(X))
#define DecQuadPtr(X) ((decQuad*)(X))

#define DECLARE_DEC_CTXT(x)                       \
    decContext _fn_dc;                            \
    decContextDefault(&_fn_dc, x)          

#define DECLARE_DEC64_CTXT                        \
    decContext _fn_dc;                            \
    decContextDefault(&_fn_dc, DEC_INIT_DECIMAL64)

#define DECLARE_DEC128_CTXT                       \
    decContext _fn_dc;                            \
    decContextDefault(&_fn_dc, DEC_INIT_DECIMAL128)

#define CHECK_RET_STATUS(flag, rc)                \
    if (1) {                                      \
        uint32_t _fn_dc_status = decContextGetStatus(&_fn_dc); \
        if ((_fn_dc_status & flag) != 0) {        \
            return rc;                            \
        }                                         \
    } else (void) 0    


#define DEC_STATUS_OFUF (DEC_Overflow | DEC_Underflow)
#define DEC_STATUS_DIV (DEC_Division_by_zero | DEC_Division_impossible | DEC_Division_undefined)
#define DEC_STATUS_INEXACT (DEC_Inexact | DEC_Clamped | DEC_Rounded)
#define DEC_STATUS_SYNTAX (DEC_Conversion_syntax)
#define DEC_STATUS_ALL (0xFFFFFFFF)

#define CHECK_OFUF CHECK_RET_STATUS(DEC_STATUS_OFUF, RC_OUT_OF_RANGE)
#define CHECK_DIV CHECK_RET_STATUS(DEC_STATUS_DIV, RC_DIVISION_BY_ZERO)
#define CHECK_INEXACT CHECK_RET_STATUS(DEC_STATUS_INEXACT, RC_DATA_TRUNCATED) 
#define CHECK_ALL CHECK_RET_STATUS(DEC_STATUS_ALL, RC_INVALID_ARGUMENT)

/*
 * About decDouble/decQuad cohort.  One decimal number may have serveral different 
 * representations (cohort).   Cohort is useful in computing while maintaining a 
 * meaningful accuracy.   MO does not use cohort, instead, cohort causes trouble when
 * MO hash the value.
 *
 * Calling reduce before returning the number.
 */

int32_t Decimal64_Compare(int32_t *cmp, int64_t *a, int64_t *b)
{
    decDouble r;
    DECLARE_DEC64_CTXT;

    decDoubleCompare(&r, DecDoublePtr(a), DecDoublePtr(b), &_fn_dc);
    if (decDoubleIsPositive(&r)) {
        *cmp = 1;
        return RC_SUCCESS;
    } else if (decDoubleIsZero(&r)) {
        *cmp = 0;
        return RC_SUCCESS;
    } else if (decDoubleIsNegative(&r)) {
        *cmp = -1;
        return RC_SUCCESS;
    }
    return RC_INVALID_ARGUMENT;
}

int32_t Decimal128_Compare(int32_t *cmp, int64_t *a, int64_t *b)
{
    decQuad r;
    DECLARE_DEC128_CTXT;

    decQuadCompare(&r, DecQuadPtr(a), DecQuadPtr(b), &_fn_dc);
    if (decQuadIsPositive(&r)) {
        *cmp = 1;
        return RC_SUCCESS;
    } else if (decQuadIsZero(&r)) {
        *cmp = 0;
        return RC_SUCCESS;
    } else if (decQuadIsNegative(&r)) {
        *cmp = -1;
        return RC_SUCCESS;
    }
    return RC_INVALID_ARGUMENT;
}

int32_t Decimal64_FromInt32(int64_t *d, int32_t v) 
{
    DECLARE_DEC64_CTXT;
    decDouble tmp;
    decDoubleFromInt32(&tmp, v);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_FromInt32(int64_t *d, int32_t v) 
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    decQuadFromInt32(&tmp, v);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_FromUint32(int64_t *d, uint32_t v) 
{
    DECLARE_DEC64_CTXT;
    decDouble tmp;
    decDoubleFromUInt32(&tmp, v);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_FromUint32(int64_t *d, uint32_t v) 
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    decQuadFromUInt32(&tmp, v);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_FromInt64(int64_t *d, int64_t v) 
{
    DECLARE_DEC64_CTXT;
    char s[128];
    decDouble tmp;
    sprintf(s, "%" PRId64 "", v);
    decDoubleFromString(&tmp, s, &_fn_dc);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_FromInt64(int64_t *d, int64_t v) 
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    char s[128];
    sprintf(s, "%" PRId64 "", v);
    decQuadFromString(&tmp, s, &_fn_dc);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_FromUint64(int64_t *d, uint64_t v) 
{
    DECLARE_DEC64_CTXT;
    decDouble tmp;
    char s[128];
    sprintf(s, "%" PRIu64 "", v);
    decDoubleFromString(&tmp, s, &_fn_dc);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_FromUint64(int64_t *d, uint64_t v) 
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    char s[128];
    sprintf(s, "%" PRIu64 "", v);
    decQuadFromString(&tmp, s, &_fn_dc);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_FromFloat64(int64_t *d, double v) 
{
    DECLARE_DEC64_CTXT;
    decDouble tmp;
    char s[128];
    sprintf(s, "%g", v);
    decDoubleFromString(&tmp, s, &_fn_dc);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_FromFloat64(int64_t *d, double v) 
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    char s[128];
    sprintf(s, "%g", v);
    decQuadFromString(&tmp, s, &_fn_dc);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_FromString(int64_t *d, char *s)
{
    DECLARE_DEC64_CTXT;
    decDouble tmp;
    decDoubleFromString(&tmp, s, &_fn_dc);
    decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
    CHECK_INEXACT;
    CHECK_ALL;
    return RC_SUCCESS;
}

int32_t Decimal128_FromString(int64_t *d, char *s)
{
    DECLARE_DEC128_CTXT;
    decQuad tmp;
    decQuadFromString(&tmp, s, &_fn_dc);
    decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
    CHECK_INEXACT;
    CHECK_ALL;
    return RC_SUCCESS;
}

int32_t Decimal64_ToString(char *s, int64_t *d)
{
    DECLARE_DEC64_CTXT;
    decDoubleToString(DecDoublePtr(d), s); 
    return RC_SUCCESS;
}

int32_t Decimal128_ToString(char *s, int64_t *d)
{
    DECLARE_DEC128_CTXT;
    decQuadToString(DecQuadPtr(d), s); 
    return RC_SUCCESS;
}

decDouble* dec64_scale(int32_t s) {
#define NSCALE 16 
    static decDouble *p0;
    static decDouble scale[NSCALE];
    if (p0 == NULL) {
        DECLARE_DEC64_CTXT;
        decDouble ten;
        decDoubleFromInt32(&ten, 10);
        decDoubleFromInt32(&scale[0], 1);
        for (int i = 1; i < NSCALE; i++) {
            decDoubleDivide(&scale[i], &scale[i-1], &ten, &_fn_dc);
        }
        p0 = &scale[0];
    }

    if (s < 0 || s >= NSCALE) { 
        return NULL;
    }
    return &scale[s];
#undef NSCALE
}

decQuad* dec128_scale(int32_t s) {
#define NSCALE 34
    static decQuad *p0;
    static decQuad scale[NSCALE];
    if (p0 == NULL) {
        DECLARE_DEC128_CTXT;
        decQuad ten;
        decQuadFromInt32(&ten, 10);
        decQuadFromInt32(&scale[0], 1);
        for (int i = 1; i < NSCALE; i++) {
            decQuadDivide(&scale[i], &scale[i-1], &ten, &_fn_dc);
        }
        p0 = &scale[0];
    }

    if (s < 0 || s >= NSCALE) { 
        return NULL;
    }
    return &scale[s];
#undef NSCALE
}

int32_t Decimal64_ToStringWithScale(char *s, int64_t *d, int32_t scale)
{
    DECLARE_DEC64_CTXT;
    decDouble *quan = dec64_scale(scale);
    if (quan == NULL) {
        return RC_INVALID_ARGUMENT;
    }
    decDouble tmp;
    decDoubleQuantize(&tmp, DecDoublePtr(d), quan, &_fn_dc);
    decDoubleToString(&tmp, s); 
    return RC_SUCCESS;
}

int32_t Decimal128_ToStringWithScale(char *s, int64_t *d, int32_t scale)
{
    DECLARE_DEC128_CTXT;
    decQuad *quan = dec128_scale(scale);
    if (quan == NULL) {
        return RC_INVALID_ARGUMENT;
    }
    decQuad tmp;
    decQuadQuantize(&tmp, DecQuadPtr(d), quan, &_fn_dc);
    decQuadToString(&tmp, s); 
    return RC_SUCCESS;
}

int32_t Decimal64_FromStringWithScale(int64_t *d, char *s, int32_t scale)
{
    DECLARE_DEC64_CTXT;
    decDouble tmp1;
    decDouble tmp;
    int32_t rc = Decimal64_FromString((int64_t*) &tmp1, s);
    if (rc == 0 || rc == RC_DATA_TRUNCATED) {
        decDouble *quan = dec64_scale(scale);
        if (quan == NULL) {
            return RC_INVALID_ARGUMENT;
        }
        decDoubleQuantize(&tmp, &tmp1, quan, &_fn_dc); 
        decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
        return RC_SUCCESS;
    } else {
        decDoubleReduce(DecDoublePtr(d), &tmp, &_fn_dc);
        return rc;
    }
}

int32_t Decimal128_FromStringWithScale(int64_t *d, char *s, int32_t scale)
{
    DECLARE_DEC128_CTXT;
    decQuad tmp1;
    decQuad tmp;
    int32_t rc = Decimal128_FromString((int64_t*) &tmp1, s);
    if (rc == 0 || rc == RC_DATA_TRUNCATED) {
        decQuad *quan = dec128_scale(scale);
        if (quan == NULL) {
            return RC_INVALID_ARGUMENT;
        }
        decQuadQuantize(&tmp, &tmp1, quan, &_fn_dc); 
        decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
        return RC_SUCCESS;
    } else {
        decQuadReduce(DecQuadPtr(d), &tmp, &_fn_dc);
        return rc;
    }
}

int32_t Decimal64_ToInt64(int64_t *r, int64_t *d) 
{
    DECLARE_DEC64_CTXT;
    char buf[DECDOUBLE_String];
    decDoubleToString(DecDoublePtr(d), buf); 
    char *endp = 0;
    errno = 0;
    *r = strtoll(buf, &endp, 10); 
    if (errno != 0 || endp == buf) {
        return RC_OUT_OF_RANGE;
    }
    return RC_SUCCESS;
}
int32_t Decimal128_ToInt64(int64_t *r, int64_t *d) 
{
    DECLARE_DEC128_CTXT;
    char buf[DECQUAD_String];
    decQuadToString(DecQuadPtr(d), buf); 
    char *endp = 0;
    errno = 0;
    *r = strtoll(buf, &endp, 10);
    if (errno != 0 || endp == buf) {
        return RC_OUT_OF_RANGE;
    }

    return RC_SUCCESS;
}

int32_t Decimal64_ToFloat64(double *f, int64_t *d) 
{
    DECLARE_DEC64_CTXT;
    char buf[DECDOUBLE_String];
    char *endp = 0;
    errno = 0;
    decDoubleToString(DecDoublePtr(d), buf); 
    *f = strtod(buf, &endp);
    if (errno != 0 || endp == buf) {
        return RC_OUT_OF_RANGE;
    }
    return RC_SUCCESS;
}
int32_t Decimal128_ToFloat64(double *f, int64_t *d) 
{
    DECLARE_DEC128_CTXT;
    char buf[DECQUAD_String];
    char *endp = 0;
    errno = 0;
    decQuadToString(DecQuadPtr(d), buf); 
    *f = strtod(buf, NULL); 
    if (errno != 0 || endp == buf) {
        return RC_OUT_OF_RANGE;
    }
    return RC_SUCCESS;
}

int32_t Decimal64_ToDecimal128(int64_t *d128, int64_t *d64)
{
    decQuad tmp;
    DECLARE_DEC128_CTXT;
    decDoubleToWider(DecDoublePtr(d64), &tmp);
    decQuadReduce(DecQuadPtr(d128), &tmp, &_fn_dc);
    return RC_SUCCESS;
}
int32_t Decimal128_ToDecimal64(int64_t *d64, int64_t *d128)
{
    decDouble tmp;
    DECLARE_DEC64_CTXT;
    decDoubleFromWider(&tmp, DecQuadPtr(d128), &_fn_dc);
    decDoubleReduce(DecDoublePtr(d64), &tmp, &_fn_dc);
    return RC_SUCCESS;
}

int32_t Decimal64_Add(int64_t *r, int64_t *a, int64_t *b) 
{
    decDouble tmp;
    DECLARE_DEC64_CTXT;
    decDoubleAdd(&tmp, DecDoublePtr(a), DecDoublePtr(b), &_fn_dc);
    decDoubleReduce(DecDoublePtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal64_AddInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decDouble db;
    Decimal64_FromInt64((int64_t *) &db, b);
    return Decimal64_Add(r, a, (int64_t *) &db);
}

int32_t Decimal64_Sub(int64_t *r, int64_t *a, int64_t *b) 
{
    decDouble tmp;
    DECLARE_DEC64_CTXT;
    decDoubleSubtract(&tmp, DecDoublePtr(a), DecDoublePtr(b), &_fn_dc);
    decDoubleReduce(DecDoublePtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal64_SubInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decDouble db;
    Decimal64_FromInt64((int64_t *) &db, b);
    return Decimal64_Sub(r, a, (int64_t *) &db);
}

int32_t Decimal64_Mul(int64_t *r, int64_t *a, int64_t *b) 
{
    decDouble tmp;
    DECLARE_DEC64_CTXT;
    decDoubleMultiply(&tmp, DecDoublePtr(a), DecDoublePtr(b), &_fn_dc);
    decDoubleReduce(DecDoublePtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal64_MulWiden(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad wa;
    decQuad wb;
    Decimal64_ToDecimal128((int64_t *) &wa, a);
    Decimal64_ToDecimal128((int64_t *) &wb, b);
    return Decimal128_Mul(r, (int64_t *) &wa, (int64_t *) &wb);
}

int32_t Decimal64_MulInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decDouble db;
    Decimal64_FromInt64((int64_t *) &db, b);
    return Decimal64_Mul(r, a, (int64_t *) &db);
}

int32_t Decimal64_Div(int64_t *r, int64_t *a, int64_t *b) 
{
    decDouble tmp;
    DECLARE_DEC64_CTXT;
    decDoubleDivide(&tmp, DecDoublePtr(a), DecDoublePtr(b), &_fn_dc);
    decDoubleReduce(DecDoublePtr(r), &tmp, &_fn_dc);
    CHECK_DIV;
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal64_DivWiden(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad wa;
    decQuad wb;
    Decimal64_ToDecimal128((int64_t *) &wa, a);
    Decimal64_ToDecimal128((int64_t *) &wb, b);
    return Decimal128_Div(r, (int64_t *) &wa, (int64_t *) &wb);
}

int32_t Decimal64_DivInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decDouble db;
    Decimal64_FromInt64((int64_t *) &db, b);
    return Decimal64_Div(r, a, (int64_t *) &db);
}

int32_t Decimal128_Add(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad tmp;
    DECLARE_DEC128_CTXT;
    decQuadAdd(&tmp, DecQuadPtr(a), DecQuadPtr(b), &_fn_dc);
    decQuadReduce(DecQuadPtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal128_AddInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decQuad db;
    Decimal128_FromInt64((int64_t *) &db, b);
    return Decimal128_Add(r, a, (int64_t *) &db);
}

int32_t Decimal128_AddDecimal64(int64_t *r, int64_t *a, int64_t* b) 
{
    decQuad db;
    Decimal64_ToDecimal128((int64_t *) &db, b);
    return Decimal128_Add(r, a, (int64_t *) &db);
}

int32_t Decimal128_Sub(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad tmp;
    DECLARE_DEC128_CTXT;
    decQuadSubtract(&tmp, DecQuadPtr(a), DecQuadPtr(b), &_fn_dc);
    decQuadReduce(DecQuadPtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal128_SubInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decQuad db;
    Decimal128_FromInt64((int64_t *) &db, b);
    return Decimal128_Sub(r, a, (int64_t *) &db);
}

int32_t Decimal128_Mul(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad tmp;
    DECLARE_DEC128_CTXT;
    decQuadMultiply(&tmp, DecQuadPtr(a), DecQuadPtr(b), &_fn_dc);
    decQuadReduce(DecQuadPtr(r), &tmp, &_fn_dc);
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal128_MulInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decQuad db;
    Decimal128_FromInt64((int64_t *) &db, b);
    return Decimal128_Mul(r, a, (int64_t *) &db);
}

int32_t Decimal128_Div(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad tmp;
    DECLARE_DEC128_CTXT;
    decQuadDivide(&tmp, DecQuadPtr(a), DecQuadPtr(b), &_fn_dc);
    decQuadReduce(DecQuadPtr(r), &tmp, &_fn_dc);
    CHECK_DIV;
    CHECK_OFUF;
    return RC_SUCCESS;
}

int32_t Decimal128_DivInt64(int64_t *r, int64_t *a, int64_t b) 
{
    decQuad db;
    Decimal128_FromInt64((int64_t *) &db, b);
    return Decimal128_Div(r, a, (int64_t *) &db);
}

static inline int32_t Decimal64_AddNoCheck(int64_t *r, int64_t *a, int64_t *b) 
{
    decDouble tmp;
    decDoubleAdd(&tmp, DecDoublePtr(a), DecDoublePtr(b), NULL);
    decDoubleReduce(DecDoublePtr(r), &tmp, NULL);
    return RC_SUCCESS;
}

static inline int32_t Decimal128_AddNoCheck(int64_t *r, int64_t *a, int64_t *b) 
{
    decQuad tmp;
    decQuadAdd(&tmp, DecQuadPtr(a), DecQuadPtr(b), NULL);
    decQuadReduce(DecQuadPtr(r), &tmp, NULL);
    return RC_SUCCESS;
}

#define DEF_DECIMAL_ARITH(NBITS, OP)                                  \
int32_t Decimal ## NBITS ## _VecAdd(int64_t *r, int64_t *a, int64_t *b, uint64_t n, uint64_t *nulls, int32_t flag) { \
    if ((flag & 1) != 0) {                                            \
        if (nulls != NULL) {                                          \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                if (!bitmap_test(nulls, i)) {                         \
                    int ret = Decimal ## NBITS ## _ ## OP (r+ii, a, b+ii); \
                    if (ret != RC_SUCCESS) {                          \
                        return ret;                                   \
                    }                                                 \
                }                                                     \
            }                                                         \
        } else {                                                      \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                int ret = Decimal ## NBITS ## _ ## OP (r+ii, a, b+ii);\
                if (ret != RC_SUCCESS) {                              \
                    return ret;                                       \
                }                                                     \
            }                                                         \
        }                                                             \
        return RC_SUCCESS;                                            \
    } else if ((flag & 2) != 0) {                                     \
        if (nulls != NULL) {                                          \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                if (!bitmap_test(nulls, i)) {                         \
                    int ret = Decimal ## NBITS ## _ ## OP (r+ii, a+ii, b); \
                    if (ret != RC_SUCCESS) {                          \
                        return ret;                                   \
                    }                                                 \
                }                                                     \
            }                                                         \
        } else {                                                      \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                int ret = Decimal ## NBITS ## _ ## OP (r+ii, a+ii, b);\
                if (ret != RC_SUCCESS) {                              \
                    return ret;                                       \
                }                                                     \
            }                                                         \
        }                                                             \
        return RC_SUCCESS;                                            \
    } else {                                                          \
        if (nulls != NULL) {                                          \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                if (!bitmap_test(nulls, i)) {                         \
                    int ret = Decimal ## NBITS ## _ ## OP (r+ii, a+ii, b+ii); \
                    if (ret != RC_SUCCESS) {                          \
                        return ret;                                   \
                    }                                                 \
                }                                                     \
            }                                                         \
        } else {                                                      \
            for (uint64_t i = 0; i < n; i++) {                        \
                uint64_t ii = i << (NBITS / 64 - 1);                  \
                int ret = Decimal ## NBITS ## _ ## OP (r+ii, a+ii, b+ii); \
                if (ret != RC_SUCCESS) {                              \
                    return ret;                                       \
                }                                                     \
            }                                                         \
        }                                                             \
        return RC_SUCCESS;                                            \
    }                                                                 \
}


DEF_DECIMAL_ARITH(64, AddNoCheck)

DEF_DECIMAL_ARITH(128, AddNoCheck)
