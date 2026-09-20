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

#include <math.h>

#include "xcall.h"
#include "bitmap.h"

static double xcall_scale_result(double scale, double value, bool square) {
    int exponent;
    double mantissa = frexp(scale, &exponent);
    if (square) {
        return ldexp(value * mantissa * mantissa, 2 * exponent);
    }
    return ldexp(value * mantissa, exponent);
}

static double xcall_l2_result_f32(float *left, float *right, int dim, bool square) {
    double scale = 0;
    for (int j = 0; j < dim; j++) {
        double diff = (double) left[j] - (double) right[j];
        double abs_diff = fabs(diff);
        if (isinf(abs_diff)) {
            return __builtin_huge_val();
        }
        if (abs_diff > scale) {
            scale = abs_diff;
        }
    }
    if (scale == 0) {
        return 0;
    }

    double sum = 0;
    for (int j = 0; j < dim; j++) {
        double scaled = ((double) left[j] - (double) right[j]) / scale;
        sum += scaled * scaled;
    }
    return xcall_scale_result(scale, square ? sum : sqrt(sum), square);
}

static double xcall_l2_result_f64(double *left, double *right, int dim, bool square) {
    double scale = 0;
    for (int j = 0; j < dim; j++) {
        double diff = left[j] - right[j];
        double abs_diff = fabs(diff);
        if (isinf(abs_diff)) {
            return __builtin_huge_val();
        }
        if (abs_diff > scale) {
            scale = abs_diff;
        }
    }
    if (scale == 0) {
        return 0;
    }

    double sum = 0;
    for (int j = 0; j < dim; j++) {
        double scaled = (left[j] - right[j]) / scale;
        sum += scaled * scaled;
    }
    return xcall_scale_result(scale, square ? sum : sqrt(sum), square);
}

int32_t xcall_l2distance_f32(int64_t rtid, uint8_t *errBuf, uint64_t *args, uint64_t len, bool sq) {
    /* 
     * Must be 3 args, ret, arg1, arg2.  
     * Len must be correct 
     */
    xcall_args_t *pargs = (xcall_args_t *) args;
    /* l2 distance f32 returns float64 as well */
    double *pres = (double *) pargs[0].pdata;
    bool c1const;
    ptrlen_t c1 = {0};
    bool c2const;
    ptrlen_t c2 = {0};
    varlena_t *p1 = (varlena_t *) pargs[1].pdata;
    varlena_t *p2 = (varlena_t *) pargs[2].pdata;

    c1const = pargs[1].dataSz == VARLENA_SZ;
    c2const = pargs[2].dataSz == VARLENA_SZ;
    varlena_get_ptrlen(p1, pargs[1].parea, &c1);
    varlena_get_ptrlen(p2, pargs[2].parea, &c2);

#ifdef MO_CL_CUDA
    /* if vector is too small, does not worth it */
    if (rtid == RUNTIME_CUDA 
            && (!c1const || !c2const)
            && pargs[0].pnulls == NULL 
            && len >= CUDA_THREADS_PER_BLOCK
            && c1.len > 128) {
        return cuda_l2distance_f32(errBuf, pres, (int)(len), c1.len, sq,
                p1, pargs[1].parea, c1const,
                p2, pargs[2].parea, c2const);
    }
#endif
    int dim = c1.len / sizeof(float);
    for (uint64_t i = 0; i < len; i++) {
        if (!bitmap_test(pargs[0].pnulls, i)) {
            if (!c1const) {
                varlena_get_ptrlen(p1+i, pargs[1].parea, &c1);
            }
            if (!c2const) {
                varlena_get_ptrlen(p2+i, pargs[2].parea, &c2);
            }
            float *c1val = (float *) c1.ptr;
            float *c2val = (float *) c2.ptr;
            pres[i] = xcall_l2_result_f32(c1val, c2val, dim, sq);
        }
    }
    return 0;
}

/* well well well, does it worth to make f32/f64 a macro?  probably not */
int32_t xcall_l2distance_f64(int64_t rtid, uint8_t *errBuf, uint64_t *args, uint64_t len, bool sq) {
    /* 
     * Must be 3 args, ret, arg1, arg2.  
     * Len must be correct 
     */
    xcall_args_t *pargs = (xcall_args_t *) args;
    double *pres = (double *) pargs[0].pdata;
    bool c1const;
    ptrlen_t c1 = {0};
    bool c2const;
    ptrlen_t c2 = {0};
    varlena_t *p1 = (varlena_t *) pargs[1].pdata;
    varlena_t *p2 = (varlena_t *) pargs[2].pdata;

    c1const = pargs[1].dataSz == VARLENA_SZ;
    c2const = pargs[2].dataSz == VARLENA_SZ;
    varlena_get_ptrlen(p1, pargs[1].parea, &c1);
    varlena_get_ptrlen(p2, pargs[2].parea, &c2);

#ifdef MO_CL_CUDA
    /* if vector is too small, does not worth it */
    if (rtid == RUNTIME_CUDA 
            && (!c1const || !c2const)
            && pargs[0].pnulls == NULL 
            && len >= CUDA_THREADS_PER_BLOCK
            && c1.len > 128) {
        return cuda_l2distance_f64(errBuf, pres, (int)(len), c1.len, sq,
                p1, pargs[1].parea, c1const,
                p2, pargs[2].parea, c2const);
    }
#endif

    int dim = c1.len / sizeof(double);
    for (uint64_t i = 0; i < len; i++) {
        if (!bitmap_test(pargs[0].pnulls, i)) {
            if (!c1const) {
                varlena_get_ptrlen(p1+i, pargs[1].parea, &c1);
            }
            if (!c2const) {
                varlena_get_ptrlen(p2+i, pargs[2].parea, &c2);
            }
            double *c1val = (double *) c1.ptr;
            double *c2val = (double *) c2.ptr;
            pres[i] = xcall_l2_result_f64(c1val, c2val, dim, sq);
        }
    }
    return 0;
}
