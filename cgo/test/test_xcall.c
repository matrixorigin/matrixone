// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <math.h>
#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../xcall.h"

#define CHECK(cond, message) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "xcall check failed: %s\n", message); \
            return 1; \
        } \
    } while (0)

static void set_inline_vector(varlena_t *value, const void *data, int bytes) {
    memset(value, 0, sizeof(*value));
    value->bs[0] = (uint8_t)bytes;
    memcpy(value->bs + 1, data, (size_t)bytes);
}

static double run_f32(const float *left, const float *right, int dim, bool sq) {
    varlena_t leftValue, rightValue;
    double result = 0;
    xcall_args_t args[3] = {0};
    set_inline_vector(&leftValue, left, dim * (int)sizeof(float));
    set_inline_vector(&rightValue, right, dim * (int)sizeof(float));
    args[0].pdata = (uint8_t *)&result;
    args[0].dataSz = sizeof(result);
    args[1].pdata = (uint8_t *)&leftValue;
    args[1].dataSz = VARLENA_SZ;
    args[2].pdata = (uint8_t *)&rightValue;
    args[2].dataSz = VARLENA_SZ;
    CHECK(xcall_l2distance_f32(RUNTIME_C, NULL, (uint64_t *)args, 1, sq) == 0,
          "f32 xcall returned an error");
    return result;
}

static double run_f64(const double *left, const double *right, int dim, bool sq) {
    varlena_t leftValue, rightValue;
    double result = 0;
    xcall_args_t args[3] = {0};
    set_inline_vector(&leftValue, left, dim * (int)sizeof(double));
    set_inline_vector(&rightValue, right, dim * (int)sizeof(double));
    args[0].pdata = (uint8_t *)&result;
    args[0].dataSz = sizeof(result);
    args[1].pdata = (uint8_t *)&leftValue;
    args[1].dataSz = VARLENA_SZ;
    args[2].pdata = (uint8_t *)&rightValue;
    args[2].dataSz = VARLENA_SZ;
    CHECK(xcall_l2distance_f64(RUNTIME_C, NULL, (uint64_t *)args, 1, sq) == 0,
          "f64 xcall returned an error");
    return result;
}

int main(void) {
    const float f32Large[] = {3e38f, 3e38f};
    const float f32Zero[] = {0, 0};
    const double f32Distance = run_f32(f32Large, f32Zero, 2, false);
    const double f32Expected = (double)f32Large[0] * sqrt(2.0);
    CHECK(isfinite(f32Distance), "finite f32 values must not overflow L2");
    CHECK(fabs(f32Distance / f32Expected - 1.0) < 1e-12,
          "f32 XCall L2 result is inaccurate");

    const double f64Tiny[] = {1e-310, 1e-310};
    const double f64Zero[] = {0, 0};
    const double f64Distance = run_f64(f64Tiny, f64Zero, 2, false);
    const double f64Expected = 1e-310 * sqrt(2.0);
    CHECK(f64Distance > 0, "tiny finite f64 values must not underflow to zero");
    CHECK(fabs(f64Distance / f64Expected - 1.0) < 1e-12,
          "f64 XCall tiny L2 result is inaccurate");

    const double f64Max[] = {DBL_MAX, DBL_MAX};
    const double f64Min[] = {-DBL_MAX, -DBL_MAX};
    CHECK(isinf(run_f64(f64Max, f64Min, 2, false)),
          "unrepresentable f64 L2 result must be positive infinity");

    const float f32NaN[] = {NAN, 0};
    const float f32Inf[] = {INFINITY, 0};
    const float f32InfNaN[] = {INFINITY, NAN};
    CHECK(isnan(run_f32(f32NaN, f32Zero, 2, false)) &&
              isnan(run_f32(f32NaN, f32Zero, 2, true)),
          "f32 XCall must preserve NaN in both L2 modes");
    CHECK(isnan(run_f32(f32Inf, f32Inf, 2, false)) &&
              isnan(run_f32(f32Inf, f32Inf, 2, true)),
          "f32 XCall must preserve Inf-Inf as NaN");
    CHECK(isnan(run_f32(f32InfNaN, f32Zero, 2, false)) &&
              isnan(run_f32(f32InfNaN, f32Zero, 2, true)),
          "f32 XCall must scan past infinity for NaN");
    CHECK(isinf(run_f32(f32Inf, f32Zero, 2, false)) &&
              isinf(run_f32(f32Inf, f32Zero, 2, true)),
          "f32 XCall must preserve infinity in both L2 modes");

    const double f64NaN[] = {NAN, 0};
    const double f64Inf[] = {INFINITY, 0};
    const double f64InfNaN[] = {INFINITY, NAN};
    CHECK(isnan(run_f64(f64NaN, f64Zero, 2, false)) &&
              isnan(run_f64(f64NaN, f64Zero, 2, true)),
          "f64 XCall must preserve NaN in both L2 modes");
    CHECK(isnan(run_f64(f64Inf, f64Inf, 2, false)) &&
              isnan(run_f64(f64Inf, f64Inf, 2, true)),
          "f64 XCall must preserve Inf-Inf as NaN");
    CHECK(isnan(run_f64(f64InfNaN, f64Zero, 2, false)) &&
              isnan(run_f64(f64InfNaN, f64Zero, 2, true)),
          "f64 XCall must scan past infinity for NaN");
    CHECK(isinf(run_f64(f64Inf, f64Zero, 2, false)) &&
              isinf(run_f64(f64Inf, f64Zero, 2, true)),
          "f64 XCall must preserve infinity in both L2 modes");

    puts("xcall stable L2 tests passed");
    return 0;
}
