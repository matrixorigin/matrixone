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

// Runs one batch of f64 vectors through xcall_l2distance_f64 twice -- once on the C loop and once
// on the CUDA kernel -- and requires the two to agree.
//
// The l2distance_f64 CUDA kernels took their element difference in `float` while the C loop used
// `double`, so the two backends answered the same query differently: for [0.1] against [0] the
// kernel returned 0.010000000707805157 where the C loop (and exact double arithmetic) returns
// 0.010000000000000002. Nothing covered that -- the GPU dispatch is gated on batch size and vector
// width, so it is unreachable from a small SQL query, and no Go test or BVT case calls
// l2_distance_xc at all.
//
// The batch below is sized to clear every gate in xcall_l2distance_f64: 256 rows (>=
// CUDA_THREADS_PER_BLOCK), a 20-element f64 vector (160 bytes > 128), a non-const left operand, a
// const right operand, and no null bitmap. Built only when MO_CL_CUDA=1; without it the dispatch
// is compiled out and there is nothing to compare.

#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../xcall.h"

#define ROWS 256
#define DIM 20
#define VECBYTES (DIM * (int)sizeof(double))

#define CHECK(cond, message)                                     \
    do {                                                         \
        if (!(cond)) {                                           \
            fprintf(stderr, "xcall cuda check failed: %s\n", message); \
            return 1;                                            \
        }                                                        \
    } while (0)

// A vector wider than VARLENA_INLINE_SZ lives in the area buffer: bs[0] must exceed the inline
// limit, then offset and length follow as uint32 at words 1 and 2 (see varlena_get_ptrlen).
static void set_area_vector(varlena_t *value, uint32_t offset, uint32_t bytes) {
    memset(value, 0, sizeof(*value));
    value->bs[0] = VARLENA_INLINE_SZ + 1;
    uint32_t *p = (uint32_t *)value;
    p[1] = offset;
    p[2] = bytes;
}

static int run(int64_t rtid, double *out) {
    static varlena_t left[ROWS];
    static varlena_t right;
    static uint8_t area[(ROWS + 1) * VECBYTES];
    static double query[DIM];

    // every row is the same vector, so one expected value covers the batch
    for (int r = 0; r < ROWS; r++) {
        double v[DIM];
        for (int j = 0; j < DIM; j++) {
            v[j] = (j == 0) ? 0.1 : 0.0;
        }
        memcpy(area + (size_t)r * VECBYTES, v, VECBYTES);
        set_area_vector(&left[r], (uint32_t)(r * VECBYTES), (uint32_t)VECBYTES);
    }
    memset(query, 0, sizeof(query));
    memcpy(area + (size_t)ROWS * VECBYTES, query, VECBYTES);
    set_area_vector(&right, (uint32_t)(ROWS * VECBYTES), (uint32_t)VECBYTES);

    xcall_args_t args[3] = {0};
    args[0].pdata = (uint8_t *)out;
    args[0].dataSz = sizeof(double);
    args[0].pnulls = NULL;

    args[1].pdata = (uint8_t *)left;
    args[1].dataSz = sizeof(varlena_t) * ROWS; // != VARLENA_SZ -> non-const
    args[1].parea = area;

    args[2].pdata = (uint8_t *)&right;
    args[2].dataSz = VARLENA_SZ; // const
    args[2].parea = area;

    // errBuf must be a real buffer: the CUDA path writes into it on every failure branch,
    // including the "CUDA not initialised" one.
    static uint8_t errBuf[256];
    memset(errBuf, 0, sizeof(errBuf));
    int32_t rc = xcall_l2distance_f64(rtid, errBuf, (uint64_t *)args, ROWS, true);
    if (rc != 0) {
        fprintf(stderr, "xcall returned %d, errBuf=%.200s\n", rc, (char *)errBuf + 1);
    }
    return rc;
}

int main(int argc, char **argv) {
    static double cres[ROWS];
    static double gres[ROWS];
    // -c runs only the C loop, to separate a C-side failure from a CUDA-side one.
    const bool conly = (argc > 1 && strcmp(argv[1], "-c") == 0);

    CHECK(run(RUNTIME_C, cres) == 0, "C runtime returned an error");
    if (conly) {
        printf("xcall cuda f64: C loop gave %.17g\n", cres[0]);
        return 0;
    }
    CHECK(run(RUNTIME_CUDA, gres) == 0, "CUDA runtime returned an error");

    // 0.1*0.1 in double, the value both backends must produce
    const double expected = 0.1 * 0.1;

    for (int i = 0; i < ROWS; i++) {
        if (cres[i] != expected) {
            fprintf(stderr, "row %d: C loop gave %.17g, exact double is %.17g\n",
                    i, cres[i], expected);
            return 1;
        }
        if (gres[i] != cres[i]) {
            fprintf(stderr,
                    "row %d: CUDA gave %.17g but the C loop gave %.17g -- the backends disagree "
                    "(a float difference in an f64 kernel gives 0.010000000707805157)\n",
                    i, gres[i], cres[i]);
            return 1;
        }
    }

    // Guard against the comparison passing because the GPU never ran: a float difference would
    // land on 0.010000000707805157, so assert the value is the double one to full precision.
    CHECK(fabs(gres[0] - expected) == 0.0, "CUDA result is not the exact double square");

    printf("xcall cuda f64: C and CUDA agree on %d rows at %.17g\n", ROWS, gres[0]);
    return 0;
}
