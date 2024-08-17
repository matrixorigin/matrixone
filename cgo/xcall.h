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

#ifndef _XCALL_H_
#define _XCALL_H_

#include <stdbool.h>
#include <string.h>
#include <stdint.h>

typedef struct xcall_args_t {
    uint64_t *pnulls;
    uint64_t nullCnt;
    uint8_t *pdata;
    uint64_t dataSz;
    uint8_t *parea;
    uint64_t areaSz;
} xcall_args_t;

#define VARLENA_SZ 24
#define VARLENA_INLINE_SZ 23
typedef struct varlena_t {
    uint8_t bs[24];
} varlena_t;

typedef struct ptrlen_t {
    void *ptr;
    int len;
} ptrlen_t;

int varlena_get_ptrlen(varlena_t *va, uint8_t *area, ptrlen_t *pl);

int32_t xcall_l2distance_f32(uint64_t *args, uint64_t len);
int32_t xcall_l2distance_f64(uint64_t *args, uint64_t len);
int32_t xcall_l2distance_sq_f32(uint64_t *args, uint64_t len);
int32_t xcall_l2distance_sq_f64(uint64_t *args, uint64_t len);

#ifdef MO_CL_CUDA
int32_t cuda_l2distance_f32(uint64_t *args, uint64_t len);
int32_t cuda_l2distance_f64(uint64_t *args, uint64_t len);
int32_t cuda_l2distance_sq_f32(uint64_t *args, uint64_t len);
int32_t cuda_l2distance_sq_f64(uint64_t *args, uint64_t len);
#endif


#endif /* _XCALL_H_ */

