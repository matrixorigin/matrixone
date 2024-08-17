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

extern "C" 
{
#include "../xcall.h"
#include "../bitmap.h"
}

extern "C"
int32_t cuda_l2distance_sq_f32(double *pres, int n, bool sq,
        varlena_t *p1, uint8_t *area1, bool isconst1,
        varlena_t *p2, uint8_t *area2, bool isconst2
        ) {
    return -1;
}

/* well well well, does it worth to make f32/f64 a macro?  probably not */
extern "C"
int32_t cuda_l2distance_sq_f64(double *pres, int n, bool sq,
        varlena_t *p1, uint8_t *area1, bool isconst1,
        varlena_t *p2, uint8_t *area2, bool isconst2
        ) {
    return -1;
}

