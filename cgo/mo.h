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

#ifndef _MO_H_
#define _MO_H_

#include <stdint.h>
#include <stdbool.h>

/* Bitmap ops */
void Bitmap_Add(uint64_t *p, uint64_t pos);
void Bitmap_Remove(uint64_t *p, uint64_t pos);
bool Bitmap_Contains(uint64_t *p, uint64_t pos);
bool Bitmap_IsEmpty(uint64_t *p, uint64_t nbits);
uint64_t Bitmap_Count(uint64_t *p, uint64_t nbits);
void Bitmap_And(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits);
void Bitmap_Or(uint64_t *dst, uint64_t *a, uint64_t *b, uint64_t nbits);
void Bitmap_Not(uint64_t *dst, uint64_t *a, uint64_t nbits);

/* vector arithmatics */
int32_t SignedInt_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t UnsignedInt_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t Float_VecAdd(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);

int32_t SignedInt_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t UnsignedInt_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t Float_VecSub(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);

int32_t SignedInt_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t UnsignedInt_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t Float_VecMul(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);

int32_t Float_VecDiv(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t Float_VecIntegerDiv(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);

int32_t SignedInt_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t UnsignedInt_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);
int32_t Float_VecMod(void *r, void *a, void *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t szof);

/* compare operator */
int32_t Numeric_VecEq(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

int32_t Numeric_VecNe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

int32_t Numeric_VecGt(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

int32_t Numeric_VecGe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

int32_t Numeric_VecLt(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

int32_t Numeric_VecLe(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag, int32_t type);

/* vector logical operation */
int32_t Logic_VecAnd(void *r, void *a, void  *b, uint64_t n, uint64_t *anulls, uint64_t *bnulls, uint64_t *rnulls, int32_t flag);
int32_t Logic_VecOr(void *r, void *a, void  *b, uint64_t n, uint64_t *anulls, uint64_t *bnulls, uint64_t *rnulls, int32_t flag);
int32_t Logic_VecXor(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag);
int32_t Logic_VecNot(void *r, void *a, uint64_t n, uint64_t *nulls, int32_t flag);

#endif /* _MO_H_ */
