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

/* Decimal.   Both decimal64 and decimal128 are passed in/out as int64_t* */
int32_t Decimal64_Compare(int32_t *r, int64_t *a, int64_t *b);
int32_t Decimal128_Compare(int32_t *r, int64_t *a, int64_t *b);

int32_t Decimal64_FromInt32(int64_t *r, int32_t v);
int32_t Decimal128_FromInt32(int64_t *r, int32_t v);

int32_t Decimal64_FromUint32(int64_t *r, uint32_t v);
int32_t Decimal128_FromUint32(int64_t *r, uint32_t v);

int32_t Decimal64_FromInt64(int64_t *r, int64_t v);
int32_t Decimal128_FromInt64(int64_t *r, int64_t v);

int32_t Decimal64_FromUint64(int64_t *r, uint64_t v);
int32_t Decimal128_FromUint64(int64_t *r, uint64_t v);

int32_t Decimal64_FromFloat64(int64_t *r, double v);
int32_t Decimal128_FromFloat64(int64_t *r, double v);

int32_t Decimal64_FromString(int64_t *r, char* s);
int32_t Decimal128_FromString(int64_t *r, char* s);

int32_t Decimal64_FromStringWithScale(int64_t *r, char* s, int32_t scale);
int32_t Decimal128_FromStringWithScale(int64_t *r, char* s, int32_t scale);

int32_t Decimal64_ToFloat64(double *r, int64_t *d);
int32_t Decimal128_ToFloat64(double *r, int64_t *d);

int32_t Decimal64_ToInt64(int64_t *r, int64_t *d);
int32_t Decimal128_ToInt64(int64_t *r, int64_t *d);

int32_t Decimal64_ToString(char *s, int64_t *d);
int32_t Decimal128_ToString(char *s, int64_t *d);

int32_t Decimal64_ToStringWithScale(char *s, int64_t *d, int32_t scale);
int32_t Decimal128_ToStringWithScale(char *s, int64_t *d, int32_t scale);

int32_t Decimal64_ToDecimal128(int64_t *d128, int64_t *d64);
int32_t Decimal128_ToDecimal64(int64_t *d64, int64_t *d128);

int32_t Decimal64_Add(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_AddInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal64_Sub(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_SubInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal64_Mul(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_MulWiden(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_MulInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal64_Div(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_DivWiden(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal64_DivInt64(int64_t *r, int64_t *a, int64_t b);

int32_t Decimal128_Add(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal128_AddInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal128_AddDecimal64(int64_t *r, int64_t *a, int64_t* b);
int32_t Decimal128_Sub(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal128_SubInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal128_Mul(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal128_MulInt64(int64_t *r, int64_t *a, int64_t b);
int32_t Decimal128_Div(int64_t *r, int64_t *a, int64_t *b);
int32_t Decimal128_DivInt64(int64_t *r, int64_t *a, int64_t b);

#endif /* _MO_H_ */
