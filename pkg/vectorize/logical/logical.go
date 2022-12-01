// Copyright 2021 - 2022 Matrix Origin
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

package logical

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/
import "C"

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func And(xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[bool](xs), vector.MustTCols[bool](ys), vector.MustTCols[bool](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	//int32_t Logic_VecAnd(void *r, void *a, void  *b, uint64_t n, uint64_t *anulls, uint64_t *bnulls, uint64_t *rnulls, int32_t flag)
	rc := C.Logic_VecAnd(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(xs.Nsp)), (*C.uint64_t)(nulls.Ptr(ys.Nsp)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInternalErrorNoCtx("logical AND")
	}
	return nil
}

func Or(xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[bool](xs), vector.MustTCols[bool](ys), vector.MustTCols[bool](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	rc := C.Logic_VecOr(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(xs.Nsp)), (*C.uint64_t)(nulls.Ptr(ys.Nsp)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInternalErrorNoCtx("logic OR")
	}
	return nil
}

func Xor(xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[bool](xs), vector.MustTCols[bool](ys), vector.MustTCols[bool](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	// int32_t Logic_VecXor(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag)
	rc := C.Logic_VecXor(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInternalErrorNoCtx("logic XOR")
	}
	return nil
}

func Not(xs, rs *vector.Vector) error {
	xt, rt := vector.MustTCols[bool](xs), vector.MustTCols[bool](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	rc := C.Logic_VecNot(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInternalErrorNoCtx("logic NOT")
	}
	return nil
}
