// Copyright 2021 Matrix Origin
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

package add

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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func dec64PtrToC(p *types.Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func dec128PtrToC(p *types.Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func Decimal64VecAdd(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[types.Decimal64](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Decimal64_VecAdd(dec64PtrToC(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal64", "decimal add overflow")
	}
	return nil
}

func Decimal128VecAdd(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[types.Decimal128](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	rc := C.Decimal128_VecAdd(dec128PtrToC(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal128", "decimal add overflow")
	}
	return nil
}
