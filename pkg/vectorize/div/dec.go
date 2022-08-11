// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package div

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

const (
	RC_DIVISION_BY_ZERO = 2000
	RC_OUT_OF_RANGE     = 2001
)

func dec64PtrToC(p *types.Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func dec128PtrToC(p *types.Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func Decimal64VecDiv(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[types.Decimal128](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Decimal64_VecDiv(dec128PtrToC(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		if rc == RC_DIVISION_BY_ZERO {
			return moerr.NewError(moerr.DIVIVISION_BY_ZERO, "Decimal64 div by zero")
		} else if rc == RC_OUT_OF_RANGE {
			return moerr.NewError(moerr.OUT_OF_RANGE, "Decimal64 div overflow")
		} else {
			return moerr.NewError(moerr.INTERNAL_ERROR, "Decimal64 div internal error")
		}
	}
	return nil
}

func Decimal128VecDiv(xs, ys, rs *vector.Vector) error {
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

	rc := C.Decimal128_VecDiv(dec128PtrToC(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		if rc == RC_DIVISION_BY_ZERO {
			return moerr.NewError(moerr.DIVIVISION_BY_ZERO, "Decimal128 div by zero")
		} else if rc == RC_OUT_OF_RANGE {
			return moerr.NewError(moerr.OUT_OF_RANGE, "Decimal128 div overflow")
		} else {
			return moerr.NewError(moerr.INTERNAL_ERROR, "Decimal128 div internal error")
		}
	}
	return nil
}
