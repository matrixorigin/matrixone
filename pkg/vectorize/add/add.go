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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func signedOverflow[T constraints.Signed](a, b, c T) bool {
	return (a > 0 && b > 0 && c < 0) || (a < 0 && b < 0 && c > 0)
}

func unsignedOverflow[T constraints.Unsigned](a, b, c T) bool {
	return c < a || c < b
}

func NumericAddSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	return cNumericAddSigned[T](xs, ys, rs)
}

func cNumericAddSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.SignedInt_VecAdd(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("int", "int add overflow")
	}
	return nil
}

func goNumericAddSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
				if signedOverflow(xt[0], y, rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if signedOverflow(x, yt[0], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if signedOverflow(x, yt[i], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int add overflow")
				}
			}
		}
		return nil
	}
}

func NumericAddUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	return cNumericAddUnsigned[T](xs, ys, rs)
}

func cNumericAddUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.UnsignedInt_VecAdd(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int add overflow")
	}
	return nil
}

func goNumericAddUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
				if unsignedOverflow(xt[0], y, rt[i]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if unsignedOverflow(x, yt[0], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if unsignedOverflow(x, yt[i], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int add overflow")
				}
			}
		}
		return nil
	}
}

func NumericAddFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	return cNumericAddFloat[T](xs, ys, rs)
}

func cNumericAddFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Float_VecAdd(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("float", "float add overflow")
	}
	return nil
}

func goNumericAddFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
			}
		}
		return nil
	}
}

func Uint32AddScalar(x uint32, ys []uint32, zs []uint32) {
	rc := C.UnsignedInt_VecAdd(unsafe.Pointer(&zs[0]), unsafe.Pointer(&x), unsafe.Pointer(&ys[0]),
		C.uint64_t(len(zs)), nil, C.int32_t(1), C.int32_t(4))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("uint32", "uint32 add overflow"))
	}
}
