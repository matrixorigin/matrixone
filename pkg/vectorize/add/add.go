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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
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
		return moerr.NewOutOfRange("int", "int add overflow")
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
					return moerr.NewOutOfRange("int", "int add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if signedOverflow(x, yt[0], rt[i]) {
					return moerr.NewOutOfRange("int", "int add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if signedOverflow(x, yt[i], rt[i]) {
					return moerr.NewOutOfRange("int", "int add overflow")
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
		return moerr.NewOutOfRange("unsigned int", "unsigned int add overflow")
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
					return moerr.NewOutOfRange("unsigned int", "unsigned int add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if unsignedOverflow(x, yt[0], rt[i]) {
					return moerr.NewOutOfRange("unsigned int", "unsigned int add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if unsignedOverflow(x, yt[i], rt[i]) {
					return moerr.NewOutOfRange("unsigned int", "unsigned int add overflow")
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
		return moerr.NewOutOfRange("float", "float add overflow")
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

func StringAddString(xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustStrCols(xs), vector.MustStrCols(ys), vector.MustTCols[float64](rs)
	xf := make([]float64, len(xt))
	yf := make([]float64, len(yt))
	binary.StringToFloat[float64](xt, xf)
	binary.StringToFloat[float64](yt, yf)

	if xs.IsScalar() {
		for i, y := range yf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xf[0] + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yf[0]
			}
		}
		return nil
	} else {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yf[i]
			}
		}
		return nil
	}
}

func StringAddFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustStrCols(xs), vector.MustTCols[T](ys), vector.MustTCols[float64](rs)
	xf := make([]float64, len(xt))
	binary.StringToFloat[float64](xt, xf)

	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xf[0] + float64(y)
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[0])
			}
		}
		return nil
	} else {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[i])
			}
		}
		return nil
	}
}

func StringAddSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustStrCols(xs), vector.MustTCols[T](ys), vector.MustTCols[float64](rs)
	xf := make([]float64, len(xt))
	binary.StringToFloat[float64](xt, xf)

	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xf[0] + float64(y)
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[0])
			}
		}
		return nil
	} else {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[i])
			}
		}
		return nil
	}
}

func StringAddUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustStrCols(xs), vector.MustTCols[T](ys), vector.MustTCols[float64](rs)
	xf := make([]float64, len(xt))
	binary.StringToFloat[float64](xt, xf)

	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xf[0] + float64(y)
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[0])
			}
		}
		return nil
	} else {
		for i, x := range xf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + float64(yt[i])
			}
		}
		return nil
	}
}

func FloatAddString[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustStrCols(ys), vector.MustTCols[float64](rs)
	yf := make([]float64, len(yt))
	binary.StringToFloat[float64](yt, yf)

	if xs.IsScalar() {
		for i, y := range yf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(xt[0]) + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[i]
			}
		}
		return nil
	}
}

func SignedAddString[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustStrCols(ys), vector.MustTCols[float64](rs)

	yf := make([]float64, len(yt))
	binary.StringToFloat[float64](yt, yf)

	if xs.IsScalar() {
		for i, y := range yf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(xt[0]) + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[i]
			}
		}
		return nil
	}
}

func UnsignedAddString[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustStrCols(ys), vector.MustTCols[float64](rs)

	yf := make([]float64, len(yt))
	binary.StringToFloat[float64](yt, yf)

	if xs.IsScalar() {
		for i, y := range yf {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(xt[0]) + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = float64(x) + yf[i]
			}
		}
		return nil
	}
}

func Uint32AddScalar(x uint32, ys []uint32, zs []uint32) {
	rc := C.UnsignedInt_VecAdd(unsafe.Pointer(&zs[0]), unsafe.Pointer(&x), unsafe.Pointer(&ys[0]),
		C.uint64_t(len(zs)), nil, C.int32_t(1), C.int32_t(4))
	if rc != 0 {
		panic(moerr.NewOutOfRange("uint32", "uint32 add overflow"))
	}
}
