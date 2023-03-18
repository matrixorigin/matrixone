// Copyright 2022 Matrix Origin
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

package compare

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
	"golang.org/x/exp/constraints"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func GetScalarFlag(xs, ys, rs *vector.Vector) int {
	flag := 0
	if xs.IsConst() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsConst() {
		flag |= RIGHT_IS_SCALAR
	}
	return flag
}

func NumericEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecEq(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric equal", "")
	}
	return nil
}

func NumericNotEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecNe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric not equal", "")
	}
	return nil
}

func NumericGreatThan[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecGt(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric greater than", "")
	}
	return nil
}

func NumericGreatEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecGe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric greater equal", "")
	}
	return nil
}

func NumericLessThan[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecLt(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric less than", "")
	}
	return nil
}

func NumericLessEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecLe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(int32(xs.GetType().Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric less equal", "")
	}
	return nil
}

func Decimal64VecEq(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x == yt[i])
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0] == y)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x == yt[0])
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i] == y)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x == yt[i])
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i] == y)
		}
	}
	return nil
}

func Decimal128VecEq(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x == yt[i])
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0] == y)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x == yt[0])
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i] == y)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x == yt[i])
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i] == y)
		}
	}
	return nil
}

func Decimal64VecNe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x != yt[i])
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0] != y)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x != yt[0])
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i] != y)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x != yt[i])
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i] != y)
		}
	}
	return nil
}

func Decimal128VecNe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x != yt[i])
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0] != y)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x != yt[0])
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i] != y)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x != yt[i])
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i] != y)
		}
	}
	return nil
}

func Decimal64VecGt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) > 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) > 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) > 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) > 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) > 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) > 0)
		}
	}
	return nil
}

func Decimal128VecGt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) > 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) > 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) > 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) > 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) > 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) > 0)
		}
	}
	return nil
}

func Decimal64VecGe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) >= 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) >= 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) >= 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) >= 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) >= 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) >= 0)
		}
	}
	return nil
}

func Decimal128VecGe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) >= 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) >= 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) >= 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) >= 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) >= 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) >= 0)
		}
	}
	return nil
}

func Decimal64VecLt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) < 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) < 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) < 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) < 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) < 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) < 0)
		}
	}
	return nil
}

func Decimal128VecLt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) < 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) < 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) < 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) < 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) < 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) < 0)
		}
	}
	return nil
}

func Decimal64VecLe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) <= 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) <= 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) <= 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) <= 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) <= 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) <= 0)
		}
	}
	return nil
}

func Decimal128VecLe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[bool](rs)
	n := len(rt)
	m := ys.GetType().Scale - xs.GetType().Scale
	if xs.IsConst() {
		if m >= 0 {
			x, _ := xt[0].Scale(m)
			for i := 0; i < n; i++ {
				rt[i] = (x.Compare(yt[i]) <= 0)
			}
		} else {
			for i := 0; i < n; i++ {
				y, _ := yt[i].Scale(-m)
				rt[i] = (xt[0].Compare(y) <= 0)
			}
		}
		return nil
	}
	if ys.IsConst() {
		if m >= 0 {
			for i := 0; i < n; i++ {
				x, _ := xt[i].Scale(m)
				rt[i] = (x.Compare(yt[0]) <= 0)
			}

		} else {
			y, _ := yt[0].Scale(-m)
			for i := 0; i < n; i++ {
				rt[i] = (xt[i].Compare(y) <= 0)
			}
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if m >= 0 {
			x, _ := xt[i].Scale(m)
			rt[i] = (x.Compare(yt[i]) <= 0)
		} else {
			y, _ := yt[i].Scale(-m)
			rt[i] = (xt[i].Compare(y) <= 0)
		}
	}
	return nil
}
