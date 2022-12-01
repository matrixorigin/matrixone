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

package sub

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
	"github.com/matrixorigin/matrixone/pkg/vectorize/overflow"
	"golang.org/x/exp/constraints"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func goNumericSubUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] - y
				if overflow.OverflowUIntSub(xt[i], y, rt[i]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int SUB")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[0]
				if overflow.OverflowUIntSub(x, yt[0], rt[0]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int SUB")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[i]
				if overflow.OverflowUIntSub(x, yt[i], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int SUB")
				}
			}
		}
		return nil
	}
}

func goNumericSubSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] - y
				if overflow.OverflowIntSub(xt[0], y, rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int SUB")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[0]
				if overflow.OverflowIntSub(x, yt[0], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int SUB")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[i]
				if overflow.OverflowIntSub(x, yt[i], rt[i]) {
					return moerr.NewOutOfRangeNoCtx("int", "int SUB")
				}
			}
		}
		return nil
	}
}

func goNumericSubFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] - y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x - yt[i]
			}
		}
		return nil
	}
}

func NumericSubSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.SignedInt_VecSub(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("int", "int SUB")
	}
	return nil
}

func NumericSubUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	rc := C.UnsignedInt_VecSub(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("unsigned int", "unsigned int SUB")
	}
	return nil
}

func NumericSubFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Float_VecSub(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(rs.Typ.TypeSize()))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("float", "float SUB")
	}
	return nil
}
