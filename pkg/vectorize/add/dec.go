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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func dec64PtrToC(p *types.Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func dec128PtrToC(p *types.Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func Decimal64VecAdd(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[types.Decimal64](rs)
	if xs.Typ.Scale > ys.Typ.Scale {
		rs.Typ.Scale = xs.Typ.Scale
	} else {
		rs.Typ.Scale = ys.Typ.Scale
	}
	n := len(rt)
	err = nil
	if xs.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Add(yt[i], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Add(yt[0], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Add(yt[i], xs.Typ.Scale, ys.Typ.Scale)

	}
	return
}

func Decimal128VecAdd(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[types.Decimal128](rs)
	if xs.Typ.Scale > ys.Typ.Scale {
		rs.Typ.Scale = xs.Typ.Scale
	} else {
		rs.Typ.Scale = ys.Typ.Scale
	}
	n := len(rt)
	err = nil
	if xs.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Add(yt[i], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Add(yt[0], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Add(yt[i], xs.Typ.Scale, ys.Typ.Scale)
		if err != nil {
			return
		}
	}
	return
}
