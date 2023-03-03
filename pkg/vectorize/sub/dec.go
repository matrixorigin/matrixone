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

package sub

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/
import "C"

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func Decimal64VecSub(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[types.Decimal64](rs)
	if xs.Typ.Scale > ys.Typ.Scale {
		rs.Typ.Scale = xs.Typ.Scale
	} else {
		rs.Typ.Scale = ys.Typ.Scale
	}
	n := len(rt)
	if xs.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Sub(yt[i], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Sub(yt[0], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Sub(yt[i], xs.Typ.Scale, ys.Typ.Scale)
		if err != nil {
			return
		}
	}
	return
}

func Decimal128VecSub(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[types.Decimal128](rs)
	if xs.Typ.Scale > ys.Typ.Scale {
		rs.Typ.Scale = xs.Typ.Scale
	} else {
		rs.Typ.Scale = ys.Typ.Scale
	}
	n := len(rt)
	if xs.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Sub(yt[i], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsScalar() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Sub(yt[0], xs.Typ.Scale, ys.Typ.Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Sub(yt[i], xs.Typ.Scale, ys.Typ.Scale)
		if err != nil {
			return
		}
	}
	return
}

func DatetimeSub(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Datetime](xs)
	yt := vector.MustTCols[types.Datetime](ys)
	rt := vector.MustTCols[int64](rs)

	if len(xt) == 1 && len(yt) == 1 {
		res := xt[0].DatetimeMinusWithSecond(yt[0])
		rt[0] = res
	} else if len(xt) == 1 {
		for i := range yt {
			res := xt[0].DatetimeMinusWithSecond(yt[i])
			rt[i] = res
		}
	} else if len(yt) == 1 {
		for i := range xt {
			res := xt[i].DatetimeMinusWithSecond(yt[0])
			rt[i] = res
		}
	} else {
		for i := range xt {
			res := xt[i].DatetimeMinusWithSecond(yt[i])
			rt[i] = res
		}
	}
	return nil
}
