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

package mult

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/
import "C"
import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Decimal64VecMult(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[types.Decimal128](rs)
	n := len(rt)
	if xs.IsConst() {
		x := types.Decimal128{B0_63: uint64(xt[0]), B64_127: 0}
		if xt[0]>>63 != 0 {
			x.B64_127 = ^x.B64_127
		}
		for i := 0; i < n; i++ {
			y := types.Decimal128{B0_63: uint64(yt[i]), B64_127: 0}
			if yt[i]>>63 != 0 {
				y.B64_127 = ^y.B64_127
			}
			rt[i], rs.GetType().Scale, err = x.Mul(y, xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		y := types.Decimal128{B0_63: uint64(yt[0]), B64_127: 0}
		if yt[0]>>63 != 0 {
			y.B64_127 = ^y.B64_127
		}
		for i := 0; i < n; i++ {
			x := types.Decimal128{B0_63: uint64(xt[i]), B64_127: 0}
			if xt[i]>>63 != 0 {
				x.B64_127 = ^x.B64_127
			}
			rt[i], rs.GetType().Scale, err = x.Mul(y, xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		x := types.Decimal128{B0_63: uint64(xt[i]), B64_127: 0}
		if xt[i]>>63 != 0 {
			x.B64_127 = ^x.B64_127
		}
		y := types.Decimal128{B0_63: uint64(yt[i]), B64_127: 0}
		if yt[i]>>63 != 0 {
			y.B64_127 = ^y.B64_127
		}
		rt[i], rs.GetType().Scale, err = x.Mul(y, xs.GetType().Scale, ys.GetType().Scale)
		if err != nil {
			return
		}
	}
	return nil
}

func Decimal128VecMult(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[types.Decimal128](rs)
	n := len(rt)
	if xs.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[0].Mul(yt[i], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[i].Mul(yt[0], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], rs.GetType().Scale, err = xt[i].Mul(yt[i], xs.GetType().Scale, ys.GetType().Scale)
		if err != nil {
			return
		}
	}
	return
}
