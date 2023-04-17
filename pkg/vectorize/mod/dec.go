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

package mod

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Decimal64VecMod(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[types.Decimal64](rs)
	n := len(rt)
	if xs.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[0].Mod(yt[i], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[i].Mod(yt[0], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], rs.GetType().Scale, err = xt[i].Mod(yt[i], xs.GetType().Scale, ys.GetType().Scale)
		if err != nil {
			return
		}
	}
	return nil
}

func Decimal128VecMod(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[types.Decimal128](rs)
	n := len(rt)
	if xs.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[0].Mod(yt[i], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], rs.GetType().Scale, err = xt[i].Mod(yt[0], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], rs.GetType().Scale, err = xt[i].Mod(yt[i], xs.GetType().Scale, ys.GetType().Scale)
		if err != nil {
			return
		}
	}
	return
}
