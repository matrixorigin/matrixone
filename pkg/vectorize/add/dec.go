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

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Decimal64VecAdd(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[types.Decimal64](rs)
	if xs.GetType().Scale > ys.GetType().Scale {
		rs.GetType().Scale = xs.GetType().Scale
	} else {
		rs.GetType().Scale = ys.GetType().Scale
	}
	n := len(rt)
	err = nil
	if xs.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Add(yt[i], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Add(yt[0], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Add(yt[i], xs.GetType().Scale, ys.GetType().Scale)

	}
	return
}

func Decimal128VecAdd(xs, ys, rs *vector.Vector) (err error) {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[types.Decimal128](rs)
	if xs.GetType().Scale > ys.GetType().Scale {
		rs.GetType().Scale = xs.GetType().Scale
	} else {
		rs.GetType().Scale = ys.GetType().Scale
	}
	n := len(rt)
	err = nil
	if xs.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[0].Add(yt[i], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	if ys.IsConst() {
		for i := 0; i < n; i++ {
			rt[i], _, err = xt[i].Add(yt[0], xs.GetType().Scale, ys.GetType().Scale)
			if err != nil {
				return
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		rt[i], _, err = xt[i].Add(yt[i], xs.GetType().Scale, ys.GetType().Scale)
		if err != nil {
			return
		}
	}
	return
}
