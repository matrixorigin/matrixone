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

package avg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func NewAvg[T Numeric]() *Avg[T] {
	return &Avg[T]{
		cnt: int64(0),
	}
}

func (a *Avg[T]) Grows(_ int) {
}

func (a *Avg[T]) Eval(vs []float64) []float64 {
	for i := range vs {
		vs[i] = float64(vs[i]) / float64(a.cnt)
	}
	return vs
}

func (a *Avg[T]) Fill(_ int64, value T, ov float64, z int64, isEmpty bool, isNull bool) (float64, bool) {
	if !isNull {
		a.cnt += z
		return ov + float64(value)*float64(z), false
	}
	return ov, isEmpty
}

func (a *Avg[T]) Merge(xIndex int64, yIndex int64, x float64, y float64, xEmpty bool, yEmpty bool, yAvg any) (float64, bool) {
	if !yEmpty {
		ya := yAvg.(*Avg[T])
		a.cnt += ya.cnt
		if !xEmpty {
			return x + y, false
		}
		return y, false
	}

	return x, xEmpty
}

func NewD64Avg() *Decimal64Avg {
	return &Decimal64Avg{
		cnt: int64(0),
	}
}

func (a *Decimal64Avg) Grows(_ int) {
}

func (a *Decimal64Avg) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i := range vs {
		vs[i] = vs[i].DivInt64(a.cnt)
	}
	return vs
}

func (a *Decimal64Avg) Fill(_ int64, value types.Decimal64, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		a.cnt += z
		tmp := types.InitDecimal128(value.ToInt64())
		return ov.Add(tmp.MulInt64(z)), false
	}
	return ov, isEmpty
}

func (a *Decimal64Avg) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, yAvg any) (types.Decimal128, bool) {
	if !yEmpty {
		ya := yAvg.(*Decimal128Avg)
		a.cnt += ya.cnt
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}

	return x, xEmpty
}

func NewD128Avg() *Decimal128Avg {
	return &Decimal128Avg{
		cnt: int64(0),
	}
}

func (a *Decimal128Avg) Grows(_ int) {
}

func (a *Decimal128Avg) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i := range vs {
		vs[i] = vs[i].DivInt64(a.cnt)
	}
	return vs
}

func (a *Decimal128Avg) Fill(_ int64, value types.Decimal128, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		a.cnt += z
		return ov.Add(value.MulInt64(z)), false
	}
	return ov, isEmpty
}

func (a *Decimal128Avg) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, yAvg any) (types.Decimal128, bool) {
	if !yEmpty {
		ya := yAvg.(*Decimal128Avg)
		a.cnt += ya.cnt
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}

	return x, xEmpty
}
