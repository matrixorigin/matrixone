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

package sum

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_float32, types.T_float64:
		return types.New(types.T_float64, 0, 0, 0)
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return types.New(types.T_int64, 0, 0, 0)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return types.New(types.T_uint64, 0, 0, 0)
	case types.T_decimal64:
		return typs[0]
	case types.T_decimal128:
		return typs[0]
	}
	panic(fmt.Errorf("unsupport type '%v' for sum", typs[0]))
}

func NewSum[T1 Numeric, T2 ReturnTyp]() *Sum[T1, T2] {
	return &Sum[T1, T2]{}
}

func (s *Sum[T1, T2]) Grows(_ int) {
}

func (s *Sum[T1, T2]) Eval(vs []T2) []T2 {
	return vs
}

func (s *Sum[T1, T2]) Fill(_ int64, value T1, ov T2, z int64, isEmpty bool, isNull bool) (T2, bool) {
	if !isNull {
		return ov + T2(value)*T2(z), false
	}
	return ov, isEmpty
}

func (s *Sum[T1, T2]) Merge(_ int64, _ int64, x T2, y T2, xEmpty bool, yEmpty bool, _ any) (T2, bool) {
	if !yEmpty {
		if !xEmpty {
			return x + y, false
		}
		return y, false
	}
	return x, xEmpty

}

func NewD64Sum() *Decimal64Sum {
	return &Decimal64Sum{}
}

func (s *Decimal64Sum) Grows(_ int) {
}

func (s *Decimal64Sum) Eval(vs []types.Decimal64) []types.Decimal64 {
	return vs
}

func (s *Decimal64Sum) Fill(_ int64, value types.Decimal64, ov types.Decimal64, z int64, isEmpty bool, isNull bool) (types.Decimal64, bool) {
	if !isNull {
		return ov.Add(value.MulInt64(z)), false
	}
	return ov, isEmpty
}

func (s *Decimal64Sum) Merge(_ int64, _ int64, x types.Decimal64, y types.Decimal64, xEmpty bool, yEmpty bool, _ any) (types.Decimal64, bool) {
	if !yEmpty {
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}
	return x, xEmpty
}

func NewD128Sum() *Decimal128Sum {
	return &Decimal128Sum{}
}

func (s *Decimal128Sum) Grows(_ int) {
}

func (s *Decimal128Sum) Eval(vs []types.Decimal128) []types.Decimal128 {
	return vs
}

func (s *Decimal128Sum) Fill(_ int64, value types.Decimal128, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		return ov.Add(value.MulInt64(z)), false
	}
	return ov, isEmpty
}

func (s *Decimal128Sum) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool) {
	if !yEmpty {
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}
	return x, xEmpty
}
