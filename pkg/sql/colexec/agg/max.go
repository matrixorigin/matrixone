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

package agg

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Max[T Compare] struct {
}
type Decimal64Max struct {
}

type Decimal128Max struct {
}

type BoolMax struct {
}

type StrMax struct {
}

type UuidMax struct {
}

var MaxSupported = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_varchar, types.T_char, types.T_blob, types.T_text,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
}

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

func NewMax[T Compare]() *Max[T] {
	return &Max[T]{}
}

func (m *Max[T]) Grows(_ int) {
}

func (m *Max[T]) Eval(vs []T, err error) ([]T, error) {
	return vs, nil
}

func (m *Max[T]) Fill(_ int64, value T, ov T, _ int64, isEmpty bool, isNull bool) (T, bool, error) {
	if !isNull {
		if value > ov || isEmpty {
			return value, false, nil
		}
	}
	return ov, isEmpty, nil
}

func (m *Max[T]) Merge(_ int64, _ int64, x T, y T, xEmpty bool, yEmpty bool, _ any) (T, bool, error) {
	if !yEmpty {
		if !xEmpty && x > y {
			return x, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *Max[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Max[T]) UnmarshalBinary(data []byte) error {
	return nil
}

func NewD64Max() *Decimal64Max {
	return &Decimal64Max{}
}

func (m *Decimal64Max) Grows(_ int) {
}

func (m *Decimal64Max) Eval(vs []types.Decimal64, err error) ([]types.Decimal64, error) {
	return vs, nil
}

func (m *Decimal64Max) Fill(_ int64, value types.Decimal64, ov types.Decimal64, _ int64, isEmpty bool, isNull bool) (types.Decimal64, bool, error) {
	if !isNull {
		if value.Compare(ov) > 0 || isEmpty {
			return value, false, nil
		}
	}
	return ov, isEmpty, nil

}
func (m *Decimal64Max) Merge(_ int64, _ int64, x types.Decimal64, y types.Decimal64, xEmpty bool, yEmpty bool, _ any) (types.Decimal64, bool, error) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) > 0 {
			return x, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *Decimal64Max) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Decimal64Max) UnmarshalBinary(data []byte) error {
	return nil
}

func NewD128Max() *Decimal128Max {
	return &Decimal128Max{}
}

func (m *Decimal128Max) Grows(_ int) {
}

func (m *Decimal128Max) Eval(vs []types.Decimal128, err error) ([]types.Decimal128, error) {
	return vs, nil
}

func (m *Decimal128Max) Fill(_ int64, value types.Decimal128, ov types.Decimal128, _ int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if !isNull {
		if ov.Compare(value) <= 0 || isEmpty {
			return value, false, nil
		}
	}
	return ov, isEmpty, nil

}
func (m *Decimal128Max) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool, error) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) > 0 {
			return x, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *Decimal128Max) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Decimal128Max) UnmarshalBinary(data []byte) error {
	return nil
}

func NewBoolMax() *BoolMax {
	return &BoolMax{}
}

func (m *BoolMax) Grows(_ int) {
}

func (m *BoolMax) Eval(vs []bool, err error) ([]bool, error) {
	return vs, nil
}

func (m *BoolMax) Fill(_ int64, value bool, ov bool, _ int64, isEmpty bool, isNull bool) (bool, bool, error) {
	if !isNull {
		if isEmpty {
			return value, false, nil
		}
		return value || ov, false, nil
	}
	return ov, isEmpty, nil

}
func (m *BoolMax) Merge(_ int64, _ int64, x bool, y bool, xEmpty bool, yEmpty bool, _ any) (bool, bool, error) {
	if !yEmpty {
		if !xEmpty {
			return x || y, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *BoolMax) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *BoolMax) UnmarshalBinary(data []byte) error {
	return nil
}

func NewStrMax() *StrMax {
	return &StrMax{}
}

func (m *StrMax) Grows(_ int) {
}

func (m *StrMax) Eval(vs [][]byte, err error) ([][]byte, error) {
	return vs, nil
}

func (m *StrMax) Fill(_ int64, value []byte, ov []byte, _ int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		if bytes.Compare(value, ov) > 0 || isEmpty {
			v := make([]byte, 0, len(value))
			v = append(v, value...)
			return v, false, nil
		}
	}
	return ov, isEmpty, nil

}
func (m *StrMax) Merge(_ int64, _ int64, x []byte, y []byte, xEmpty bool, yEmpty bool, _ any) ([]byte, bool, error) {
	if !yEmpty {
		if !xEmpty && bytes.Compare(x, y) > 0 {
			return x, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *StrMax) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *StrMax) UnmarshalBinary(data []byte) error {
	return nil
}

func NewUuidMax() *UuidMax {
	return &UuidMax{}
}

func (m *UuidMax) Grows(_ int) {
}

func (m *UuidMax) Eval(vs []types.Uuid, err error) ([]types.Uuid, error) {
	return vs, nil
}

func (m *UuidMax) Fill(_ int64, value types.Uuid, ov types.Uuid, _ int64, isEmpty bool, isNull bool) (types.Uuid, bool, error) {
	if !isNull {
		if ov.Le(value) || isEmpty {
			return value, false, nil
		}
	}
	return ov, isEmpty, nil

}
func (m *UuidMax) Merge(_ int64, _ int64, x types.Uuid, y types.Uuid, xEmpty bool, yEmpty bool, _ any) (types.Uuid, bool, error) {
	if !yEmpty {
		if !xEmpty && x.Gt(y) {
			return x, false, nil
		}
		return y, false, nil
	}
	return x, xEmpty, nil
}

func (m *UuidMax) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *UuidMax) UnmarshalBinary(data []byte) error {
	return nil
}
