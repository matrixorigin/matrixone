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

type Min[T Compare] struct {
}
type Decimal64Min struct {
}

type Decimal128Min struct {
}

type BoolMin struct {
}

type StrMin struct {
}

type UuidMin struct {
}

var MinSupported = []types.T{
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

func MinReturnType(typs []types.Type) types.Type {
	return typs[0]
}

func NewMin[T Compare]() *Min[T] {
	return &Min[T]{}
}

func (m *Min[T]) Grows(_ int) {
}

func (m *Min[T]) Eval(vs []T) []T {
	return vs
}

func (m *Min[T]) Fill(_ int64, value T, ov T, _ int64, isEmpty bool, isNull bool) (T, bool) {
	if !isNull {
		if value < ov || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty
}

func (m *Min[T]) Merge(_ int64, _ int64, x T, y T, xEmpty bool, yEmpty bool, _ any) (T, bool) {
	if !yEmpty {
		if !xEmpty && x < y {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *Min[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Min[T]) UnmarshalBinary(data []byte) error {
	return nil
}

func NewD64Min() *Decimal64Min {
	return &Decimal64Min{}
}

func (m *Decimal64Min) Grows(_ int) {
}

func (m *Decimal64Min) Eval(vs []types.Decimal64) []types.Decimal64 {
	return vs
}

func (m *Decimal64Min) Fill(_ int64, value types.Decimal64, ov types.Decimal64, _ int64, isEmpty bool, isNull bool) (types.Decimal64, bool) {
	if !isNull {
		if value.Compare(ov) < 0 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal64Min) Merge(_ int64, _ int64, x types.Decimal64, y types.Decimal64, xEmpty bool, yEmpty bool, _ any) (types.Decimal64, bool) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) < 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *Decimal64Min) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Decimal64Min) UnmarshalBinary(data []byte) error {
	return nil
}

func NewD128Min() *Decimal128Min {
	return &Decimal128Min{}
}

func (m *Decimal128Min) Grows(_ int) {
}

func (m *Decimal128Min) Eval(vs []types.Decimal128) []types.Decimal128 {
	return vs
}

func (m *Decimal128Min) Fill(_ int64, value types.Decimal128, ov types.Decimal128, _ int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		if value.Compare(ov) < 0 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal128Min) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) < 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *Decimal128Min) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *Decimal128Min) UnmarshalBinary(data []byte) error {
	return nil
}

func NewBoolMin() *BoolMin {
	return &BoolMin{}
}

func (m *BoolMin) Grows(_ int) {
}

func (m *BoolMin) Eval(vs []bool) []bool {
	return vs
}

func (m *BoolMin) Fill(_ int64, value bool, ov bool, _ int64, isEmpty bool, isNull bool) (bool, bool) {
	if !isNull {
		if isEmpty {
			return value, false
		}
		return value && ov, false
	}
	return ov, isEmpty

}
func (m *BoolMin) Merge(_ int64, _ int64, x bool, y bool, xEmpty bool, yEmpty bool, _ any) (bool, bool) {
	if !yEmpty {
		if !xEmpty {
			return x && y, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *BoolMin) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *BoolMin) UnmarshalBinary(data []byte) error {
	return nil
}

func NewStrMin() *StrMin {
	return &StrMin{}
}

func (m *StrMin) Grows(_ int) {
}

func (m *StrMin) Eval(vs [][]byte) [][]byte {
	return vs
}

func (m *StrMin) Fill(_ int64, value []byte, ov []byte, _ int64, isEmpty bool, isNull bool) ([]byte, bool) {
	if !isNull {
		if bytes.Compare(value, ov) < 0 || isEmpty {
			v := make([]byte, 0, len(value))
			v = append(v, value...)
			return v, false
		}
	}
	return ov, isEmpty

}
func (m *StrMin) Merge(_ int64, _ int64, x []byte, y []byte, xEmpty bool, yEmpty bool, _ any) ([]byte, bool) {
	if !yEmpty {
		if !xEmpty && bytes.Compare(x, y) < 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *StrMin) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *StrMin) UnmarshalBinary(data []byte) error {
	return nil
}

func NewUuidMin() *UuidMin {
	return &UuidMin{}
}

func (m *UuidMin) Grows(_ int) {
}

func (m *UuidMin) Eval(vs []types.Uuid) []types.Uuid {
	return vs
}

func (m *UuidMin) Fill(_ int64, value types.Uuid, ov types.Uuid, _ int64, isEmpty bool, isNull bool) (types.Uuid, bool) {
	if !isNull {
		if value.Lt(ov) || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty
}

func (m *UuidMin) Merge(_ int64, _ int64, x types.Uuid, y types.Uuid, xEmpty bool, yEmpty bool, _ any) (types.Uuid, bool) {
	if !yEmpty {
		if !xEmpty && x.Lt(y) {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *UuidMin) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *UuidMin) UnmarshalBinary(data []byte) error {
	return nil
}
