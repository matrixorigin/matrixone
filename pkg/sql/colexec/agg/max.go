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

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

func NewMax[T Compare]() *Max[T] {
	return &Max[T]{}
}

func (m *Max[T]) Grows(_ int) {
}

func (m *Max[T]) Eval(vs []T) []T {
	return vs
}

func (m *Max[T]) Fill(_ int64, value T, ov T, _ int64, isEmpty bool, isNull bool) (T, bool) {
	if !isNull {
		if value > ov || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty
}

func (m *Max[T]) Merge(_ int64, _ int64, x T, y T, xEmpty bool, yEmpty bool, _ any) (T, bool) {
	if !yEmpty {
		if !xEmpty && x > y {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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

func (m *Decimal64Max) Eval(vs []types.Decimal64) []types.Decimal64 {
	return vs
}

func (m *Decimal64Max) Fill(_ int64, value types.Decimal64, ov types.Decimal64, _ int64, isEmpty bool, isNull bool) (types.Decimal64, bool) {
	if !isNull {
		if value.Compare(ov) > 0 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal64Max) Merge(_ int64, _ int64, x types.Decimal64, y types.Decimal64, xEmpty bool, yEmpty bool, _ any) (types.Decimal64, bool) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) > 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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

func (m *Decimal128Max) Eval(vs []types.Decimal128) []types.Decimal128 {
	return vs
}

func (m *Decimal128Max) Fill(_ int64, value types.Decimal128, ov types.Decimal128, _ int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		if ov.Compare(value) <= 0 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal128Max) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool) {
	if !yEmpty {
		if !xEmpty && x.Compare(y) > 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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

func (m *BoolMax) Eval(vs []bool) []bool {
	return vs
}

func (m *BoolMax) Fill(_ int64, value bool, ov bool, _ int64, isEmpty bool, isNull bool) (bool, bool) {
	if !isNull {
		if isEmpty {
			return value, false
		}
		return value || ov, false
	}
	return ov, isEmpty

}
func (m *BoolMax) Merge(_ int64, _ int64, x bool, y bool, xEmpty bool, yEmpty bool, _ any) (bool, bool) {
	if !yEmpty {
		if !xEmpty {
			return x || y, false
		}
		return y, false
	}
	return x, xEmpty
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

func (m *StrMax) Eval(vs [][]byte) [][]byte {
	return vs
}

func (m *StrMax) Fill(_ int64, value []byte, ov []byte, _ int64, isEmpty bool, isNull bool) ([]byte, bool) {
	if !isNull {
		if bytes.Compare(value, ov) > 0 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *StrMax) Merge(_ int64, _ int64, x []byte, y []byte, xEmpty bool, yEmpty bool, _ any) ([]byte, bool) {
	if !yEmpty {
		if !xEmpty && bytes.Compare(x, y) > 0 {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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

func (m *UuidMax) Eval(vs []types.Uuid) []types.Uuid {
	return vs
}

func (m *UuidMax) Fill(_ int64, value types.Uuid, ov types.Uuid, _ int64, isEmpty bool, isNull bool) (types.Uuid, bool) {
	if !isNull {
		if ov.Le(value) || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *UuidMax) Merge(_ int64, _ int64, x types.Uuid, y types.Uuid, xEmpty bool, yEmpty bool, _ any) (types.Uuid, bool) {
	if !yEmpty {
		if !xEmpty && x.Gt(y) {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (m *UuidMax) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *UuidMax) UnmarshalBinary(data []byte) error {
	return nil
}
