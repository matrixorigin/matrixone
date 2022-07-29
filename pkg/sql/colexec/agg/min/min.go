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

package min

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ReturnType(typs []types.Type) types.Type {
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
		if value.Lt(ov) || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal64Min) Merge(_ int64, _ int64, x types.Decimal64, y types.Decimal64, xEmpty bool, yEmpty bool, _ any) (types.Decimal64, bool) {
	if !yEmpty {
		if !xEmpty && x.Lt(y) {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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
		if value.Lt(ov) || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal128Min) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool) {
	if !yEmpty {
		if !xEmpty && x.Lt(y) {
			return x, false
		}
		return y, false
	}
	return x, xEmpty
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
			return value, false
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
