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

package max

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

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
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		if y > x {
			return y, false
		} else {
			return x, false
		}
	}

	return x, true
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
		if types.CompareDecimal128Decimal128Aligned(value, ov) == 1 || isEmpty {
			return value, false
		}
	}
	return ov, isEmpty

}
func (m *Decimal128Max) Merge(_ int64, _ int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, _ any) (types.Decimal128, bool) {
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		if types.CompareDecimal128Decimal128Aligned(x, y) == 1 {
			return x, false
		}
		return y, false
	}
	return x, true
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
		return (value || ov), false
	}
	return ov, isEmpty

}
func (m *BoolMax) Merge(_ int64, _ int64, x bool, y bool, xEmpty bool, yEmpty bool, _ any) (bool, bool) {
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		return x || y, false
	}
	return x, true
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
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		if bytes.Compare(x, y) > 0 {
			return x, false
		} else {
			return y, false
		}
	}
	return x, true
}
