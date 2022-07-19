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

package bit_xor

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TransInt(a any) int64 {
	switch v := a.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0 //can't reach
	}
}

func ReturnType(_ []types.Type) types.Type {
	return types.New(types.T_int64, 0, 0, 0)
}

func New[T1, T2 any]() *BitXor[T1, T2] {
	return &BitXor[T1, T2]{}
}

func (c *BitXor[T1, T2]) Grows(_ int) {
}

func (c *BitXor[T1, T2]) Eval(vs []T2) []T2 {
	return vs
}

func (c *BitXor[T1, T2]) Merge(_, _ int64, x, y T2, IsEmpty1 bool, IsEmpty2 bool, _ any) (T2, bool) {
	if IsEmpty1 && !IsEmpty2 {
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		return any(TransInt(x) ^ TransInt(y)).(T2), false
	}
}

func (c *BitXor[T1, T2]) Fill(_ int64, v1 T1, v2 T2, z int64, IsEmpty bool, hasNull bool) (T2, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		if z%2 == 0 {
			return (any)(0).(T2), false
		} else {
			return (any)(TransInt(v1)).(T2), false
		}
	}
	if z%2 == 0 {
		return any(TransInt(v1)).(T2), false
	} else {
		return any(TransInt(v1) ^ TransInt(v2)).(T2), false
	}
}
