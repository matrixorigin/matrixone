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

package count

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ReturnType(_ []types.Type) types.Type {
	return types.New(types.T_int64, 0, 0, 0)
}

func New[T1 types.Generic | Decimal128AndString](isStar bool) *Count[T1] {
	return &Count[T1]{isStar: isStar}
}

func (c *Count[T1]) Grows(_ int) {
}

func (c *Count[T1]) Eval(vs []int64) []int64 {
	return vs
}

func (c *Count[T1]) Merge(_, _ int64, x, y int64, _ bool, _ bool, _ any) (int64, bool) {
	return x + y, false
}

func (c *Count[T1]) Fill(_ int64, _ T1, v int64, z int64, _ bool, hasNull bool) (int64, bool) {
	if hasNull && !c.isStar {
		return v, false
	}
	return v + z, false
}
