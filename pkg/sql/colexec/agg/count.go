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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Decimal128AndString interface {
	types.Decimal | []byte | bool | types.Uuid
}

type Count[T1 types.OrderedT | Decimal128AndString] struct {
	// IsStar is true: count(*)
	IsStar bool
}

func CountReturnType(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}

func NewCount[T1 types.OrderedT | Decimal128AndString](isStar bool) *Count[T1] {
	return &Count[T1]{IsStar: isStar}
}

func (c *Count[T1]) Grows(_ int) {
}

func (c *Count[T1]) Eval(vs []int64, err error) ([]int64, error) {
	return vs, nil
}

func (c *Count[T1]) Merge(_, _ int64, x, y int64, _ bool, _ bool, _ any) (int64, bool, error) {
	return x + y, false, nil
}

func (c *Count[T1]) Fill(_ int64, _ T1, v int64, z int64, _ bool, hasNull bool) (int64, bool, error) {
	if hasNull {
		if !c.IsStar {
			return v, false, nil
		} else {
			return v + z, false, nil
		}
	}
	return v + z, false, nil
}

func (c *Count[T1]) BatchFill(rs, _ any, start, count int64, vps []uint64, nsp *nulls.Nulls) error {
	resultVs := rs.([]int64)
	// count(*) will count the null
	if c.IsStar || !nsp.Any() {
		for i := int64(0); i < count; i++ {
			if vps[i] != 0 {
				resultVs[vps[i]-1]++
			}
		}
	} else {
		for i := int64(0); i < count; i++ {
			index := uint64(i + start)
			if !nsp.Contains(index) && vps[i] != 0 {
				resultVs[vps[i]-1]++
			}
		}
	}
	return nil
}

func (c *Count[T1]) MarshalBinary() ([]byte, error) {
	return types.EncodeBool(&c.IsStar), nil
}

func (c *Count[T1]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	c.IsStar = types.DecodeBool(copyData)
	return nil
}
