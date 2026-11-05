// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// decimalReductionPrefix never owns or publishes persistent aggregate state.
// Rows ends before the first unhandled four-row block; scalar summation resumes there.
type decimalReductionPrefix struct {
	rows  int
	sum   types.Decimal128
	count int64
}

// Concrete carriers keep SIMD conversions out of scalar executor dictionaries.
func decimalBulkReduce(exec *aggExec, isSum bool, groupIndex int, v *vector.Vector) error {
	x, y := exec.getXY(uint64(groupIndex))
	state := exec.state[x].vecs[0]
	target := exec.aggInfo.stateTypes[0].Oid
	source := v.GetType().Oid
	// Same rule as newSumAvgDecExec, restricted to its decimal inputs.
	uncheckedLocal := source != types.T_decimal256
	n := v.Length()
	prefix := reduceDecimalPrefix(v, 0, n)
	if target == types.T_decimal128 {
		sum := prefix.sum
		for i := prefix.rows; i < n; i++ {
			var value types.Decimal128
			if source == types.T_decimal64 {
				value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, i)))
			} else {
				value = vector.GetFixedAtNoTypeCheck[types.Decimal128](v, i)
			}
			var err error
			if uncheckedLocal {
				sum = sum.Add128Unchecked(value)
			} else {
				sum, err = sum.Add128(value)
			}
			if err != nil {
				return err
			}
		}
		sums := chunkArr[types.Decimal128](state)
		if isSum && state.IsNull(uint64(y)) {
			state.UnsetNull(uint64(y))
			sums[y] = sum
			return nil
		}
		var err error
		sums[y], err = sums[y].Add128(sum)
		if err != nil {
			return err
		}
	} else {
		sum := types.Decimal256FromDecimal128(prefix.sum)
		for i := prefix.rows; i < n; i++ {
			var value types.Decimal256
			switch source {
			case types.T_decimal64:
				value = types.Decimal256FromInt64(int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, i)))
			case types.T_decimal128:
				value = types.Decimal256FromDecimal128(vector.GetFixedAtNoTypeCheck[types.Decimal128](v, i))
			case types.T_decimal256:
				value = vector.GetFixedAtNoTypeCheck[types.Decimal256](v, i)
			}
			var err error
			sum, err = sum.Add256(value)
			if uncheckedLocal {
				err = nil
			}
			if err != nil {
				return err
			}
		}
		sums := chunkArr[types.Decimal256](state)
		if isSum && state.IsNull(uint64(y)) {
			state.UnsetNull(uint64(y))
			sums[y] = sum
			return nil
		}
		var err error
		sums[y], err = sums[y].Add256(sum)
		if err != nil {
			return err
		}
	}
	if !isSum {
		vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])[y] += int64(n)
	}
	return nil
}
