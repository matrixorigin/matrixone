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

package multi

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	substrindex "github.com/matrixorigin/matrixone/pkg/vectorize/subStrIndex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func SubStrIndex(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(vecs[0].Typ), nil
	}
	//get the first arg str
	sourceCols := vector.MustStrCols(vecs[0])
	//get the second arg delim
	delimCols := vector.MustStrCols(vecs[1])
	//get the third arg count
	countCols := getCount(vecs[2])

	//calcute rows
	rowCount := vector.Length(vecs[0])

	var resultVec *vector.Vector = nil
	resultValues := make([]string, rowCount)
	resultNsp := nulls.NewWithSize(rowCount)

	// set null row
	nulls.Or(vecs[0].Nsp, vecs[1].Nsp, resultNsp)
	nulls.Or(vecs[2].Nsp, resultNsp, resultNsp)

	constVectors := []bool{vecs[0].IsScalar(), vecs[1].IsScalar(), vecs[2].IsScalar()}
	//get result values
	substrindex.SubStrIndex(sourceCols, delimCols, countCols, rowCount, constVectors, resultValues)
	resultVec = vector.NewWithStrings(types.T_varchar.ToType(), resultValues, resultNsp, proc.Mp())

	return resultVec, nil
}

func getCount(vec *vector.Vector) []int64 {
	switch vec.GetType().Oid {
	case types.T_float64:
		vs := vector.MustTCols[float64](vec)
		res := make([]int64, 0, len(vs))
		for _, v := range vs {
			if v > float64(math.MaxInt64) {
				res = append(res, math.MaxInt64)
			} else if v < float64(math.MinInt64) {
				res = append(res, math.MinInt64)
			} else {
				res = append(res, int64(v))
			}
		}
		return res
	case types.T_uint64:
		vs := vector.MustTCols[uint64](vec)
		res := make([]int64, 0, len(vs))
		for _, v := range vs {
			if v > uint64(math.MaxInt64) {
				res = append(res, math.MaxInt64)
			} else {
				res = append(res, int64(v))
			}
		}
		return res
	default:
		return castTVecAsInt64(vec)
	}
}
