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

func SubStrIndex(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if ivecs[0].IsConstNull() || ivecs[1].IsConstNull() || ivecs[2].IsConstNull() {
		return vector.NewConstNull(*ivecs[0].GetType(), ivecs[0].Length(), proc.Mp()), nil
	}
	//get the first arg str
	sourceCols := vector.MustStrCol(ivecs[0])
	//get the second arg delim
	delimCols := vector.MustStrCol(ivecs[1])
	//get the third arg count
	countCols := getCount(ivecs[2])

	//calcute rows
	rowCount := ivecs[0].Length()

	rvals := make([]string, rowCount)

	constVectors := []bool{ivecs[0].IsConst(), ivecs[1].IsConst(), ivecs[2].IsConst()}
	//get result values
	substrindex.SubStrIndex(sourceCols, delimCols, countCols, rowCount, constVectors, rvals)
	rvec := vector.NewVec(types.T_varchar.ToType())
	vector.AppendStringList(rvec, rvals, nil, proc.Mp())

	// set null row
	nulls.Or(ivecs[0].GetNulls(), ivecs[1].GetNulls(), rvec.GetNulls())
	nulls.Or(ivecs[2].GetNulls(), rvec.GetNulls(), rvec.GetNulls())

	return rvec, nil
}

func getCount(vec *vector.Vector) []int64 {
	switch vec.GetType().Oid {
	case types.T_float64:
		vs := vector.MustFixedCol[float64](vec)
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
		vs := vector.MustFixedCol[uint64](vec)
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
