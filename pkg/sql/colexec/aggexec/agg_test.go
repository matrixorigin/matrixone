// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func fromValueListToVector[T types.FixedSizeTExceptStrType](
	mp *mpool.MPool,
	typ types.Type, values []T, isNull []bool) *vector.Vector {

	v := vector.NewVec(typ)
	var err error
	switch typ.Oid {
	case types.T_int64:
		err = vector.AppendFixedList[int64](v, any(values).([]int64), isNull, mp)

	case types.T_bool:
		err = vector.AppendFixedList[bool](v, any(values).([]bool), isNull, mp)

	default:
		panic(fmt.Sprintf("test util do not support the type %s now", typ))
	}

	if err != nil {
		panic(err)
	}
	return v
}

func fromIdxListToNullList(start, end int, idxList []int) []bool {
	if len(idxList) == 0 {
		return nil
	}

	bs := make([]bool, end-start+1)
	for _, idx := range idxList {
		if idx >= start && idx <= end {
			bs[idx] = true
		}
	}
	return bs
}

func doAggTest[input types.FixedSizeTExceptStrType, output types.FixedSizeTExceptStrType](
	t *testing.T,
	agg AggFuncExec,
	mp *mpool.MPool, paramType types.Type,
	group1 []input, nullList1 []int, result1 output, isResult1Null bool,
	group2 []input, nullList2 []int, result2 output, isResult2Null bool,
) {

	// tool methods.
	cutGroup := func(
		typ types.Type,
		group []input, nullList []int,
	) (v1, v2, v3 *vector.Vector) {
		// we cut the group input data as 3 part for test the following methods.
		// 1. fill.
		// 2. bulk fill.
		// 3. batch fill.
		switch len(group) {
		case 0:
			v1, v2, v3 = nil, nil, nil
		case 1:
			v1 = fromValueListToVector[input](mp, typ, group, fromIdxListToNullList(0, 0, nullList))
			v2 = nil
			v3 = nil
		case 2:
			v1 = fromValueListToVector[input](mp, typ, group[:1], fromIdxListToNullList(0, 0, nullList))
			v2 = fromValueListToVector[input](mp, typ, group[1:], fromIdxListToNullList(1, 1, nullList))
			v3 = nil
		default:
			gap := len(group) / 3
			v1 = fromValueListToVector[input](mp, typ, group[:gap], fromIdxListToNullList(0, gap-1, nullList))
			v2 = fromValueListToVector[input](mp, typ, group[gap:2*gap], fromIdxListToNullList(gap, 2*gap-1, nullList))
			v3 = fromValueListToVector[input](mp, typ, group[2*gap:], fromIdxListToNullList(2*gap, len(group)-1, nullList))
		}

		return v1, v2, v3
	}

	fillToGroup := func(
		idx int, v1, v2, v3 *vector.Vector) {

		// 1. fill
		if v1 != nil {
			vs := []*vector.Vector{v1}
			for i, j := 0, v1.Length(); i < j; i++ {
				require.NoError(t, agg.Fill(idx, i, vs))
			}
		}

		// 2. bulk fill
		if v2 != nil {
			vs := []*vector.Vector{v2}
			require.NoError(t, agg.BulkFill(idx, vs))
		}

		// 3. batch fill
		if v3 != nil {
			gs := make([]uint64, v3.Length())
			v := uint64(idx + 1)
			for i := range gs {
				gs[i] = v
			}
			vs := []*vector.Vector{v3}
			require.NoError(t, agg.BatchFill(0, gs, vs))
		}
	}

	checkResult := func(
		expectedNull bool, expectedResult output, resultV *vector.Vector, row uint64) {
		if expectedNull {
			require.True(t, resultV.IsNull(row))
		} else {
			require.Equal(t, expectedResult, vector.GetFixedAtNoTypeCheck[output](resultV, int(row)))
		}
	}

	// Real Logic start from here.
	// 1. fill group1 first.
	// 2. fill group2 second.
	// 3. add merge action (not implement now).
	// 4. marshal and unmarshal (not implement now).
	// 5. flush the result and do result check.
	require.NoError(t, agg.GroupGrow(1))
	q1, q2, q3 := cutGroup(paramType, group1, nullList1)
	fillToGroup(0, q1, q2, q3)

	require.NoError(t, agg.GroupGrow(1))
	p1, p2, p3 := cutGroup(paramType, group2, nullList2)
	fillToGroup(1, p1, p2, p3)

	r, err := agg.Flush()
	require.NoError(t, err)

	checkResult(isResult1Null, result1, r, 0)
	checkResult(isResult2Null, result2, r, 1)
}

type hackManager struct {
	mp *mpool.MPool
}

func (h hackManager) Mp() *mpool.MPool {
	return h.mp
}

func hackAggMemoryManager() hackManager {
	return hackManager{mp: mpool.MustNewZeroNoFixed()}
}

func TestCount(t *testing.T) {
	m := hackAggMemoryManager()
	info := singleAggInfo{
		aggID:    aggIdOfCountColumn,
		distinct: false,
		retType:  types.T_int64.ToType(),
	}
	a := newCountColumnExecExec(m, info)

	doAggTest[int64, int64](
		t, a,
		m.Mp(), types.T_int64.ToType(),
		[]int64{1, 2, 3}, []int{0}, 2, false,
		[]int64{1, 2, 3}, nil, 3, false)
}
