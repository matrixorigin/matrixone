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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Due to concerns that the vector may consume additional memory,
// I set a more conservative row-capacity for each block to ensure its memory not more than 1 gigabyte.
const (
	blockCapacityFor1Byte   = 1024 * 1024 * 800
	blockCapacityFor2Byte   = 1024 * 1024 * 400
	blockCapacityFor4Byte   = 1024 * 1024 * 200
	blockCapacityFor8Byte   = 1024 * 1024 * 100
	blockCapacityFor12Byte  = 1024 * 1024 * 60
	blockCapacityFor16Byte  = 1024 * 1024 * 50
	blockCapacityFor24Byte  = 1024 * 1024 * 30
	blockCapacityFor32Byte  = 1024 * 1024 * 25
	blockCapacityFor64Byte  = 1024 * 1024 * 12
	blockCapacityFor128Byte = 1024 * 1024 * 6
	blockCapacityForStrType = 8192 * 4
)

var blockCapacityMap = map[int]int{
	1:   blockCapacityFor1Byte,
	2:   blockCapacityFor2Byte,
	4:   blockCapacityFor4Byte,
	8:   blockCapacityFor8Byte,
	12:  blockCapacityFor12Byte,
	16:  blockCapacityFor16Byte,
	24:  blockCapacityFor24Byte,
	32:  blockCapacityFor32Byte,
	64:  blockCapacityFor64Byte,
	128: blockCapacityFor128Byte,
}

var _ = optSplitResult{}

// optSplitResult is a more stable version for aggregation basic result.
//
// this structure will split the aggregation result as many part of `vector`,
// it was to avoid one block memory exceeds the limit.
//
// But should watch that, the split action will make a performance degradation
// due to
// 1. the simple get and set method will convert to {get the block and the row index, get / set result}.
// 2. more pointer allocation.
type optSplitResult struct {
	mp *mpool.MPool

	// some information to help we improve performance.
	optInformation struct {
		// nsp opt.
		doesThisNeedEmptyList     bool
		shouldSetNullToEmptyGroup bool

		// split opt.
		eachSplitCapacity int
	}

	// the column type of result,
	// it was used to generate the result vector.
	resultType types.Type

	// the list of each group's result and empty situation.
	resultList       []*vector.Vector
	emptyList        []*vector.Vector
	nowIdx1, nowIdx2 int
}

func (r *optSplitResult) init(
	mg AggMemoryManager, typ types.Type) {
	if mg != nil {
		r.mp = mg.Mp()
	}
	r.resultType = typ

	r.optInformation.doesThisNeedEmptyList = true
	r.optInformation.shouldSetNullToEmptyGroup = true

	r.optInformation.eachSplitCapacity = blockCapacityForStrType
	if !typ.IsVarlen() {
		if newCap, ok := blockCapacityMap[typ.TypeSize()]; ok {
			r.optInformation.eachSplitCapacity = newCap
		}
	}

	r.resultList = append(r.resultList, vector.NewOffHeapVecWithType(typ))
	r.emptyList = append(r.emptyList, vector.NewOffHeapVecWithType(types.T_bool.ToType()))
	r.nowIdx1, r.nowIdx2 = 0, 0
}

func (r *optSplitResult) noNeedToCountEmptyGroup() {
	r.optInformation.doesThisNeedEmptyList = false
	r.optInformation.shouldSetNullToEmptyGroup = false
}

func getNspFromBoolVector(v *vector.Vector) *nulls.Nulls {
	bs := vector.MustFixedColNoTypeCheck[bool](v)

	nsp := nulls.NewWithSize(len(bs))
	bm := nsp.GetBitmap()
	for i, j := uint64(0), uint64(len(bs)); i < j; i++ {
		if bs[i] {
			bm.Add(i)
		}
	}
	return nsp
}

// flushOneVector return the agg result one by one.
//
// for the better performance, I will not update the result status.
// so, once flush starts, do not call other methods except the free function.
func (r *optSplitResult) flushOneVector() *vector.Vector {
	if len(r.resultList) > 0 {
		ret := r.resultList[0]
		r.resultList = r.resultList[1:]

		if r.optInformation.doesThisNeedEmptyList && r.optInformation.shouldSetNullToEmptyGroup {
			ret.SetNulls(getNspFromBoolVector(r.emptyList[0]))
			r.emptyList = r.emptyList[1:]
		}
		return ret
	}
	return nil
}

func (r *optSplitResult) extend(more int) error {

	// simple extend if there is no need to do append.
	maxToExtendWithoutAppend := r.optInformation.eachSplitCapacity - r.nowIdx2
	if maxToExtendWithoutAppend > more {
		if err := r.resultList[r.nowIdx1].PreExtend(more, r.mp); err != nil {
			return err
		}
		if err := r.emptyList[r.nowIdx1].PreExtend(more, r.mp); err != nil {
			return err
		}

		r.nowIdx2 += more
		r.resultList[r.nowIdx1].SetLength(r.nowIdx2)
		r.emptyList[r.nowIdx2].SetLength(r.nowIdx2)
		return nil
	}

	// full the last result first.
	if err := r.resultList[r.nowIdx1].PreExtend(maxToExtendWithoutAppend, r.mp); err != nil {
		return err
	}
	if err := r.emptyList[r.nowIdx1].PreExtend(maxToExtendWithoutAppend, r.mp); err != nil {
		return err
	}
	r.resultList[r.nowIdx1].SetLength(r.optInformation.eachSplitCapacity)
	r.emptyList[r.nowIdx2].SetLength(r.optInformation.eachSplitCapacity)

	more -= maxToExtendWithoutAppend

	apFullCount, rowMore := more/r.optInformation.eachSplitCapacity, more%r.optInformation.eachSplitCapacity
	for i := 0; i < apFullCount; i++ {
		v1 := vector.NewOffHeapVecWithType(r.resultType)
		v2 := vector.NewOffHeapVecWithType(types.T_bool.ToType())
		if err := v1.PreExtend(r.optInformation.eachSplitCapacity, r.mp); err != nil {
			return err
		}
		if err := v2.PreExtend(r.optInformation.eachSplitCapacity, r.mp); err != nil {
			return err
		}
		v1.SetLength(r.optInformation.eachSplitCapacity)
		v2.SetLength(r.optInformation.eachSplitCapacity)

		r.resultList = append(r.resultList, v1)
		r.emptyList = append(r.emptyList, v2)
	}

	if rowMore > 0 {
		v1 := vector.NewOffHeapVecWithType(r.resultType)
		v2 := vector.NewOffHeapVecWithType(types.T_bool.ToType())

		if err := v1.PreExtend(rowMore, r.mp); err != nil {
			return err
		}
		if err := v2.PreExtend(rowMore, r.mp); err != nil {
			return err
		}
		v1.SetLength(rowMore)
		v2.SetLength(rowMore)

		r.resultList = append(r.resultList, v1)
		r.emptyList = append(r.emptyList, v2)
	}

	r.nowIdx1, r.nowIdx2 = len(r.resultList)-1, rowMore
	return nil
}
