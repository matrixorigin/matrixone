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
	resultList []*vector.Vector
	emptyList  []*vector.Vector
	nowIdx1    int

	// for easy get from / set to emptyList.
	bsFromEmptyList [][]bool

	// for easy get / set result and empty from outer.
	accessIdx1, accessIdx2 int
}

func (r *optSplitResult) marshalToBytes() ([][]byte, [][]byte, error) {
	var err error

	resultData := make([][]byte, len(r.resultList))
	emptyData := make([][]byte, len(r.emptyList))

	for i := range r.resultList {
		if resultData[i], err = r.resultList[i].MarshalBinary(); err != nil {
			return nil, nil, err
		}
	}
	for i := range r.emptyList {
		if emptyData[i], err = r.emptyList[i].MarshalBinary(); err != nil {
			return nil, nil, err
		}
	}
	return resultData, emptyData, nil
}

func (r *optSplitResult) unmarshalFromBytes(resultData [][]byte, emptyData [][]byte) error {
	var err error
	defer func() {
		if err != nil {
			for i := range r.resultList {
				if r.resultList[i] != nil {
					r.resultList[i].Free(r.mp)
				}
			}
			for i := range r.emptyList {
				if r.emptyList[i] != nil {
					r.emptyList[i].Free(r.mp)
				}
			}
		}
	}()

	r.resultList = make([]*vector.Vector, len(resultData))
	r.emptyList = make([]*vector.Vector, len(emptyData))
	for i := range r.resultList {
		r.resultList[i] = vector.NewOffHeapVecWithType(r.resultType)
		if err = vectorUnmarshal(r.resultList[i], resultData[i], r.mp); err != nil {
			return err
		}
	}
	for i := range r.emptyList {
		r.emptyList[i] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
		if err = vectorUnmarshal(r.emptyList[i], emptyData[i], r.mp); err != nil {
			return err
		}
	}
	return nil
}

func (r *optSplitResult) init(
	mg AggMemoryManager, typ types.Type, needEmptyList bool) {
	if mg != nil {
		r.mp = mg.Mp()
	}
	r.resultType = typ

	r.optInformation.doesThisNeedEmptyList = needEmptyList
	r.optInformation.shouldSetNullToEmptyGroup = needEmptyList

	r.optInformation.eachSplitCapacity = blockCapacityForStrType
	if !typ.IsVarlen() {
		if newCap, ok := blockCapacityMap[typ.TypeSize()]; ok {
			r.optInformation.eachSplitCapacity = newCap
		}
	}

	r.resultList = append(r.resultList, vector.NewOffHeapVecWithType(typ))
	if needEmptyList {
		r.emptyList = append(r.emptyList, vector.NewOffHeapVecWithType(types.T_bool.ToType()))
		r.bsFromEmptyList = append(r.bsFromEmptyList, nil)
	}
	r.nowIdx1 = 0
}

// getNspFromBoolVector generate a nsp bitmap from a bool type vector and return it.
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

// isOnlyOneBlock return true if only one block to save the result.
// this can help we reduce to call getResultRealIndex, because the x is always 0.
func (r *optSplitResult) isOnlyOneBlock() bool {
	return len(r.resultList) == 1
}

func (r *optSplitResult) getResultRealIndex(src int) (x, y int) {
	x = src / r.optInformation.eachSplitCapacity
	y = src % r.optInformation.eachSplitCapacity
	return x, y
}

func (r *optSplitResult) updateNextAccessIdx(idx int) (x, y int) {
	r.accessIdx1, r.accessIdx2 = r.getResultRealIndex(idx)
	return r.accessIdx1, r.accessIdx2
}

func (r *optSplitResult) setNextAccessDirectly(x, y int) {
	r.accessIdx1, r.accessIdx2 = x, y
}

func (r *optSplitResult) totalGroupCount() int {
	return r.resultList[r.nowIdx1].Length() + (len(r.resultList)-1)*r.optInformation.eachSplitCapacity
}

func (r *optSplitResult) isGroupEmpty(x, y int) bool {
	return r.bsFromEmptyList[x][y]
}

func (r *optSplitResult) setGroupNotEmpty(x, y int) {
	r.bsFromEmptyList[x][y] = false
}

func (r *optSplitResult) MergeAnotherEmpty(x, y int, anotherIsEmpty bool) {
	r.bsFromEmptyList[x][y] = r.bsFromEmptyList[x][y] && anotherIsEmpty
}

func (r *optSplitResult) getEachBlockLimitation() int {
	return r.optInformation.eachSplitCapacity
}

func (r *optSplitResult) getEmptyList() [][]bool {
	return r.bsFromEmptyList
}

func (r *optSplitResult) getEmptyListOnX(x int) []bool {
	return r.bsFromEmptyList[x]
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

// flushAll return all the result.
//
// todo: for easy refactor without changing too much, I remain this function.
func (r *optSplitResult) flushAll() []*vector.Vector {
	if r.optInformation.doesThisNeedEmptyList && r.optInformation.shouldSetNullToEmptyGroup {
		for i := range r.emptyList {
			r.resultList[i].SetNulls(getNspFromBoolVector(r.emptyList[i]))
		}
	}

	ret := r.resultList
	r.resultList = nil
	return ret
}

// extendResultPurely
// try to expand the length forward from the current position.
// if there is not enough free space, do memory allocation first.
//
// do not call this method directly, plz use the preExtend and resExtend.
func (r *optSplitResult) extendResultPurely(more int) error {

	// try tp full the using part first.
	l1 := r.resultList[r.nowIdx1].Length()
	maxToExtendWithinTheUsingPart := r.optInformation.eachSplitCapacity - l1
	if maxToExtendWithinTheUsingPart >= more {
		if err := r.extendMoreToKthGroup(r.nowIdx1, more); err != nil {
			return err
		}

		r.setLengthPartK(r.nowIdx1, l1+more)
		return nil
	}
	if err := r.extendMoreToKthGroup(r.nowIdx1, maxToExtendWithinTheUsingPart); err != nil {
		return err
	}
	r.setLengthPartK(r.nowIdx1, r.optInformation.eachSplitCapacity)
	more -= maxToExtendWithinTheUsingPart

	// try to full the allocated part first.
	maxToExtendWithoutPartAppend := (len(r.resultList) - 1 - r.nowIdx1) * r.optInformation.eachSplitCapacity
	if maxToExtendWithoutPartAppend >= more {
		r.nowIdx1++

		fullPart, rowMore := more/r.optInformation.eachSplitCapacity, more%r.optInformation.eachSplitCapacity
		for i, j := r.nowIdx1, r.nowIdx1+fullPart; i < j; i++ {
			if err := r.extendMoreToKthGroup(i, r.optInformation.eachSplitCapacity); err != nil {
				return err
			}
			r.setLengthPartK(i, r.optInformation.eachSplitCapacity)
		}

		if rowMore > 0 {
			r.nowIdx1 += fullPart
			if err := r.extendMoreToKthGroup(r.nowIdx1, rowMore); err != nil {
				return err
			}
			r.setLengthPartK(r.nowIdx1, rowMore)
		}
		return nil
	}
	for i := r.nowIdx1 + 1; i < len(r.resultList); i++ {
		if err := r.extendMoreToKthGroup(i, r.optInformation.eachSplitCapacity); err != nil {
			return err
		}
		r.setLengthPartK(i, r.optInformation.eachSplitCapacity)
	}
	more -= maxToExtendWithoutPartAppend

	// append more part.
	apFullPart, rowMore := more/r.optInformation.eachSplitCapacity, more%r.optInformation.eachSplitCapacity
	for i := 0; i < apFullPart; i++ {
		k := r.appendPartK()
		if err := r.extendMoreToKthGroup(k, r.optInformation.eachSplitCapacity); err != nil {
			return nil
		}
		r.setLengthPartK(k, r.optInformation.eachSplitCapacity)
	}
	if rowMore > 0 {
		k := r.appendPartK()
		if err := r.extendMoreToKthGroup(k, rowMore); err != nil {
			return err
		}
		r.setLengthPartK(k, rowMore)
	}
	r.nowIdx1 = len(r.resultList) - 1

	return nil
}

func (r *optSplitResult) extendMoreToKthGroup(k int, more int) error {
	if err := r.resultList[k].PreExtend(more, r.mp); err != nil {
		return err
	}
	if r.optInformation.doesThisNeedEmptyList {
		return r.emptyList[k].PreExtend(more, r.mp)
	}
	return nil
}

func (r *optSplitResult) setLengthPartK(k int, row int) {
	r.resultList[k].SetLength(row)
	if r.optInformation.doesThisNeedEmptyList {
		r.emptyList[k].SetLength(row)
	}
}

func (r *optSplitResult) appendPartK() int {
	r.resultList = append(r.resultList, vector.NewOffHeapVecWithType(r.resultType))
	if r.optInformation.doesThisNeedEmptyList {
		r.emptyList = append(r.emptyList, vector.NewOffHeapVecWithType(types.T_bool.ToType()))
	}
	return len(r.resultList) - 1
}

// preExtend
// allocate space of length more forward from the current position of the optSplitResult,
// and without any modification for all the memory usage indicators.
func (r *optSplitResult) preExtend(more int) (err error) {
	oldNowIdx1 := r.nowIdx1
	oldNowIdx2 := r.resultList[oldNowIdx1].Length()

	if err = r.extendResultPurely(more); err != nil {
		return err
	}

	r.resultList[oldNowIdx1].SetLength(oldNowIdx2)
	for i := oldNowIdx1 + 1; i < len(r.resultList); i++ {
		r.resultList[i].SetLength(0)
	}

	if r.optInformation.doesThisNeedEmptyList {
		r.emptyList[oldNowIdx1].SetLength(oldNowIdx2)

		for i := oldNowIdx1 + 1; i < len(r.resultList); i++ {
			r.emptyList[i].SetLength(0)
		}
	}
	r.nowIdx1 = oldNowIdx1
	return nil
}

// resExtend obtains memory of length more from the current position for use,
// while also altering the memory usage indicators and other structure related.
func (r *optSplitResult) resExtend(more int) (startX, startY, endX, endY int, err error) {
	startX = r.nowIdx1
	startY = r.resultList[startX].Length()

	if err = r.extendResultPurely(more); err != nil {
		return -1, -1, -1, -1, err
	}

	endX = r.nowIdx1
	endY = r.resultList[endX].Length()

	if r.optInformation.doesThisNeedEmptyList {
		r.bsFromEmptyList[startX] = vector.MustFixedColNoTypeCheck[bool](r.emptyList[startX])
		for i := startX + 1; i <= r.nowIdx1; i++ {
			r.bsFromEmptyList = append(r.bsFromEmptyList, vector.MustFixedColNoTypeCheck[bool](r.emptyList[i]))
		}
		setValueFromX1Y1ToX2Y2(r.bsFromEmptyList, startX, startY, endX, endY, true)
	}
	return startX, startY, endX, endY, nil
}

func (r *optSplitResult) free() {
	if r.mp == nil {
		return
	}

	for _, v := range r.resultList {
		if v == nil {
			continue
		}
		v.Free(r.mp)
	}
	for _, v := range r.emptyList {
		if v == nil {
			continue
		}
		v.Free(r.mp)
	}
	r.resultList = nil
	r.emptyList = nil
}

func setValueFromX1Y1ToX2Y2[T types.FixedSizeTExceptStrType](
	src [][]T, x1, y1 int, x2, y2 int, value T) {

	if x1 == x2 {
		for y1 < y2 {
			src[x1][y1] = value
			y1++
		}
		return
	}

	lengthLimitation := len(src[x1])
	for i := y1; i < lengthLimitation; i++ {
		src[x1][i] = value
	}
	for x := x1 + 1; x < x2; x++ {
		for i := range src[x] {
			src[x][i] = value
		}
	}
	for i := 0; i < y2; i++ {
		src[x2][i] = value
	}
	return
}
