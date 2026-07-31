// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func PrefixEq(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if parameters[1].AllNull() {
		result.GetResultVector().GetNulls().AddRange(0, uint64(length))
		return nil
	}

	lvec := parameters[0]
	rval := parameters[1].GetBytesAt(0)
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	lcol, larea := vector.MustVarlenaRawData(lvec)

	if lvec.HasNull() {
		lNulls := lvec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := uint64(0); i < uint64(length); i++ {
			if lNulls.Contains(i) {
				res[i] = false
				rNulls.Add(i)
			} else {
				res[i] = bytes.HasPrefix(lcol[i].GetByteSlice(larea), rval)
			}
		}
	} else if lvec.GetSorted() {
		lowerBound := sort.Search(len(lcol), func(i int) bool {
			return bytes.Compare(rval, lcol[i].GetByteSlice(larea)) <= 0
		})

		upperBound := lowerBound
		for upperBound < len(lcol) && bytes.HasPrefix(lcol[upperBound].GetByteSlice(larea), rval) {
			upperBound++
		}

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := range length {
			res[i] = bytes.HasPrefix(lcol[i].GetByteSlice(larea), rval)
		}
	}

	return nil
}

func PrefixBetween(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBetweenBytesWithFunc(parameters, result, proc, length, selectList, types.PrefixCompare)
}

func PrefixInRange(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return inRangeBytesWithFunc(parameters, result, proc, length, selectList, types.PrefixCompare)
}

type implPrefixIn struct {
	ready        bool
	vals         [][]byte
	scratch      []byte
	scratchCount int
}

func newImplPrefixIn() *implPrefixIn {
	return &implPrefixIn{ready: false}
}

func (op *implPrefixIn) init(rvec *vector.Vector, mp *mpool.MPool) error {
	op.vals = make([][]byte, rvec.Length())
	vlen := 0

	var tmpVec *vector.Vector
	var err error
	if !rvec.GetSorted() {
		tmpVec, err = rvec.Dup(mp)
		if err != nil {
			return err
		}
		tmpVec.InplaceSortAndCompact()
		rvec = tmpVec
	}
	defer func() {
		if tmpVec != nil {
			tmpVec.Free(mp)
		}
	}()

	rcol, rarea := vector.MustVarlenaRawData(rvec)
	for i := 0; i < rvec.Length(); i++ {
		var rval []byte
		rval = append(rval, rcol[i].GetByteSlice(rarea)...)
		if vlen == 0 || !bytes.HasPrefix(rval, op.vals[vlen-1]) {
			op.vals[vlen] = rval
			vlen++
		}
	}
	op.vals = op.vals[:vlen]
	op.ready = true
	return nil
}

const prefixScratchEntrySize = 8

type prefixScratchEntries struct {
	data  []byte
	count int
}

func (e prefixScratchEntries) Len() int {
	return e.count
}

func (e prefixScratchEntries) Less(left, right int) bool {
	return bytes.Compare(e.value(left), e.value(right)) < 0
}

func (e prefixScratchEntries) Swap(left, right int) {
	leftEntry := e.data[left*prefixScratchEntrySize : (left+1)*prefixScratchEntrySize]
	rightEntry := e.data[right*prefixScratchEntrySize : (right+1)*prefixScratchEntrySize]
	var saved [prefixScratchEntrySize]byte
	copy(saved[:], leftEntry)
	copy(leftEntry, rightEntry)
	copy(rightEntry, saved[:])
}

func (e prefixScratchEntries) value(index int) []byte {
	entry := e.data[index*prefixScratchEntrySize:]
	offset := binary.LittleEndian.Uint32(entry)
	length := binary.LittleEndian.Uint32(entry[4:])
	return e.data[int(offset):int(offset+length)]
}

func (op *implPrefixIn) initAccounted(
	rvec *vector.Vector,
	result vector.FunctionResultWrapper,
) error {
	rowCount := rvec.Length()
	if rowCount < 0 || rowCount > math.MaxInt/prefixScratchEntrySize {
		return mpool.ErrAllocationAccountInvalid
	}
	total := rowCount * prefixScratchEntrySize
	for row := 0; row < rowCount; row++ {
		valueSize := len(rvec.GetBytesAt(row))
		if valueSize > math.MaxInt-total {
			return mpool.ErrAllocationAccountInvalid
		}
		total += valueSize
	}
	if uint64(total) > math.MaxUint32 {
		return mpool.ErrAllocationAccountInvalid
	}
	scratch, selected, err := result.ResizeFunctionScratch(total)
	if err != nil {
		return err
	}
	if !selected {
		return mpool.ErrAllocationAccountInvalid
	}
	entries := prefixScratchEntries{data: scratch, count: rowCount}
	payloadOffset := rowCount * prefixScratchEntrySize
	for row := 0; row < rowCount; row++ {
		value := rvec.GetBytesAt(row)
		entry := scratch[row*prefixScratchEntrySize:]
		binary.LittleEndian.PutUint32(entry, uint32(payloadOffset))
		binary.LittleEndian.PutUint32(entry[4:], uint32(len(value)))
		payloadOffset += copy(scratch[payloadOffset:], value)
	}
	if !rvec.GetSorted() {
		sort.Sort(entries)
	}
	compactCount := 0
	for row := 0; row < rowCount; row++ {
		value := entries.value(row)
		if compactCount != 0 && bytes.HasPrefix(value, entries.value(compactCount-1)) {
			continue
		}
		if compactCount != row {
			copy(
				scratch[compactCount*prefixScratchEntrySize:],
				scratch[row*prefixScratchEntrySize:(row+1)*prefixScratchEntrySize],
			)
		}
		compactCount++
	}
	op.scratch = scratch
	op.scratchCount = compactCount
	op.ready = true
	return nil
}

func (op *implPrefixIn) valueCount() int {
	if op.scratch != nil {
		return op.scratchCount
	}
	return len(op.vals)
}

func (op *implPrefixIn) valueAt(index int) []byte {
	if op.scratch != nil {
		return (prefixScratchEntries{
			data:  op.scratch,
			count: op.scratchCount,
		}).value(index)
	}
	return op.vals[index]
}

func (op *implPrefixIn) doPrefixIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		var err error
		if result.HasFunctionScratch() {
			err = op.initAccounted(parameters[1], result)
		} else {
			err = op.init(parameters[1], proc.Mp())
		}
		if err != nil {
			return err
		}
	}

	lvec := parameters[0]
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())
	if op.valueCount() == 0 {
		for i := range length {
			res[i] = false
		}
		return nil
	}

	lcol, larea := vector.MustVarlenaRawData(lvec)
	lvecHasNull := lvec.HasNull()

	if lvec.GetSorted() && !lvecHasNull {
		rval := op.valueAt(0)
		rpos := 0
		rlen := op.valueCount()

		for i := range length {
			lval := lcol[i].GetByteSlice(larea)
			for types.PrefixCompare(lval, rval) > 0 {
				rpos++
				if rpos == rlen {
					for j := i; j < length; j++ {
						res[j] = false
					}
					return nil
				}

				rval = op.valueAt(rpos)
			}

			res[i] = bytes.HasPrefix(lval, rval)
		}
	} else {
		if lvecHasNull {
			lNulls := lvec.GetNulls()
			rNulls := result.GetResultVector().GetNulls()
			for i := uint64(0); i < uint64(length); i++ {
				if lNulls.Contains(i) {
					res[i] = false
					rNulls.Add(i)
				} else {
					lval := lcol[i].GetByteSlice(larea)
					rpos, _ := sort.Find(op.valueCount(), func(j int) int {
						return types.PrefixCompare(lval, op.valueAt(j))
					})

					res[i] = rpos < op.valueCount() && bytes.HasPrefix(lval, op.valueAt(rpos))
				}
			}
		} else {
			for i := range length {
				lval := lcol[i].GetByteSlice(larea)
				rpos, _ := sort.Find(op.valueCount(), func(j int) int {
					return types.PrefixCompare(lval, op.valueAt(j))
				})

				res[i] = rpos < op.valueCount() && bytes.HasPrefix(lval, op.valueAt(rpos))
			}
		}
	}

	return nil
}
