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

package shuffleV2

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "shuffleV2"

func (shuffle *ShuffleV2) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (shuffle *ShuffleV2) OpType() vm.OpType {
	return vm.ShuffleV2
}

func (shuffle *ShuffleV2) Prepare(proc *process.Process) error {
	if shuffle.OpAnalyzer == nil {
		shuffle.OpAnalyzer = process.NewAnalyzer(shuffle.GetIdx(), shuffle.IsFirst, shuffle.IsLast, opName)
	} else {
		shuffle.OpAnalyzer.Reset()
	}

	if shuffle.ctr.sels == nil {
		shuffle.ctr.sels = make([][]int32, shuffle.BucketNum)
	}
	if shuffle.GetShufflePool() == nil {
		shuffle.SetShufflePool(NewShufflePool(shuffle.BucketNum, shuffle.BucketNum))
	}
	shuffle.ctr.shufflePool.hold()
	shuffle.ctr.ending = false
	return nil
}

func (shuffle *ShuffleV2) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := shuffle.OpAnalyzer

	result := vm.NewCallResult()
SENDLAST:
	if shuffle.ctr.ending { //send last batch in shuffle pool
		result.Batch = shuffle.ctr.shufflePool.getEndingBatch(shuffle.ctr.buf, shuffle.CurrentShuffleIdx, proc, shuffle.IsDebug)
		shuffle.ctr.buf = result.Batch
		return result, nil
	}

	var err error
	for {
		tmpBat := shuffle.ctr.shufflePool.getFullBatch(shuffle.ctr.buf, shuffle.CurrentShuffleIdx)
		if tmpBat != nil && tmpBat.RowCount() > 0 { // find a full batch
			shuffle.ctr.buf = tmpBat
			break
		}
		// do input
		result, err = vm.ChildrenCall(shuffle.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		bat := result.Batch
		if bat == nil {
			shuffle.ctr.ending = true
			shuffle.ctr.shufflePool.stopWriting()
			goto SENDLAST
		} else if !bat.IsEmpty() {
			if shuffle.ShuffleType == int32(plan.ShuffleType_Hash) {
				bat, err = hashShuffle(shuffle, bat, proc)
			} else if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
				bat, err = rangeShuffle(shuffle, bat, proc)
			}
			if err != nil {
				return result, err
			}
			if bat != nil {
				// can directly send this batch
				shuffle.ctr.shufflePool.statsDirectlySentBatch(bat)
				return result, nil
			}
		}
	}
	// send the batch
	result.Batch = shuffle.ctr.buf
	return result, nil
}

func (shuffle *ShuffleV2) clearSels() [][]int32 {
	for i := range shuffle.ctr.sels {
		if len(shuffle.ctr.sels[i]) > 0 {
			shuffle.ctr.sels[i] = shuffle.ctr.sels[i][:0]
		}
	}
	return shuffle.ctr.sels
}

func shuffleConstVectorByHash(ap *ShuffleV2, bat *batch.Batch) uint64 {
	lenRegs := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		return plan2.SimpleCharHashToRange(groupByCol[0].GetByteSlice(area), lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
}

func getShuffledSelsByHashWithNull(ap *ShuffleV2, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	lenRegs := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleCharHashToRange(groupByCol[row].GetByteSlice(area), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByHashWithoutNull(ap *ShuffleV2, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_decimal64:
		groupByCol := vector.MustFixedColNoTypeCheck[types.Decimal64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_decimal128:
		groupByCol := vector.MustFixedColNoTypeCheck[types.Decimal128](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v.B0_63^v.B64_127), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row := range groupByCol {
			regIndex := plan2.SimpleCharHashToRange(groupByCol[row].GetByteSlice(area), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	default:
		panic(fmt.Sprintf("unsupported shuffle type %v, wrong plan!", groupByVec.GetType())) //something got wrong here!
	}
	return sels
}

func hashShuffle(ap *ShuffleV2, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		err := ap.ctr.shufflePool.putAllBatchIntoPoolByShuffleIdx(bat, proc, 0)
		return nil, err
	}
	if groupByVec.IsConst() {
		bat.ShuffleIDX = int32(shuffleConstVectorByHash(ap, bat))
		err := ap.ctr.shufflePool.putAllBatchIntoPoolByShuffleIdx(bat, proc, bat.ShuffleIDX)
		return nil, err
	}

	var sels [][]int32
	if groupByVec.HasNull() {
		sels = getShuffledSelsByHashWithNull(ap, bat)
	} else {
		sels = getShuffledSelsByHashWithoutNull(ap, bat)
	}

	err := ap.ctr.shufflePool.putBatchIntoShuffledPoolsBySels(bat, sels, proc)
	return nil, err
}

func allBatchInOneRange(ap *ShuffleV2, bat *batch.Batch) (bool, uint64) {
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		return true, 0
	}
	if groupByVec.HasNull() {
		return false, 0
	}

	var firstValueSigned, lastValueSigned int64
	var firstValueUnsigned, lastValueUnsigned uint64
	var signed bool
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		firstValueSigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_int32:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_int16:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		firstValueUnsigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		firstValueUnsigned = uint64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = uint64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		firstValueUnsigned = uint64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = uint64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		firstValueUnsigned = plan2.VarlenaToUint64(&groupByCol[0], area)
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = plan2.VarlenaToUint64(&groupByCol[groupByVec.Length()-1], area)
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}

	var regIndexFirst, regIndexLast uint64
	if ap.ShuffleRangeInt64 != nil {
		regIndexFirst = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, firstValueSigned)
		regIndexLast = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, lastValueSigned)
	} else if ap.ShuffleRangeUint64 != nil {
		regIndexFirst = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, firstValueUnsigned)
		regIndexLast = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, lastValueUnsigned)
	} else if signed {
		regIndexFirst = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, firstValueSigned, bucketNum)
		regIndexLast = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, lastValueSigned, bucketNum)
	} else {
		regIndexFirst = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), firstValueUnsigned, bucketNum)
		regIndexLast = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), lastValueUnsigned, bucketNum)
	}

	if regIndexFirst == regIndexLast {
		return true, regIndexFirst
	} else {
		return false, 0
	}
}

func getShuffledSelsByRangeWithoutNull(ap *ShuffleV2, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		} else {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByRangeWithNull(ap *ShuffleV2, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		} else {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func rangeShuffle(ap *ShuffleV2, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() || groupByVec.IsConst() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			bat.ShuffleIDX = int32(regIndex)
			if bat.ShuffleIDX == ap.CurrentShuffleIdx {
				return bat, nil
			}
			err := ap.ctr.shufflePool.putAllBatchIntoPoolByShuffleIdx(bat, proc, bat.ShuffleIDX)
			return nil, err
		}
	}
	var sels [][]int32
	if groupByVec.HasNull() {
		sels = getShuffledSelsByRangeWithNull(ap, bat)
	} else {
		sels = getShuffledSelsByRangeWithoutNull(ap, bat)
	}
	for i := range sels {
		if len(sels[i]) > 0 && len(sels[i]) != bat.RowCount() {
			break
		}
		if len(sels[i]) == bat.RowCount() { // all batch in one range
			bat.ShuffleIDX = int32(i)
			if bat.ShuffleIDX == ap.CurrentShuffleIdx {
				return bat, nil
			}
			err := ap.ctr.shufflePool.putAllBatchIntoPoolByShuffleIdx(bat, proc, bat.ShuffleIDX)
			return nil, err
		}
	}
	err := ap.ctr.shufflePool.putBatchIntoShuffledPoolsBySels(bat, sels, proc)
	return nil, err
}
