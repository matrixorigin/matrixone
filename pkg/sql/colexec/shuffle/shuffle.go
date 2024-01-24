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

package shuffle

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString("shuffle")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ctr := new(container)
	ap.ctr = ctr
	ap.initShuffle()
	return nil
}

func findBatchToSend(ap *Argument, threshHold int) int {
	for i := range ap.ctr.shuffledBats {
		if ap.ctr.shuffledBats[i] != nil && ap.ctr.shuffledBats[i].RowCount() > threshHold {
			return i
		}
	}
	return -1
}

// there are two ways for shuffle to send a batch
// if a batch belongs to one bucket, send this batch directly, and shuffle need to do nothing
// else split this batch into pieces, write data into pool. if one bucket is full, send this bucket.
// next time, set this bucket rowcount to 0 and reuse it
// for now, we shuffle null to the first bucket
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ap := arg
	var index int

	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	// clean last sent batch
	if ap.ctr.lastSentIdx != -1 {
		// todo: reuse this batch, need to fix this
		//	ap.ctr.shuffledBats[ap.ctr.lastSentIdx].SetRowCount(0)
		ap.ctr.shuffledBats[ap.ctr.lastSentIdx] = nil
		ap.ctr.lastSentIdx = -1
	}

	//find output
	if ap.ctr.ending {
		index = findBatchToSend(ap, 0)
		if index == -1 {
			result := vm.NewCallResult()
			result.Status = vm.ExecStop
			return result, nil
		}
	} else {
		index = findBatchToSend(ap, shuffleBatchSize)
	}

	for index == -1 {
		// do input

		result, err := vm.ChildrenCall(arg.children[0], proc, anal)

		if err != nil {
			return result, err
		}
		bat := result.Batch

		if bat == nil {
			ap.ctr.ending = true
		} else if !bat.IsEmpty() {
			if ap.ShuffleType == int32(plan.ShuffleType_Hash) {
				bat, err = hashShuffle(ap, bat, proc)
			} else if ap.ShuffleType == int32(plan.ShuffleType_Range) {
				bat, err = rangeShuffle(ap, bat, proc)
			}
			if err != nil {
				return result, err
			}
			if bat != nil {
				// can directly send this batch
				return result, nil
			}
		}

		// find output again
		if ap.ctr.ending {
			index = findBatchToSend(ap, 0)
			if index == -1 {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
		} else {
			index = findBatchToSend(ap, shuffleBatchSize)
		}
	}

	// do output
	result := vm.NewCallResult()
	result.Batch = ap.ctr.shuffledBats[index]
	ap.ctr.lastSentIdx = index
	return result, nil
}

func (arg *Argument) initShuffle() {
	arg.ctr.lastSentIdx = -1
	if arg.ctr.sels == nil {
		arg.ctr.sels = make([][]int32, arg.AliveRegCnt)
		for i := 0; i < int(arg.AliveRegCnt); i++ {
			arg.ctr.sels[i] = make([]int32, shuffleBatchSize/arg.AliveRegCnt*2)
		}
		arg.ctr.shuffledBats = make([]*batch.Batch, arg.AliveRegCnt)
	}
}

func (arg *Argument) getSels() [][]int32 {
	for i := range arg.ctr.sels {
		arg.ctr.sels[i] = arg.ctr.sels[i][:0]
	}
	return arg.ctr.sels
}

func shuffleConstVectorByHash(ap *Argument, bat *batch.Batch) uint64 {
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		return plan2.SimpleCharHashToRange(groupByCol[0].GetByteSlice(area), lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
}

func getShuffledSelsByHashWithNull(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleCharHashToRange(v.GetByteSlice(area), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByHashWithoutNull(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleCharHashToRange(v.GetByteSlice(area), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func initShuffledBats(ap *Argument, bat *batch.Batch, proc *process.Process, regIndex int) error {
	lenVecs := len(bat.Vecs)
	shuffledBats := ap.ctr.shuffledBats

	shuffledBats[regIndex] = batch.NewWithSize(lenVecs)
	shuffledBats[regIndex].ShuffleIDX = regIndex
	for j := range shuffledBats[regIndex].Vecs {
		v := proc.GetVector(*bat.Vecs[j].GetType())
		if v.Capacity() < shuffleBatchSize {
			err := v.PreExtend(shuffleBatchSize, proc.Mp())
			if err != nil {
				v.Free(proc.Mp())
				return err
			}
		}
		shuffledBats[regIndex].Vecs[j] = v
	}
	return nil
}

func hashShuffle(ap *Argument, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		bat.ShuffleIDX = 0
		return bat, nil
	}
	if groupByVec.IsConst() {
		bat.ShuffleIDX = int(shuffleConstVectorByHash(ap, bat))
		return bat, nil
	}

	var sels [][]int32
	if groupByVec.HasNull() {
		sels = getShuffledSelsByHashWithNull(ap, bat)
	} else {
		sels = getShuffledSelsByHashWithoutNull(ap, bat)
	}
	for i := range sels {
		if len(sels[i]) > 0 && len(sels[i]) != bat.RowCount() {
			break
		}
		if len(sels[i]) == bat.RowCount() {
			bat.ShuffleIDX = i
			return bat, nil
		}
	}

	return nil, putBatchIntoShuffledPoolsBySels(ap, bat, sels, proc)
}

func allBatchInOneRange(ap *Argument, bat *batch.Batch) (bool, uint64) {
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		return true, 0
	}
	if groupByVec.HasNull() {
		return false, 0
	}

	var regIndexFirst, regIndexLast uint64
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, vfirst, lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, vlast, lenRegs)

	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vfirst), lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vlast), lenRegs)
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vfirst), lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vlast), lenRegs)
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vfirst, lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vlast, lenRegs)
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vfirst), lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vlast), lenRegs)
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		vfirst := groupByCol[0]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vfirst), lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vlast), lenRegs)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		vfirst := plan2.VarlenaToUint64(&groupByCol[0], area)
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vfirst, lenRegs)
		if groupByVec.IsConst() {
			return true, regIndexFirst
		}
		vlast := plan2.VarlenaToUint64(&groupByCol[groupByVec.Length()-1], area)
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vlast, lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	if regIndexFirst == regIndexLast {
		return true, regIndexFirst
	} else {
		return false, 0
	}
}

func getShuffledSelsByRangeWithoutNull(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			for row := range groupByCol {
				v := plan2.VarlenaToUint64Inline(&groupByCol[row])
				regIndex := plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row := range groupByCol {
				v := plan2.VarlenaToUint64(&groupByCol[row], area)
				regIndex := plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByRangeWithNull(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			for row := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func putBatchIntoShuffledPoolsBySels(ap *Argument, bat *batch.Batch, sels [][]int32, proc *process.Process) error {
	shuffledBats := ap.ctr.shuffledBats
	for regIndex := range shuffledBats {
		lenSels := len(sels[regIndex])
		if lenSels > 0 {
			b := shuffledBats[regIndex]
			if b == nil {
				err := initShuffledBats(ap, bat, proc, regIndex)
				if err != nil {
					return err
				}
				b = shuffledBats[regIndex]
			}
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				v.SetSorted(false)
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return err
				}
			}
			b.AddRowCount(lenSels)
		}
	}
	return nil
}

func rangeShuffle(ap *Argument, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() || groupByVec.IsConst() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			bat.ShuffleIDX = int(regIndex)
			return bat, nil
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
		if len(sels[i]) == bat.RowCount() {
			bat.ShuffleIDX = i
			return bat, nil
		}
	}
	err := putBatchIntoShuffledPoolsBySels(ap, bat, sels, proc)
	return nil, err
}
