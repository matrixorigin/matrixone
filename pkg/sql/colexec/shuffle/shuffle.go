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

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "shuffle"

func (shuffle *Shuffle) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": shuffle")
}

func (shuffle *Shuffle) OpType() vm.OpType {
	return vm.Shuffle
}

func (shuffle *Shuffle) Prepare(proc *process.Process) error {
	if shuffle.RuntimeFilterSpec != nil {
		shuffle.ctr.runtimeFilterHandled = false
	}
	if shuffle.ctr.sels == nil {
		shuffle.ctr.sels = make([][]int64, shuffle.AliveRegCnt)
		for i := 0; i < int(shuffle.AliveRegCnt); i++ {
			shuffle.ctr.sels[i] = make([]int64, 0, colexec.DefaultBatchSize/shuffle.AliveRegCnt*2)
		}
	}
	shuffle.SetShufflePool(NewShufflePool(shuffle.AliveRegCnt))
	shuffle.ctr.shufflePool.Init()
	shuffle.ctr.ending = false
	return nil
}

// there are two ways for shuffle to send a batch
// if a batch belongs to one bucket, send this batch directly, and shuffle need to do nothing
// else split this batch into pieces, write data into pool. if one bucket is full, send this bucket.
// next time, set this bucket rowcount to 0 and reuse it
// for now, we shuffle null to the first bucket
func (shuffle *Shuffle) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}
	anal := proc.GetAnalyze(shuffle.GetIdx(), shuffle.GetParallelIdx(), shuffle.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	result := vm.NewCallResult()
SENDLAST:
	if shuffle.ctr.ending {
		//send shuffle pool
		result.Batch = shuffle.ctr.shufflePool.GetEndingBatch()
		//need to wait for runtimefilter_pass before send batch
		if err := shuffle.handleRuntimeFilter(proc); err != nil {
			return vm.CancelResult, err
		}
		if result.Batch == nil {
			result.Status = vm.ExecStop
		}
		return result, nil
	}

	var err error
	for {
		shuffle.ctr.buf, err = shuffle.ctr.shufflePool.GetFullBatch(shuffle.ctr.buf, proc)
		if err != nil {
			return result, err
		}
		if shuffle.ctr.buf != nil && shuffle.ctr.buf.RowCount() > 0 { // find a full batch
			break
		}
		// do input
		result, err = vm.ChildrenCall(shuffle.GetChildren(0), proc, anal)
		if err != nil {
			return result, err
		}
		bat := result.Batch
		if bat == nil {
			shuffle.ctr.ending = true
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
				//need to wait for runtimefilter_pass before send batch
				if err = shuffle.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				return result, nil
			}
		}
	}
	//need to wait for runtimefilter_pass before send batch
	if err = shuffle.handleRuntimeFilter(proc); err != nil {
		return vm.CancelResult, err
	}

	// send the batch
	result.Batch = shuffle.ctr.buf
	return result, nil
}

func (shuffle *Shuffle) handleRuntimeFilter(proc *process.Process) error {
	if !shuffle.ctr.runtimeFilterHandled && shuffle.RuntimeFilterSpec != nil {
		shuffle.msgReceiver = message.NewMessageReceiver(
			[]int32{shuffle.RuntimeFilterSpec.Tag},
			message.AddrBroadCastOnCurrentCN(),
			proc.GetMessageBoard())
		msgs, ctxDone, err := shuffle.msgReceiver.ReceiveMessage(true, proc.Ctx)
		if ctxDone {
			shuffle.ctr.runtimeFilterHandled = true
			return nil
		}
		if err != nil {
			return err
		} else {
			for i := range msgs {
				msg, _ := msgs[i].(message.RuntimeFilterMessage)
				switch msg.Typ {
				case message.RuntimeFilter_PASS, message.RuntimeFilter_DROP:
					shuffle.ctr.runtimeFilterHandled = true
					continue
				}
			}
		}
	}
	return nil
}

func (shuffle *Shuffle) getSels() [][]int64 {
	for i := range shuffle.ctr.sels {
		shuffle.ctr.sels[i] = shuffle.ctr.sels[i][:0]
	}
	return shuffle.ctr.sels
}

func shuffleConstVectorByHash(ap *Shuffle, bat *batch.Batch) uint64 {
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		return plan2.SimpleInt64HashToRange(groupByCol[0], lenRegs)
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

func getShuffledSelsByHashWithNull(ap *Shuffle, bat *batch.Batch) [][]int64 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleCharHashToRange(v.GetByteSlice(area), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByHashWithoutNull(ap *Shuffle, bat *batch.Batch) [][]int64 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleCharHashToRange(v.GetByteSlice(area), lenRegs)
			sels[regIndex] = append(sels[regIndex], int64(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func hashShuffle(ap *Shuffle, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		bat.ShuffleIDX = 0
		return bat, nil
	}
	if groupByVec.IsConst() {
		bat.ShuffleIDX = int32(shuffleConstVectorByHash(ap, bat))
		return bat, nil
	}

	var sels [][]int64
	if groupByVec.HasNull() {
		sels = getShuffledSelsByHashWithNull(ap, bat)
	} else {
		sels = getShuffledSelsByHashWithoutNull(ap, bat)
	}

	err := ap.ctr.shufflePool.putBatchIntoShuffledPoolsBySels(bat, sels, proc)
	return nil, err
}

func allBatchInOneRange(ap *Shuffle, bat *batch.Batch) (bool, uint64) {
	lenRegs := uint64(ap.AliveRegCnt)
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
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		firstValueUnsigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = groupByCol[groupByVec.Length()-1]
		}
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
		regIndexFirst = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, firstValueSigned, lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, lastValueSigned, lenRegs)
	} else {
		regIndexFirst = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), firstValueUnsigned, lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), lastValueUnsigned, lenRegs)
	}

	if regIndexFirst == regIndexLast {
		return true, regIndexFirst
	} else {
		return false, 0
	}
}

func getShuffledSelsByRangeWithoutNull(ap *Shuffle, bat *batch.Batch) [][]int64 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			}
		} else {
			if ap.ShuffleRangeUint64 != nil {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByRangeWithNull(ap *Shuffle, bat *batch.Batch) [][]int64 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		}
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
				sels[regIndex] = append(sels[regIndex], int64(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int64(row))
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
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
					}
					sels[regIndex] = append(sels[regIndex], int64(row))
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
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
					}
					sels[regIndex] = append(sels[regIndex], int64(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func rangeShuffle(ap *Shuffle, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() || groupByVec.IsConst() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			bat.ShuffleIDX = int32(regIndex)
			return bat, nil
		}
	}
	var sels [][]int64
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
			bat.ShuffleIDX = int32(i)
			return bat, nil
		}
	}
	err := ap.ctr.shufflePool.putBatchIntoShuffledPoolsBySels(bat, sels, proc)
	return nil, err
}
