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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "shuffle"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": shuffle")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	if arg.RuntimeFilterSpec != nil {
		arg.RuntimeFilterSpec.Handled = false
	}
	arg.initShuffle()
	return nil
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
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	if arg.ctr.lastSentBatch != nil {
		proc.PutBatch(arg.ctr.lastSentBatch)
		arg.ctr.lastSentBatch = nil
	}

SENDLAST:
	if arg.ctr.ending {
		result := vm.NewCallResult()
		//send shuffle pool
		for i, bat := range arg.ctr.shufflePool {
			if bat != nil {
				//need to wait for runtimefilter_pass before send batch
				if err := arg.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				result.Batch = bat
				arg.ctr.lastSentBatch = result.Batch
				arg.ctr.shufflePool[i] = nil
				return result, nil
			}
		}
		//end
		result.Status = vm.ExecStop
		return result, nil
	}

	for len(arg.ctr.sendPool) == 0 {
		// do input
		result, err := vm.ChildrenCall(arg.GetChildren(0), proc, anal)
		if err != nil {
			return result, err
		}
		bat := result.Batch
		if bat == nil {
			arg.ctr.ending = true
			goto SENDLAST
		} else if !bat.IsEmpty() {
			if arg.ShuffleType == int32(plan.ShuffleType_Hash) {
				bat, err = hashShuffle(arg, bat, proc)
			} else if arg.ShuffleType == int32(plan.ShuffleType_Range) {
				bat, err = rangeShuffle(arg, bat, proc)
			}
			if err != nil {
				return result, err
			}
			if bat != nil {
				// can directly send this batch
				//need to wait for runtimefilter_pass before send batch
				if err := arg.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				return result, nil
			}
		}
	}
	//need to wait for runtimefilter_pass before send batch
	if err := arg.handleRuntimeFilter(proc); err != nil {
		return vm.CancelResult, err
	}

	// send batch in send pool
	result := vm.NewCallResult()
	length := len(arg.ctr.sendPool)
	result.Batch = arg.ctr.sendPool[length-1]
	arg.ctr.lastSentBatch = result.Batch
	arg.ctr.sendPool = arg.ctr.sendPool[:length-1]
	return result, nil
}

func (arg *Argument) handleRuntimeFilter(proc *process.Process) error {
	if arg.RuntimeFilterSpec != nil && !arg.RuntimeFilterSpec.Handled {
		msgReceiver := proc.NewMessageReceiver([]int32{arg.RuntimeFilterSpec.Tag}, process.AddrBroadCastOnCurrentCN())
		msgs, ctxDone := msgReceiver.ReceiveMessage(true, proc.Ctx)
		if ctxDone {
			return proc.Ctx.Err()
		}
		for i := range msgs {
			msg, ok := msgs[i].(process.RuntimeFilterMessage)
			if !ok {
				panic("expect runtime filter message, receive unknown message!")
			}
			switch msg.Typ {
			case process.RuntimeFilter_PASS, process.RuntimeFilter_DROP:
				arg.RuntimeFilterSpec.Handled = true
				continue
			default:
				panic("unsupported runtime filter type!")
			}
		}
	}
	return nil
}

func (arg *Argument) initShuffle() {
	if arg.ctr.sels == nil {
		arg.ctr.sels = make([][]int32, arg.AliveRegCnt)
		for i := 0; i < int(arg.AliveRegCnt); i++ {
			arg.ctr.sels[i] = make([]int32, 0, colexec.DefaultBatchSize/arg.AliveRegCnt*2)
		}
		arg.ctr.shufflePool = make([]*batch.Batch, arg.AliveRegCnt)
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
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		return plan2.SimpleInt64HashToRange(groupByCol[0], lenRegs)
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
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
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
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
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

func hashShuffle(ap *Argument, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		bat.ShuffleIDX = 0
		return bat, nil
	}
	if groupByVec.IsConst() {
		bat.ShuffleIDX = int32(shuffleConstVectorByHash(ap, bat))
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
			bat.ShuffleIDX = int32(i)
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

	var firstValueSigned, lastValueSigned int64
	var firstValueUnsigned, lastValueUnsigned uint64
	var signed bool
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		firstValueUnsigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_int64:
		signed = true
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		firstValueSigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_int32:
		signed = true
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_int16:
		signed = true
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		firstValueUnsigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		firstValueUnsigned = uint64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = uint64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
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

func getShuffledSelsByRangeWithoutNull(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		if ap.ShuffleRangeInt64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		if ap.ShuffleRangeUint64 != nil {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), lenRegs)
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
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
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
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
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
	case types.T_bit:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
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
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), lenRegs)
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
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
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
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, lenRegs)
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

func putBatchIntoShuffledPoolsBySels(ap *Argument, srcBatch *batch.Batch, sels [][]int32, proc *process.Process) error {
	shuffledPool := ap.ctr.shufflePool
	var err error
	for regIndex := range shuffledPool {
		newSels := sels[regIndex]
		for len(newSels) > 0 {
			bat := shuffledPool[regIndex]
			if bat == nil {
				bat, err = proc.NewBatchFromSrc(srcBatch, colexec.DefaultBatchSize)
				if err != nil {
					return err
				}
				bat.ShuffleIDX = int32(regIndex)
				ap.ctr.shufflePool[regIndex] = bat
			}
			length := len(newSels)
			if length+bat.RowCount() > colexec.DefaultBatchSize {
				length = colexec.DefaultBatchSize - bat.RowCount()
			}
			for vecIndex := range bat.Vecs {
				v := bat.Vecs[vecIndex]
				v.SetSorted(false)
				err = v.Union(srcBatch.Vecs[vecIndex], newSels[:length], proc.Mp())
				if err != nil {
					return err
				}
			}
			bat.AddRowCount(length)
			newSels = newSels[length:]
			if bat.RowCount() == colexec.DefaultBatchSize {
				ap.ctr.sendPool = append(ap.ctr.sendPool, bat)
				shuffledPool[regIndex] = nil
			}
		}
	}
	return nil
}

func rangeShuffle(ap *Argument, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() || groupByVec.IsConst() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			bat.ShuffleIDX = int32(regIndex)
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
			bat.ShuffleIDX = int32(i)
			return bat, nil
		}
	}
	err := putBatchIntoShuffledPoolsBySels(ap, bat, sels, proc)
	return nil, err
}
