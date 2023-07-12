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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("shuffle")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ctr := new(container)
	ap.ctr = ctr
	ap.initShuffle()
	return nil
}

func combineShuffleBatches(ap *Argument, proc *process.Process) *batch.Batch {
	var lenVecs int
	shuffledBats := ap.ctr.shuffledBats
	var newBat *batch.Batch

	allEmpty := true
	for _, bat := range shuffledBats {
		if bat == nil || bat.Length() == 0 {
			continue
		}
		lenVecs = len(bat.Vecs)
		allEmpty = false
		if newBat == nil {
			newBat = batch.NewWithSize(lenVecs)
			newBat.ShuffleIDX = make([]int, 0, len(shuffledBats))
			newBat.Zs = proc.Mp().GetSels()
			for j := range bat.Vecs {
				newBat.Vecs[j] = proc.GetVector(*bat.Vecs[j].GetType())
			}
		}
	}
	if allEmpty {
		return nil
	}

	for _, bat := range shuffledBats {
		if bat != nil && bat.Length() != 0 {
			newBat.Append(context.TODO(), proc.Mp(), bat)
		}
		newBat.ShuffleIDX = append(newBat.ShuffleIDX, newBat.Length())
	}

	return newBat
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		proc.SetInputBatch(combineShuffleBatches(ap, proc))
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		sendOneBatch(ap, proc)
		return false, nil
	}
	if ap.ShuffleType == int32(plan.ShuffleType_Hash) {
		return hashShuffle(bat, ap, proc)
	} else if ap.ShuffleType == int32(plan.ShuffleType_Range) {
		return rangeShuffle(bat, ap, proc)
	} else {
		panic("unsupported shuffle type!")
	}
}

func (arg *Argument) initShuffle() {
	if arg.ctr.sels == nil {
		arg.ctr.sels = make([][]int32, arg.AliveRegCnt)
		for i := 0; i < int(arg.AliveRegCnt); i++ {
			arg.ctr.sels[i] = make([]int32, 8192)
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

func getShuffledSelsByHash(ap *Argument, bat *batch.Batch) [][]int32 {
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
		groupByCol := vector.MustFixedCol[types.Varlena](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleCharHashToRange(v.GetByteSlice(groupByVec.GetArea()), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func initShuffledBats(ap *Argument, bat *batch.Batch, proc *process.Process, regIndex int) {
	lenVecs := len(bat.Vecs)
	shuffledBats := ap.ctr.shuffledBats

	shuffledBats[regIndex] = batch.NewWithSize(lenVecs)
	shuffledBats[regIndex].ShuffleIDX = []int{regIndex}
	shuffledBats[regIndex].Zs = proc.Mp().GetSels()
	for j := range shuffledBats[regIndex].Vecs {
		shuffledBats[regIndex].Vecs[j] = proc.GetVector(*bat.Vecs[j].GetType())
	}
}

func genShuffledBatsByHash(ap *Argument, bat *batch.Batch, proc *process.Process) error {
	//release old bats
	defer proc.PutBatch(bat)
	shuffledBats := ap.ctr.shuffledBats
	sels := getShuffledSelsByHash(ap, bat)

	//generate new shuffled bats
	for regIndex := range shuffledBats {
		lenSels := len(sels[regIndex])
		if lenSels > 0 {
			b := shuffledBats[regIndex]
			if b == nil {
				initShuffledBats(ap, bat, proc, regIndex)
				b = shuffledBats[regIndex]
			}
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return err
				}
			}
			for i := 0; i < lenSels; i++ {
				b.Zs = append(b.Zs, bat.Zs[sels[regIndex][i]])
			}
		}
	}

	return nil
}

func findBatchToSend(ap *Argument) int {
	maxSize := 0
	maxIndex := -1
	for i := range ap.ctr.shuffledBats {
		if ap.ctr.shuffledBats[i] != nil && ap.ctr.shuffledBats[i].Length() > maxSize {
			maxSize = ap.ctr.shuffledBats[i].Length()
			maxIndex = i
		}
	}
	return maxIndex
}

func sendOneBatch(ap *Argument, proc *process.Process) {
	regIndex := findBatchToSend(ap)
	if regIndex >= 0 {
		proc.SetInputBatch(ap.ctr.shuffledBats[regIndex])
		ap.ctr.shuffledBats[regIndex] = nil
	} else {
		proc.SetInputBatch(batch.EmptyBatch)
	}
}

func hashShuffle(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	err := genShuffledBatsByHash(ap, bat, proc)
	if err != nil {
		return false, err
	}
	sendOneBatch(ap, proc)
	return false, nil
}

func allBatchInOneRange(ap *Argument, bat *batch.Batch) (bool, uint64) {
	lenRegs := uint64(ap.AliveRegCnt)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	var regIndexFirst, regIndexLast uint64
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, vfirst, lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, vlast, lenRegs)

	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vfirst), lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vlast), lenRegs)
	case types.T_int16:
		groupByCol := vector.MustFixedCol[int16](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vfirst), lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexSigned(ap.ShuffleColMin, ap.ShuffleColMax, int64(vlast), lenRegs)
	case types.T_uint64:
		groupByCol := vector.MustFixedCol[uint64](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vfirst, lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), vlast, lenRegs)
	case types.T_uint32:
		groupByCol := vector.MustFixedCol[uint32](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vfirst), lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vlast), lenRegs)
	case types.T_uint16:
		groupByCol := vector.MustFixedCol[uint16](groupByVec)
		vfirst := groupByCol[0]
		vlast := groupByCol[groupByVec.Length()-1]
		regIndexFirst = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vfirst), lenRegs)
		regIndexLast = plan2.GetRangeShuffleIndexUnsigned(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(vlast), lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	if regIndexFirst == regIndexLast {
		return true, regIndexFirst
	} else {
		return false, 0
	}
}

func getShuffledSelsByRange(ap *Argument, bat *batch.Batch) [][]int32 {
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
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func genShuffledBatsByRange(ap *Argument, bat *batch.Batch, proc *process.Process) error {
	//release old bats
	defer proc.PutBatch(bat)

	shuffledBats := ap.ctr.shuffledBats
	sels := getShuffledSelsByRange(ap, bat)

	//generate new shuffled bats
	for regIndex := range shuffledBats {
		lenSels := len(sels[regIndex])
		if lenSels > 0 {
			b := shuffledBats[regIndex]
			if b == nil {
				initShuffledBats(ap, bat, proc, regIndex)
				b = shuffledBats[regIndex]
			}
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return err
				}
			}
			for i := 0; i < lenSels; i++ {
				b.Zs = append(b.Zs, bat.Zs[sels[regIndex][i]])
			}
		}
	}

	return nil
}

func rangeShuffle(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			bat.ShuffleIDX = []int{int(regIndex)}
			proc.SetInputBatch(bat)
			return false, nil
		}
	}

	err := genShuffledBatsByRange(ap, bat, proc)
	if err != nil {
		return false, err
	}
	sendOneBatch(ap, proc)
	return false, nil
}
