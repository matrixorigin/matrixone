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

package dispatch

import (
	"context"
	"hash/crc32"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getShuffledSelsByHash(ap *Argument, bat *batch.Batch) [][]int32 {
	sels := ap.getSels()
	lenRegs := uint64(ap.ctr.aliveRegCnt)
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

func genShuffledBatsByHash(ap *Argument, bat *batch.Batch, proc *process.Process) error {
	//release old bats
	defer proc.PutBatch(bat)

	lenVecs := len(bat.Vecs)
	shuffledBats := ap.ctr.shuffledBats
	if ap.ctr.batsCount == 0 {
		//initialize shuffled bats
		for regIndex := range shuffledBats {
			shuffledBats[regIndex] = batch.NewWithSize(lenVecs)
			for j := range shuffledBats[regIndex].Vecs {
				shuffledBats[regIndex].Vecs[j] = proc.GetVector(*bat.Vecs[j].GetType())
			}
		}
	}

	sels := getShuffledSelsByHash(ap, bat)

	//generate new shuffled bats
	for regIndex := range shuffledBats {
		lenSels := len(sels[regIndex])
		if lenSels > 0 {
			b := shuffledBats[regIndex]
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return err
				}
			}
			bat.AddRowCount(lenSels)
		}
	}

	ap.ctr.batsCount++
	return nil
}

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	refCountAdd := int64(ap.ctr.localRegsCnt - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(ap.ctr.localRegsCnt))
	}

	for i, reg := range ap.LocalRegs {
		select {
		case <-proc.Ctx.Done():
			handleUnsent(proc, bat, refCountAdd, int64(i))
			logutil.Debugf("proc context done during dispatch to local")
			return true, nil
		case <-reg.Ctx.Done():
			if ap.IsSink {
				atomic.AddInt64(&bat.Cnt, -1)
				continue
			}
			handleUnsent(proc, bat, refCountAdd, int64(i))
			logutil.Warnf("the receiver's ctx done during dispatch to all local")
			return true, nil
		case reg.Ch <- bat:
		}
	}

	return false, nil
}

// common sender: send to all RemoteReceiver
func sendToAllRemoteFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.ctr.prepared {
		end, err := ap.waitRemoteRegsReady(proc)
		if err != nil {
			return false, err
		}
		if end {
			return true, nil
		}
	}

	{ // send to remote regs
		encodeData, errEncode := types.Encode(bat)
		if errEncode != nil {
			return false, errEncode
		}
		for _, r := range ap.ctr.remoteReceivers {
			if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
				return false, err
			}
		}
	}

	return false, nil
}

func sendBatToIndex(ap *Argument, proc *process.Process, bat *batch.Batch, regIndex uint64) (bool, error) {
	for _, r := range ap.ctr.remoteReceivers {
		batIndex := uint64(ap.ctr.remoteToIdx[r.uuid])
		if regIndex == batIndex {
			if bat != nil && bat.Length() != 0 {
				encodeData, errEncode := types.Encode(bat)
				if errEncode != nil {
					return false, errEncode
				}
				if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
					return false, err
				}
			}
		}
	}

	for i, reg := range ap.LocalRegs {
		batIndex := uint64(ap.ShuffleRegIdxLocal[i])
		if regIndex == batIndex {
			if bat != nil && bat.Length() != 0 {
				select {
				case <-reg.Ctx.Done():
					logutil.Warnf("the receiver's ctx done during shuffle dispatch to all local")
				case reg.Ch <- bat:
				}
			}
		}
	}
	return false, nil
}

func sendShuffledBats(ap *Argument, proc *process.Process) (bool, error) {
	if ap.ctr.batsCount == 0 {
		return false, nil
	}

	// send to remote regs
	for _, r := range ap.ctr.remoteReceivers {
		batIndex := ap.ctr.remoteToIdx[r.uuid]
		batToSend := ap.ctr.shuffledBats[batIndex]
		if batToSend != nil && batToSend.Length() != 0 {
			encodeData, errEncode := types.Encode(batToSend)
			if errEncode != nil {
				return false, errEncode
			}
			if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
				return false, err
			}
		}
	}

	//send to all local regs
	for i, reg := range ap.LocalRegs {
		batIndex := ap.ShuffleRegIdxLocal[i]
		batToSend := ap.ctr.shuffledBats[batIndex]
		if batToSend != nil && batToSend.Length() != 0 {
			select {
			case <-reg.Ctx.Done():
				logutil.Warnf("the receiver's ctx done during shuffle dispatch to all local")
			case reg.Ch <- batToSend:
			}
		}
	}

	ap.ctr.batsCount = 0
	return false, nil
}

// accumulate enough batch size
func needToSendShuffledBats(ap *Argument) bool {
	if ap.ctr.batsCount > 0 {
		maxSize := 0
		for i := range ap.ctr.shuffledBats {
			if ap.ctr.shuffledBats[i].Length() > maxSize {
				maxSize = ap.ctr.shuffledBats[i].Length()
			}
		}
		if maxSize > shuffleBatchSize {
			return true
		}
	}
	return false
}

func hashShuffle(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	err := genShuffledBatsByHash(ap, bat, proc)
	if err != nil {
		return false, err
	}

	if needToSendShuffledBats(ap) {
		return sendShuffledBats(ap, proc)
	}

	return false, nil
}

func allBatchInOneRange(ap *Argument, bat *batch.Batch) (bool, uint64) {
	lenRegs := uint64(ap.ctr.aliveRegCnt)
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
	lenRegs := uint64(ap.ctr.aliveRegCnt)
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

	lenVecs := len(bat.Vecs)
	shuffledBats := ap.ctr.shuffledBats
	if ap.ctr.batsCount == 0 {
		//initialize shuffled bats
		for regIndex := range shuffledBats {
			shuffledBats[regIndex] = batch.NewWithSize(lenVecs)
			for j := range shuffledBats[regIndex].Vecs {
				shuffledBats[regIndex].Vecs[j] = proc.GetVector(*bat.Vecs[j].GetType())
			}
		}
	}

	sels := getShuffledSelsByRange(ap, bat)

	//generate new shuffled bats
	for regIndex := range shuffledBats {
		lenSels := len(sels[regIndex])
		if lenSels > 0 {
			b := shuffledBats[regIndex]
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return err
				}
			}
			b.AddRowCount(lenSels)
		}
	}

	ap.ctr.batsCount++
	return nil
}

func rangeShuffle(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			//send ap to regIndex
			return sendBatToIndex(ap, proc, bat, regIndex)
		}
	}

	err := genShuffledBatsByRange(ap, bat, proc)
	if err != nil {
		return false, err
	}
	if needToSendShuffledBats(ap) {
		return sendShuffledBats(ap, proc)
	}
	return false, nil
}

// shuffle to all receiver (include LocalReceiver and RemoteReceiver)
func shuffleToAllFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.ctr.prepared {
		end, err := ap.waitRemoteRegsReady(proc)
		if err != nil {
			return false, err
		}
		if end {
			return true, nil
		}
	}

	if ap.ShuffleType == int32(plan.ShuffleType_Hash) {
		return hashShuffle(bat, ap, proc)
	} else {
		return rangeShuffle(bat, ap, proc)
	}
}

// send to all receiver (include LocalReceiver and RemoteReceiver)
func sendToAllFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	end, remoteErr := sendToAllRemoteFunc(bat, ap, proc)
	if remoteErr != nil || end {
		return end, remoteErr
	}

	return sendToAllLocalFunc(bat, ap, proc)
}

// common sender: send to any LocalReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	for {
		sendto := ap.ctr.sendCnt % ap.ctr.localRegsCnt
		reg := ap.LocalRegs[sendto]
		select {
		case <-proc.Ctx.Done():
			logutil.Debugf("proc context done during dispatch to any")
			return true, nil
		case <-reg.Ctx.Done():
			logutil.Debugf("reg.Ctx done during dispatch to any")
			ap.LocalRegs = append(ap.LocalRegs[:sendto], ap.LocalRegs[sendto+1:]...)
			ap.ctr.localRegsCnt--
			ap.ctr.aliveRegCnt--
			close(reg.Ch)
			if ap.ctr.localRegsCnt == 0 {
				return true, nil
			}
		case reg.Ch <- bat:
			proc.SetInputBatch(nil)
			ap.ctr.sendCnt++
			return false, nil
		}
	}
}

// common sender: send to any RemoteReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyRemoteFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.ctr.prepared {
		end, _ := ap.waitRemoteRegsReady(proc)
		// update the cnt
		ap.ctr.remoteRegsCnt = len(ap.ctr.remoteReceivers)
		ap.ctr.aliveRegCnt = ap.ctr.remoteRegsCnt + ap.ctr.localRegsCnt
		if end || ap.ctr.remoteRegsCnt == 0 {
			return true, nil
		}
	}
	select {
	case <-proc.Ctx.Done():
		logutil.Debugf("conctx done during dispatch")
		return true, nil
	default:
	}

	encodeData, errEncode := types.Encode(bat)
	if errEncode != nil {
		return false, errEncode
	}

	for {
		regIdx := ap.ctr.sendCnt % ap.ctr.remoteRegsCnt
		reg := ap.ctr.remoteReceivers[regIdx]

		if err := sendBatchToClientSession(proc.Ctx, encodeData, reg); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrStreamClosed) {
				ap.ctr.remoteReceivers = append(ap.ctr.remoteReceivers[:regIdx], ap.ctr.remoteReceivers[regIdx+1:]...)
				ap.ctr.remoteRegsCnt--
				ap.ctr.aliveRegCnt--
				if ap.ctr.remoteRegsCnt == 0 {
					return true, nil
				}
				ap.ctr.sendCnt++
				continue
			} else {
				return false, err
			}
		}
		ap.ctr.sendCnt++
		return false, nil
	}
}

// Make sure enter this function LocalReceiver and RemoteReceiver are both not equal 0
func sendToAnyFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	toLocal := (ap.ctr.sendCnt % ap.ctr.aliveRegCnt) < ap.ctr.localRegsCnt
	if toLocal {
		allclosed, err := sendToAnyLocalFunc(bat, ap, proc)
		if err != nil {
			return false, nil
		}
		if allclosed { // all local reg closed, change sendFunc to send remote only
			proc.PutBatch(bat)
			ap.ctr.sendFunc = sendToAnyRemoteFunc
			return ap.ctr.sendFunc(bat, ap, proc)
		}
	} else {
		allclosed, err := sendToAnyRemoteFunc(bat, ap, proc)
		if err != nil {
			return false, nil
		}
		if allclosed { // all remote reg closed, change sendFunc to send local only
			ap.ctr.sendFunc = sendToAnyLocalFunc
			return ap.ctr.sendFunc(bat, ap, proc)
		}
	}
	return false, nil

}

func sendBatchToClientSession(ctx context.Context, encodeBatData []byte, wcs *WrapperClientSession) error {
	checksum := crc32.ChecksumIEEE(encodeBatData)
	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = pipeline.Last
			msg.Checksum = checksum
		}
		if err := wcs.cs.Write(ctx, msg); err != nil {
			return err
		}
		return nil
	}

	start := 0
	for start < len(encodeBatData) {
		end := start + maxMessageSizeToMoRpc
		sid := pipeline.WaitingNext
		if end > len(encodeBatData) {
			end = len(encodeBatData)
			sid = pipeline.Last
		}
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData[start:end]
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = uint64(sid)
			msg.Checksum = checksum
		}

		if err := wcs.cs.Write(ctx, msg); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// success count is always no greater than refcnt
func handleUnsent(proc *process.Process, bat *batch.Batch, refCnt int64, successCnt int64) {
	diff := successCnt - refCnt
	atomic.AddInt64(&bat.Cnt, diff)
	if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
		jm.IncRef(diff)
		jm.SetDupCount(diff)
	}

	proc.PutBatch(bat)
}
