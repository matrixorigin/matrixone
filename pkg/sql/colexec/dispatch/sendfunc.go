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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"hash/crc32"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getShuffledBats(ap *Argument, bat *batch.Batch, lenRegs int, proc *process.Process) ([]*batch.Batch, error) {
	//release old bats
	defer proc.PutBatch(bat)

	lenVecs := len(bat.Vecs)
	shuffledBats := make([]*batch.Batch, lenRegs)
	sels, lenShuffledSels := ap.getSels()

	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedCol[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := v % int64(lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
			lenShuffledSels[regIndex]++
		}
	case types.T_int32:
		groupByCol := vector.MustFixedCol[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := v % int32(lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
			lenShuffledSels[regIndex]++
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}

	//generate new shuffled bats
	for regIndex := range shuffledBats {
		if lenShuffledSels[regIndex] > 0 {
			shuffledBats[regIndex] = batch.NewWithSize(lenVecs)
			for j := range shuffledBats[regIndex].Vecs {
				shuffledBats[regIndex].Vecs[j] = proc.GetVector(*bat.Vecs[j].GetType())
			}

			b := shuffledBats[regIndex]
			for vecIndex := range b.Vecs {
				v := b.Vecs[vecIndex]
				err := v.Union(bat.Vecs[vecIndex], sels[regIndex], proc.Mp())
				if err != nil {
					return nil, err
				}
			}
			b.Zs = proc.Mp().GetSels()
			for i := 0; i < lenShuffledSels[regIndex]; i++ {
				b.Zs = append(b.Zs, bat.Zs[sels[regIndex][i]])
			}
		}
	}

	return shuffledBats, nil
}

// common sender: shuffle to all LocalReceiver
func shuffleToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	shuffledBats, err := getShuffledBats(ap, bat, ap.ctr.aliveRegCnt, proc)
	if err != nil {
		return false, err
	}

	for i, reg := range ap.LocalRegs {
		if shuffledBats[i] != nil {
			select {
			case <-reg.Ctx.Done():
				return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
			case reg.Ch <- shuffledBats[i]:
			}
		}
	}

	return false, nil
}

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	refCountAdd := int64(ap.ctr.localRegsCnt - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(ap.ctr.localRegsCnt))
	}

	for i, reg := range ap.LocalRegs {
		select {
		case <-proc.Ctx.Done():
			handleUnsent(proc, bat, refCountAdd, int64(i))
			logutil.Infof("proc context done during dispatch to local")
			return true, nil
		case <-reg.Ctx.Done():
			handleUnsent(proc, bat, refCountAdd, int64(i))
			return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
		case reg.Ch <- bat:
		}
	}

	return false, nil
}

// common sender: shuffle to all RemoteReceiver
func shuffleToAllRemoteFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.ctr.prepared {
		end, err := ap.waitRemoteRegsReady(proc)
		if err != nil {
			return false, err
		}
		if end {
			return true, nil
		}
	}

	shuffledBats, err := getShuffledBats(ap, bat, ap.ctr.aliveRegCnt, proc)
	if err != nil {
		return false, err
	}

	// send to remote regs
	for i, r := range ap.ctr.remoteReceivers {
		if shuffledBats[i] != nil {
			encodeData, errEncode := types.Encode(shuffledBats[i])
			if errEncode != nil {
				return false, errEncode
			}
			if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
				return false, err
			}
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

	shuffledBats, err := getShuffledBats(ap, bat, ap.ctr.aliveRegCnt, proc)
	if err != nil {
		return false, err
	}
	regIdx := 0
	// send to remote regs
	for _, r := range ap.ctr.remoteReceivers {
		if shuffledBats[regIdx] != nil {
			encodeData, errEncode := types.Encode(shuffledBats[regIdx])
			if errEncode != nil {
				return false, errEncode
			}
			if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
				return false, err
			}
		}
		regIdx++
	}

	//send to all local regs
	for _, reg := range ap.LocalRegs {
		if shuffledBats[regIdx] != nil {
			select {
			case <-reg.Ctx.Done():
				return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
			case reg.Ch <- shuffledBats[regIdx]:
			}
		}
		regIdx++
	}

	return false, nil
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
			logutil.Infof("proc context done during dispatch to any")
			return true, nil
		case <-reg.Ctx.Done():
			logutil.Infof("reg.Ctx done during dispatch to any")
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
		logutil.Infof("conctx done during dispatch")
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
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(diff)
		jm.SetDupCount(diff)
	}

	proc.PutBatch(bat)
}
