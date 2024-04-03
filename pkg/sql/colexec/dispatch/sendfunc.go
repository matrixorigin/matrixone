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

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	var refCountAdd int64
	var err error
	if !ap.RecSink {
		refCountAdd = int64(ap.ctr.localRegsCnt - 1)
		atomic.AddInt64(&bat.Cnt, refCountAdd)
		if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
			jm.IncRef(refCountAdd)
			jm.SetDupCount(int64(ap.ctr.localRegsCnt))
		}
	}
	var bats []*batch.Batch
	if ap.RecSink {
		bats = append(bats, bat)
		for k := 1; k < len(ap.LocalRegs); k++ {
			bat, err = bat.Dup(proc.Mp())
			if err != nil {
				return false, err
			}
			bats = append(bats, bat)
		}
	}

	for i, reg := range ap.LocalRegs {
		if ap.RecSink {
			bat = bats[i]
		}
		select {
		case <-proc.Ctx.Done():
			handleUnsent(proc, bat, refCountAdd, int64(i))
			return true, nil

		case <-reg.Ctx.Done():
			if ap.IsSink {
				atomic.AddInt64(&bat.Cnt, -1)
				continue
			}
			handleUnsent(proc, bat, refCountAdd, int64(i))
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

func sendBatToIndex(ap *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) (err error) {
	for i, reg := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if regIndex == batIndex {
			if bat != nil && bat.RowCount() != 0 {
				select {
				case <-proc.Ctx.Done():
					return nil

				case <-reg.Ctx.Done():
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}

	forShuffle := false
	for _, r := range ap.ctr.remoteReceivers {
		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if regIndex == batIndex {
			if bat != nil && bat.RowCount() != 0 {
				forShuffle = true

				encodeData, errEncode := types.Encode(bat)
				if errEncode != nil {
					err = errEncode
					break
				}
				if err = sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
					break
				}
			}
		}
	}

	if forShuffle {
		// in shuffle dispatch, this batch only send to remote CN, we can safely put it back into pool
		proc.PutBatch(bat)
	}
	return err
}

func sendBatToLocalMatchedReg(ap *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)
	for i, reg := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if regIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && bat.RowCount() != 0 {
				select {
				case <-proc.Ctx.Done():
					return nil

				case <-reg.Ctx.Done():
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}
	return nil
}

func sendBatToMultiMatchedReg(ap *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)
	atomic.AddInt64(&bat.Cnt, 1)
	defer atomic.AddInt64(&bat.Cnt, -1)
	for i, reg := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if regIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && bat.RowCount() != 0 {
				select {
				case <-proc.Ctx.Done():
					return nil

				case <-reg.Ctx.Done():
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}
	for _, r := range ap.ctr.remoteReceivers {
		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if regIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && bat.RowCount() != 0 {
				encodeData, errEncode := types.Encode(bat)
				if errEncode != nil {
					return errEncode
				}
				if err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
					return err
				}
			}
		}
	}
	return nil
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

	ap.ctr.batchCnt[bat.ShuffleIDX]++
	ap.ctr.rowCnt[bat.ShuffleIDX] += bat.RowCount()
	if ap.ShuffleType == plan2.ShuffleToRegIndex {
		return false, sendBatToIndex(ap, proc, bat, uint32(bat.ShuffleIDX))
	} else if ap.ShuffleType == plan2.ShuffleToLocalMatchedReg {
		return false, sendBatToLocalMatchedReg(ap, proc, bat, uint32(bat.ShuffleIDX))
	} else {
		return false, sendBatToMultiMatchedReg(ap, proc, bat, uint32(bat.ShuffleIDX))
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
			return true, nil

		case <-reg.Ctx.Done():
			ap.LocalRegs = append(ap.LocalRegs[:sendto], ap.LocalRegs[sendto+1:]...)
			ap.ctr.localRegsCnt--
			ap.ctr.aliveRegCnt--
			close(reg.Ch)
			if ap.ctr.localRegsCnt == 0 {
				return true, nil
			}

		case reg.Ch <- bat:
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
		end, err := ap.waitRemoteRegsReady(proc)
		if err != nil {
			return false, err
		}
		// update the cnt
		ap.ctr.remoteRegsCnt = len(ap.ctr.remoteReceivers)
		ap.ctr.aliveRegCnt = ap.ctr.remoteRegsCnt + ap.ctr.localRegsCnt
		if end || ap.ctr.remoteRegsCnt == 0 {
			return true, nil
		}
	}
	select {
	case <-proc.Ctx.Done():
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

func sendBatchToClientSession(ctx context.Context, encodeBatData []byte, wcs process.WrapCs) error {
	checksum := crc32.ChecksumIEEE(encodeBatData)
	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.MsgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.Method_BatchMessage
			msg.Sid = pipeline.Status_Last
			msg.Checksum = checksum
		}
		if err := wcs.Cs.Write(ctx, msg, 0); err != nil {
			return err
		}
		return nil
	}

	start := 0
	for start < len(encodeBatData) {
		end := start + maxMessageSizeToMoRpc
		sid := pipeline.Status_WaitingNext
		if end > len(encodeBatData) {
			end = len(encodeBatData)
			sid = pipeline.Status_Last
		}
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.MsgId
			msg.Data = encodeBatData[start:end]
			msg.Cmd = pipeline.Method_BatchMessage
			msg.Sid = sid
			msg.Checksum = checksum
		}

		if err := wcs.Cs.Write(ctx, msg, 0); err != nil {
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
