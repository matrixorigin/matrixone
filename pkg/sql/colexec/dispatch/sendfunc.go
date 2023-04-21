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
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB

	// send to all reg functions
	SendToAllLocalFunc = iota
	SendToAllRemoteFunc
	SendToAllFunc

	// send to any reg functions
	SendToAnyLocalFunc
	SendToAnyRemoteFunc
	SendToAnyFunc
)

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	refCountAdd := int64(len(ap.LocalRegs) - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(len(ap.LocalRegs)))
	}

	for _, reg := range ap.LocalRegs {
		select {
		case <-reg.Ctx.Done():
			return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
		case reg.Ch <- bat:
		}
	}

	return false, nil
}

// common sender: send to all RemoteReceiver
func sendToAllRemoteFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.prepared {
		ap.waitRemoteRegsReady(proc)
	}

	{ // send to remote regs
		encodeData, errEncode := types.Encode(bat)
		if errEncode != nil {
			return false, errEncode
		}
		for _, r := range ap.ctr.remoteReceivers {
			if err := sendBatchToClientSession(encodeData, r); err != nil {
				return false, err
			}
		}
	}

	return false, nil
}

// send to all receiver (include LocalReceiver and RemoteReceiver)
func sendToAllFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	_, remoteErr := sendToAllRemoteFunc(bat, ap, proc)
	if remoteErr != nil {
		return false, remoteErr
	}

	_, localErr := sendToAllLocalFunc(bat, ap, proc)
	if localErr != nil {
		return false, localErr
	}

	return false, nil
}

// common sender: send to any LocalReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	for {
		sendto := ap.sendCnt % ap.localRegsCnt
		reg := ap.LocalRegs[sendto]
		select {
		case <-reg.Ctx.Done():
			for len(reg.Ch) > 0 { // free memory
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				proc.PutBatch(bat)
			}
			ap.LocalRegs = append(ap.LocalRegs[:sendto], ap.LocalRegs[sendto+1:]...)
			ap.localRegsCnt--
			ap.aliveRegCnt--
			if ap.localRegsCnt == 0 {
				return true, nil
			}
		case reg.Ch <- bat:
			proc.SetInputBatch(nil)
			ap.sendCnt++
			return false, nil
		}
	}
}

// common sender: send to any RemoteReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyRemoteFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	if !ap.prepared {
		ap.waitRemoteRegsReady(proc)
	}

	encodeData, errEncode := types.Encode(bat)
	if errEncode != nil {
		return false, errEncode
	}

	for {
		regIdx := ap.sendCnt % ap.remoteRegsCnt
		reg := ap.ctr.remoteReceivers[regIdx]

		if err := sendBatchToClientSession(encodeData, reg); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrStreamClosed) {
				ap.ctr.remoteReceivers = append(ap.ctr.remoteReceivers[:regIdx], ap.ctr.remoteReceivers[regIdx+1:]...)
				ap.remoteRegsCnt--
				ap.aliveRegCnt--
				if ap.remoteRegsCnt == 0 {
					return true, nil
				}
				ap.sendCnt++
				continue
			} else {
				return false, err
			}
		}
		ap.sendCnt++
		return false, nil
	}
}

// Make sure enter this function LocalReceiver and RemoteReceiver are both not equal 0
func sendToAnyFunc(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error) {
	toLocal := (ap.sendCnt % ap.aliveRegCnt) < ap.localRegsCnt
	if toLocal {
		allclosed, err := sendToAnyLocalFunc(bat, ap, proc)
		if err != nil {
			return false, nil
		}
		if allclosed { // all local reg closed, change sendFunc to send remote only
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

func sendBatchToClientSession(encodeBatData []byte, wcs *WrapperClientSession) error {
	checksum := crc32.ChecksumIEEE(encodeBatData)
	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
		_ = cancel
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = pipeline.Last
			msg.Checksum = checksum
		}
		if err := wcs.cs.Write(timeoutCtx, msg); err != nil {
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
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
		_ = cancel
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData[start:end]
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = uint64(sid)
			msg.Checksum = checksum
		}

		if err := wcs.cs.Write(timeoutCtx, msg); err != nil {
			return err
		}
		start = end
	}
	return nil
}
