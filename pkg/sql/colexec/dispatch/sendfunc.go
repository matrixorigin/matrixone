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

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	queryDone, err := ap.ctr.sp.SendBatch(proc.Ctx, pSpool.SendToAllLocal, bat, nil)
	if queryDone || err != nil {
		return queryDone, err
	}
	for i, reg := range ap.LocalRegs {
		reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(ap.ctr.sp, i)
	}
	return false, nil
}

// common sender: send to all RemoteReceiver
func sendToAllRemoteFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
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
		encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
		if errEncode != nil {
			return false, errEncode
		}

		for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
			remove, err := sendBatchToClientSession(proc.Ctx, encodeData, ap.ctr.remoteReceivers[i])
			if err != nil {
				return false, err
			}

			if remove {
				ap.ctr.remoteReceivers = append(ap.ctr.remoteReceivers[:i], ap.ctr.remoteReceivers[i+1:]...)
				ap.ctr.remoteRegsCnt--
				ap.ctr.aliveRegCnt--
				if ap.ctr.remoteRegsCnt == 0 {
					return true, nil
				}
				i--
			}
		}
	}

	return false, nil
}

func sendBatToIndex(ap *Dispatch, proc *process.Process, bat *batch.Batch, shuffleIndex uint32) (err error) {
	var queryDone bool

	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if shuffleIndex == batIndex {
			queryDone, err = ap.ctr.sp.SendBatch(proc.Ctx, i, bat, nil)
			if err != nil || queryDone {
				return err
			}
			onlyOneRegToDealThis(i, ap)
			break
		}
	}

	for _, r := range ap.ctr.remoteReceivers {
		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex == batIndex {
			if bat != nil && !bat.IsEmpty() {
				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
				if errEncode != nil {
					err = errEncode
					break
				}
				if _, errSend := sendBatchToClientSession(proc.Ctx, encodeData, r); errSend != nil {
					err = errSend
					break
				}
			}
		}
	}

	return err
}

func sendBatToLocalMatchedReg(ap *Dispatch, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)
	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if regIndex%localRegsCnt == batIndex%localRegsCnt {
			queryDone, err := ap.ctr.sp.SendBatch(proc.Ctx, i, bat, nil)
			if err != nil || queryDone {
				return err
			}
			onlyOneRegToDealThis(i, ap)
			break
		}
	}
	return nil
}

func sendBatToMultiMatchedReg(ap *Dispatch, proc *process.Process, bat *batch.Batch, shuffleIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)

	// send to remote first because send to spool will modify the bat.Agg.
	for _, r := range ap.ctr.remoteReceivers {
		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && !bat.IsEmpty() {
				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
				if errEncode != nil {
					return errEncode
				}
				if _, err := sendBatchToClientSession(proc.Ctx, encodeData, r); err != nil {
					return err
				}
			}
		}
	}

	// send to matched local.
	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if shuffleIndex%localRegsCnt == batIndex%localRegsCnt {
			queryDone, err := ap.ctr.sp.SendBatch(proc.Ctx, i, bat, nil)
			if err != nil || queryDone {
				return err
			}
			onlyOneRegToDealThis(i, ap)
			break
		}
	}

	return nil
}

// shuffle to all receiver (include LocalReceiver and RemoteReceiver)
func shuffleToAllFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
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
func sendToAllFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	end, remoteErr := sendToAllRemoteFunc(bat, ap, proc)
	if remoteErr != nil || end {
		return end, remoteErr
	}

	return sendToAllLocalFunc(bat, ap, proc)
}

func onlyOneRegToDealThis(sendto int, ap *Dispatch) {
	ap.LocalRegs[sendto].Ch2 <- process.NewPipelineSignalToGetFromSpool(ap.ctr.sp, sendto)
}

// common sender: send to any LocalReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyLocalFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	sendto := ap.ctr.sendCnt % ap.ctr.localRegsCnt

	queryDone, err := ap.ctr.sp.SendBatch(proc.Ctx, sendto, bat, nil)
	if err != nil || queryDone {
		return true, err
	}
	onlyOneRegToDealThis(sendto, ap)

	ap.ctr.sendCnt++

	return false, nil
}

// common sender: send to any RemoteReceiver
// if the reg which you want to send to is closed
// send it to next one.
func sendToAnyRemoteFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
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

	encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
	if errEncode != nil {
		return false, errEncode
	}

	for {
		regIdx := ap.ctr.sendCnt % ap.ctr.remoteRegsCnt
		reg := ap.ctr.remoteReceivers[regIdx]

		if remove, err := sendBatchToClientSession(proc.Ctx, encodeData, reg); err != nil {
			return false, err
		} else {
			if remove {
				ap.ctr.remoteReceivers = append(ap.ctr.remoteReceivers[:regIdx], ap.ctr.remoteReceivers[regIdx+1:]...)
				ap.ctr.remoteRegsCnt--
				ap.ctr.aliveRegCnt--
				if ap.ctr.remoteRegsCnt == 0 {
					return true, nil
				}
				ap.ctr.sendCnt++
				continue
			}
		}

		ap.ctr.sendCnt++
		return false, nil
	}
}

// Make sure enter this function LocalReceiver and RemoteReceiver are both not equal 0
func sendToAnyFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	toLocal := (ap.ctr.sendCnt % ap.ctr.aliveRegCnt) < ap.ctr.localRegsCnt
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

func sendBatchToClientSession(ctx context.Context, encodeBatData []byte, wcs *process.WrapCs) (receiverSafeDone bool, err error) {
	wcs.Lock()
	defer wcs.Unlock()

	if wcs.ReceiverDone {
		wcs.Err <- nil
		return true, nil
	}

	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.MsgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.Method_BatchMessage
			msg.Sid = pipeline.Status_Last
		}
		if err = wcs.Cs.Write(ctx, msg); err != nil {
			return false, err
		}
		return false, nil
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
		}

		if err = wcs.Cs.Write(ctx, msg); err != nil {
			return false, err
		}
		start = end
	}
	return false, nil
}
