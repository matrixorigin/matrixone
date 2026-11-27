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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// receiverFailureMode defines how to handle receiver failures
type receiverFailureMode int

const (
	// FailureModeStrict: receiver failure MUST be reported as error
	// Used for SendToAll and Shuffle scenarios where data completeness is critical
	FailureModeStrict receiverFailureMode = iota

	// FailureModeTolerant: receiver failure can be tolerated
	// Used for SendToAny scenarios where we can failover to other receivers
	FailureModeTolerant
)

type remoteSendResult uint8

const (
	remoteBatchSent remoteSendResult = iota
	remoteReceiverStopped
	remoteReceiverUnavailable
)

var (
	sendToAnyLocal  = sendToAnyLocalFunc
	sendToAnyRemote = sendToAnyRemoteFunc
)

func (ctr *container) removeIdxReceiver(idx int) {
	ctr.remoteReceivers = append(ctr.remoteReceivers[:idx], ctr.remoteReceivers[idx+1:]...)
	ctr.remoteRegsCnt--
	ctr.aliveRegCnt--
}

// common sender: send to all LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	queryDone, err := ap.ctr.sp.SendBatch(proc.Ctx, pSpool.SendToAllLocal, bat, nil)
	if queryDone || err != nil {
		return queryDone, err
	}
	for i, reg := range ap.LocalRegs {
		if !reg.SendData(proc.Ctx, ap.ctr.sp, i) {
			return true, nil
		}
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
		encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf, true)
		if errEncode != nil {
			return false, errEncode
		}

		for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
			receiver := ap.ctr.remoteReceivers[i]
			receiverID := fmt.Sprintf("%s(MsgId=%d)", receiver.Uid.String(), receiver.MsgId)

			// SendToAll requires strict failure checking
			// If any receiver fails, we must report error to prevent data loss
			outcome, err := sendBatchToClientSession(proc.Ctx, encodeData, receiver, FailureModeStrict, receiverID)
			if err != nil {
				return false, err
			}

			if outcome == remoteReceiverStopped {
				ap.ctr.removeIdxReceiver(i)
				i--
			}
		}
	}

	return ap.ctr.remoteRegsCnt == 0, nil
}

func sendBatToIndex(ap *Dispatch, proc *process.Process, bat *batch.Batch, shuffleIndex uint32) (err error) {
	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if shuffleIndex == batIndex {
			if done, err := sendLocalShuffleBatch(i, ap, proc, bat); done || err != nil {
				return err
			}
			break
		}
	}

	for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
		r := ap.ctr.remoteReceivers[i]

		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex == batIndex {
			if bat != nil && !bat.IsEmpty() {
				receiverID := fmt.Sprintf("%s(ShuffleIdx=%d)", r.Uid.String(), shuffleIndex)
				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf, true)
				if errEncode != nil {
					err = errEncode
					break
				}

				// Shuffle requires strict failure checking
				// If target receiver fails, data for this shuffle key will be lost
				outcome, errSend := sendBatchToClientSession(proc.Ctx, encodeData, r, FailureModeStrict, receiverID)
				if errSend != nil {
					err = errSend
					break
				}

				if outcome == remoteReceiverStopped {
					ap.ctr.removeIdxReceiver(i)
				}
			}
			// Found the target receiver, exit loop
			break
		}
	}

	return err
}

func sendBatToLocalMatchedReg(ap *Dispatch, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)
	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if regIndex%localRegsCnt == batIndex%localRegsCnt {
			_, err := sendLocalShuffleBatch(i, ap, proc, bat)
			return err
		}
	}
	return nil
}

func sendBatToMultiMatchedReg(ap *Dispatch, proc *process.Process, bat *batch.Batch, shuffleIndex uint32) error {
	localRegsCnt := uint32(ap.ctr.localRegsCnt)

	// send to remote first because send to spool will modify the bat.Agg.
	for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
		r := ap.ctr.remoteReceivers[i]

		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && !bat.IsEmpty() {
				receiverID := fmt.Sprintf("%s(ShuffleIdx=%d)", r.Uid.String(), shuffleIndex)
				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf, true)
				if errEncode != nil {
					return errEncode
				}

				// Shuffle requires strict failure checking
				outcome, err := sendBatchToClientSession(proc.Ctx, encodeData, r, FailureModeStrict, receiverID)
				if err != nil {
					return err
				}

				if outcome == remoteReceiverStopped {
					ap.ctr.removeIdxReceiver(i)
					i--
				}
			}
		}
	}

	// send to matched local.
	for i := range ap.LocalRegs {
		batIndex := uint32(ap.ShuffleRegIdxLocal[i])
		if shuffleIndex%localRegsCnt == batIndex%localRegsCnt {
			_, err := sendLocalShuffleBatch(i, ap, proc, bat)
			return err
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
	var err error
	switch ap.ShuffleType {
	case plan2.ShuffleToRegIndex:
		err = sendBatToIndex(ap, proc, bat, uint32(bat.ShuffleIDX))
	case plan2.ShuffleToLocalMatchedReg:
		err = sendBatToLocalMatchedReg(ap, proc, bat, uint32(bat.ShuffleIDX))
	default:
		err = sendBatToMultiMatchedReg(ap, proc, bat, uint32(bat.ShuffleIDX))
	}
	return err == nil && ap.ctr.aliveRegCnt == 0, err
}

// send to all receiver (include LocalReceiver and RemoteReceiver)
func sendToAllFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	_, remoteErr := sendToAllRemoteFunc(bat, ap, proc)
	if remoteErr != nil {
		return false, remoteErr
	}

	return sendToAllLocalFunc(bat, ap, proc)
}

func sendLocalShuffleBatch(i int, ap *Dispatch, proc *process.Process, bat *batch.Batch) (bool, error) {
	done, err := ap.ctr.sp.SendBatch(proc.Ctx, i, bat, nil)
	if err != nil {
		return done, err
	}
	if !done && onlyOneRegToDealThis(i, ap, proc) {
		return false, nil
	}
	if err := ap.LocalRegs[i].Err(); err != nil {
		return true, err
	}
	return true, moerr.PipelineContextError(proc.Ctx)
}

func onlyOneRegToDealThis(sendto int, ap *Dispatch, proc *process.Process) bool {
	return ap.LocalRegs[sendto].SendData(proc.Ctx, ap.ctr.sp, sendto)
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
	if !onlyOneRegToDealThis(sendto, ap, proc) {
		return true, nil
	}

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
	if err := moerr.PipelineContextError(proc.Ctx); err != nil {
		return false, err
	}
	if ap.ctr.remoteRegsCnt == 0 {
		return true, nil
	}

	encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf, true)
	if errEncode != nil {
		return false, errEncode
	}

	retryCount := ap.ctr.remoteRegsCnt
	unavailable := false
	for ap.ctr.remoteRegsCnt > 0 {
		regIdx := ap.ctr.sendCnt % ap.ctr.remoteRegsCnt
		reg := ap.ctr.remoteReceivers[regIdx]
		receiverID := fmt.Sprintf("%s(Idx=%d)", reg.Uid.String(), regIdx)
		outcome, err := sendBatchToClientSession(proc.Ctx, encodeData, reg, FailureModeTolerant, receiverID)
		if err != nil {
			return false, err
		}

		ap.ctr.sendCnt++
		if outcome == remoteBatchSent {
			return false, nil
		}
		unavailable = unavailable || outcome == remoteReceiverUnavailable
		ap.ctr.removeIdxReceiver(regIdx)
	}
	if unavailable {
		return false, moerr.NewInternalErrorf(proc.Ctx,
			"sendToAny failed: tried %d receivers, all unavailable", retryCount)
	}
	return true, nil
}

// Make sure enter this function LocalReceiver and RemoteReceiver are both not equal 0
func sendToAnyFunc(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error) {
	toLocal := (ap.ctr.sendCnt % ap.ctr.aliveRegCnt) < ap.ctr.localRegsCnt
	if toLocal {
		allclosed, err := sendToAnyLocal(bat, ap, proc)
		if err != nil {
			return false, err
		}
		if allclosed { // all local reg closed, change sendFunc to send remote only
			ap.ctr.sendFunc = sendToAnyRemoteFunc
			return sendToAnyRemote(bat, ap, proc)
		}
	} else {
		allclosed, err := sendToAnyRemote(bat, ap, proc)
		if err != nil {
			return false, err
		}
		if allclosed { // all remote reg closed, change sendFunc to send local only
			ap.ctr.sendFunc = sendToAnyLocalFunc
			return sendToAnyLocal(bat, ap, proc)
		}
	}
	return false, nil

}

// sendBatchToClientSession sends batch data to remote receiver
//
// Parameters:
//   - ctx: context
//   - encodeBatData: encoded batch data
//   - wcs: wrapped client session
//   - failureMode: how to handle receiver failures (strict or tolerant)
//   - receiverID: receiver identifier for error messages
//
// Returns:
//   - outcome: a sent batch, a normal stop, or an unavailable receiver
//   - err: error if any
//
// Critical fix for silent data loss:
// When ReceiverDone=true, the behavior depends on failureMode:
//   - FailureModeStrict: MUST return error (for SendToAll/Shuffle)
//   - FailureModeTolerant: Can return success (for SendToAny)
func sendBatchToClientSession(
	ctx context.Context,
	encodeBatData []byte,
	wcs *process.WrapCs,
	failureMode receiverFailureMode,
	receiverID string,
) (outcome remoteSendResult, err error) {
	wcs.Lock()
	defer wcs.Unlock()

	if wcs.ReceiverDone {
		// Critical fix: distinguish between strict and tolerant modes
		if failureMode == FailureModeStrict {
			// Strict mode: receiver done indicates data loss
			// This happens when remote CN crashes or cancels
			logutil.Debug("sendBatchToClientSession: ReceiverDone=true in strict mode",
				zap.String("receiverID", receiverID),
				zap.Uint64("msgId", wcs.MsgId),
				zap.String("uid", wcs.Uid.String()))
			return remoteReceiverUnavailable, moerr.NewInternalError(ctx, fmt.Sprintf(
				"remote receiver %s is already done, data loss may occur. "+
					"This usually indicates the remote CN has failed or been canceled",
				receiverID))
		} else {
			// Tolerant mode: acceptable for SendToAny scenarios
			// We can try other receivers
			// Use non-blocking send to avoid potential deadlock
			select {
			case wcs.Err <- nil:
				// Error notification sent successfully
			default:
				// Channel full or no receiver, that's acceptable
				// Receiver will eventually timeout or get canceled via context
			}
			return remoteReceiverUnavailable, nil
		}
	}

	var batchSequence uint64
	batchSent := false
	if wcs.ReserveBatch != nil {
		defer func() {
			if err != nil {
				wcs.BatchSendError = err
			}
		}()
		var receiverStopped bool
		batchSequence, receiverStopped, err = wcs.ReserveBatch(ctx, uint64(len(encodeBatData)))
		if err != nil {
			return remoteReceiverUnavailable, err
		}
		if receiverStopped {
			return remoteReceiverStopped, nil
		}
		defer func() {
			if !batchSent && wcs.RollbackBatch != nil {
				wcs.RollbackBatch(batchSequence)
			}
		}()
	}

	// A logical batch uses one sequence number even when morpc fragmentation is
	// required. The receiver acknowledges it only after all fragments have been
	// reconstructed and handed to the local pipeline.
	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.MsgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.Method_BatchMessage
			msg.Sid = pipeline.Status_Last
			msg.BatchSequence = batchSequence
			msg.AcceptedBatchCreditCount = wcs.BatchCredits
			msg.AcceptedBatchCreditBytes = wcs.ByteCredits
		}
		if err = wcs.Cs.Write(ctx, msg); err != nil {
			return remoteReceiverUnavailable, moerr.PipelineRPCIOError(ctx, err)
		}
		batchSent = true
		return remoteBatchSent, nil
	}

	// Send large message in chunks (original logic unchanged)
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
			msg.BatchSequence = batchSequence
			msg.AcceptedBatchCreditCount = wcs.BatchCredits
			msg.AcceptedBatchCreditBytes = wcs.ByteCredits
		}

		if err = wcs.Cs.Write(ctx, msg); err != nil {
			return remoteReceiverUnavailable, moerr.PipelineRPCIOError(ctx, err)
		}
		start = end
	}
	batchSent = true
	return remoteBatchSent, nil
}
