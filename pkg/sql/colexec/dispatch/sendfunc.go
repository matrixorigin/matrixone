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
			receiver := ap.ctr.remoteReceivers[i]
			receiverID := fmt.Sprintf("%s(MsgId=%d)", receiver.Uid.String(), receiver.MsgId)

			// SendToAll requires strict failure checking
			// If any receiver fails, we must report error to prevent data loss
			remove, err := sendBatchToClientSession(proc.Ctx, encodeData, receiver, FailureModeStrict, receiverID)
			if err != nil {
				return false, err
			}

			if remove {
				ap.ctr.removeIdxReceiver(i)
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

	for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
		r := ap.ctr.remoteReceivers[i]

		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex == batIndex {
			if bat != nil && !bat.IsEmpty() {
				receiverID := fmt.Sprintf("%s(ShuffleIdx=%d)", r.Uid.String(), shuffleIndex)

				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
				if errEncode != nil {
					err = errEncode
					break
				}

				// Shuffle requires strict failure checking
				// If target receiver fails, data for this shuffle key will be lost
				remove, errSend := sendBatchToClientSession(proc.Ctx, encodeData, r, FailureModeStrict, receiverID)
				if errSend != nil {
					err = errSend
					break
				}

				if remove {
					// In shuffle scenario, if target receiver is removed, it's a critical error
					err = moerr.NewInternalError(proc.Ctx, fmt.Sprintf(
						"shuffle target receiver %s was removed, data loss may occur", receiverID))
					break
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
	for i := 0; i < len(ap.ctr.remoteReceivers); i++ {
		r := ap.ctr.remoteReceivers[i]

		batIndex := uint32(ap.ctr.remoteToIdx[r.Uid])
		if shuffleIndex%localRegsCnt == batIndex%localRegsCnt {
			if bat != nil && !bat.IsEmpty() {
				receiverID := fmt.Sprintf("%s(ShuffleIdx=%d)", r.Uid.String(), shuffleIndex)

				encodeData, errEncode := bat.MarshalBinaryWithBuffer(&ap.ctr.marshalBuf)
				if errEncode != nil {
					return errEncode
				}

				// Shuffle requires strict failure checking
				remove, err := sendBatchToClientSession(proc.Ctx, encodeData, r, FailureModeStrict, receiverID)
				if err != nil {
					return err
				}

				if remove {
					return moerr.NewInternalError(proc.Ctx, fmt.Sprintf(
						"shuffle target receiver %s was removed, data loss may occur", receiverID))
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

	// SendToAny can tolerate individual receiver failures
	// We can try other receivers if one fails
	maxRetries := len(ap.ctr.remoteReceivers)
	retriesLeft := maxRetries

	for retriesLeft > 0 {
		if ap.ctr.remoteRegsCnt == 0 {
			return false, moerr.NewInternalError(proc.Ctx,
				"sendToAny failed: all remote receivers are unavailable")
		}

		regIdx := ap.ctr.sendCnt % ap.ctr.remoteRegsCnt
		reg := ap.ctr.remoteReceivers[regIdx]
		receiverID := fmt.Sprintf("%s(Idx=%d)", reg.Uid.String(), regIdx)

		// SendToAny uses tolerant mode - can failover to other receivers
		remove, err := sendBatchToClientSession(proc.Ctx, encodeData, reg, FailureModeTolerant, receiverID)
		if err != nil {
			// Network error or other critical error
			return false, err
		}

		if remove {
			// Receiver is done, try next one
			ap.ctr.removeIdxReceiver(regIdx)
			ap.ctr.sendCnt++
			retriesLeft--
			continue
		}

		// Send succeeded
		ap.ctr.sendCnt++
		return false, nil
	}

	// All receivers failed
	return false, moerr.NewInternalError(proc.Ctx,
		fmt.Sprintf("sendToAny failed: tried %d receivers, all unavailable", maxRetries))
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
//   - receiverDone: whether the receiver is done (normally or abnormally)
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
) (receiverDone bool, err error) {
	wcs.Lock()
	defer wcs.Unlock()

	if wcs.ReceiverDone {
		// Critical fix: distinguish between strict and tolerant modes
		if failureMode == FailureModeStrict {
			// Strict mode: receiver done indicates data loss
			// This happens when remote CN crashes or cancels
			logutil.Error("[DEBUG] sendBatchToClientSession: ReceiverDone=true in strict mode",
				zap.String("receiverID", receiverID),
				zap.Uint64("msgId", wcs.MsgId),
				zap.String("uid", wcs.Uid.String()))
			return true, moerr.NewInternalError(ctx, fmt.Sprintf(
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
			return true, nil
		}
	}

	// Send data (original logic unchanged)
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
		}

		if err = wcs.Cs.Write(ctx, msg); err != nil {
			return false, err
		}
		start = end
	}
	return false, nil
}
