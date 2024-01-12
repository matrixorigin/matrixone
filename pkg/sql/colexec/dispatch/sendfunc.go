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
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"hash/crc32"
)

// common sender: send to all LocalReceivers
func (arg *Argument) sendToAllLocalReceivers(proc *process.Process, bat *batch.Batch) error {
	list, err := dealRefers(bat, arg.ctr.localRegsCnt, arg.RecSink, proc)
	if err != nil {
		return err
	}

	if arg.RecSink {
		for i, b := range list {
			select {
			case <-proc.Ctx.Done():
			case <-arg.LocalRegs[i].Ctx.Done():
			case arg.LocalRegs[i].Ch <- b:
				continue
			}

			for j := i; j < len(list); j++ {
				proc.PutBatch(list[j])
			}
			return nil
		}

	} else {
		for i, reg := range arg.LocalRegs {
			select {
			case <-proc.Ctx.Done():
				handleUnsentRefers(bat, arg.RecSink, arg.ctr.localRegsCnt, i)
				arg.ctr.stopSending()
				return nil

			case <-reg.Ctx.Done():
				if arg.IsSink {
					bat.AddCnt(-1)
					continue
				}
				handleUnsentRefers(bat, arg.RecSink, arg.ctr.localRegsCnt, i)
				arg.ctr.stopSending()
				return nil

			case reg.Ch <- bat:
			}
		}
	}

	return nil
}

// common sender: send to all RemoteReceivers
func (arg *Argument) sendToAllRemoteReceivers(proc *process.Process, bat *batch.Batch) error {
	data, err := types.Encode(bat)
	if err != nil {
		return err
	}

	for _, receiver := range arg.ctr.remoteReceivers {
		if err = sendBatchToClientSession(proc.Ctx, data, receiver); err != nil {
			return err
		}
	}
	return nil
}

// common sender: send to all RemoteReceivers and LocalReceivers
func (arg *Argument) sendToAllReceivers(proc *process.Process, bat *batch.Batch) error {
	if err := arg.sendToAllRemoteReceivers(proc, bat); err != nil {
		return err
	}
	return arg.sendToAllLocalReceivers(proc, bat)
}

// common sender: send to any LocalReceiver
// if the reg which you want to send to is closed, send it to the next one.
func (arg *Argument) sendToAnyLocalReceiver(proc *process.Process, bat *batch.Batch) error {
	bat.AddCnt(1)
	for arg.ctr.aliveRegCnt > 0 {
		idx := arg.ctr.sendCnt % arg.ctr.localRegsCnt
		reg := arg.LocalRegs[idx]
		select {
		case <-proc.Ctx.Done():
			bat.AddCnt(-1)
			arg.ctr.stopSending()
			return nil

		case <-reg.Ctx.Done():
			arg.LocalRegs = append(arg.LocalRegs[:idx], arg.LocalRegs[idx+1:]...)
			arg.ctr.localRegsCnt--
			arg.ctr.aliveRegCnt--
			if arg.ctr.aliveRegCnt == 0 {
				bat.AddCnt(-1)
				arg.ctr.stopSending()
				return nil
			}

		case reg.Ch <- bat:
			arg.ctr.sendCnt++
			return nil
		}
	}
	return nil
}

// common sender: send to any RemoteReceiver
// if the reg which you want to send to is closed, send it to the next one.
func (arg *Argument) sendToAnyRemoteReceiver(proc *process.Process, bat *batch.Batch) error {
	data, err := types.Encode(bat)
	if err != nil {
		return err
	}

	for {
		idx := arg.ctr.sendCnt % arg.ctr.remoteRegsCnt
		reg := arg.ctr.remoteReceivers[idx]

		if err = sendBatchToClientSession(proc.Ctx, data, reg); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrStreamClosed) {
				arg.ctr.remoteReceivers = append(arg.ctr.remoteReceivers[:idx], arg.ctr.remoteReceivers[idx+1:]...)
				arg.ctr.remoteRegsCnt--
				arg.ctr.aliveRegCnt--
				if arg.ctr.aliveRegCnt == 0 {
					arg.ctr.stopSending()
					return nil
				}
				continue
			}
			return err
		}
		arg.ctr.sendCnt++
		return nil
	}
}

func (arg *Argument) sendToAnyReceiver(proc *process.Process, bat *batch.Batch) error {
	toLocal := (arg.ctr.sendCnt % arg.ctr.aliveRegCnt) < arg.ctr.localRegsCnt

	if toLocal {
		if err := arg.sendToAnyLocalReceiver(proc, bat); err != nil {
			return err
		}
		if arg.ctr.isStopSending() {
			arg.ctr.startSending()
			return arg.sendToAnyRemoteReceiver(proc, bat)
		}
	} else {
		if err := arg.sendToAnyRemoteReceiver(proc, bat); err != nil {
			return err
		}
		if arg.ctr.isStopSending() {
			arg.ctr.startSending()
			return arg.sendToAnyLocalReceiver(proc, bat)
		}
	}

	return nil
}

func sendToRegs(arg *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	for i, reg := range arg.LocalRegs {
		if uint32(arg.ShuffleRegIdxLocal[i]) == regIndex {
			if bat.RowCount() != 0 {
				bat.AddCnt(1)
				select {
				case <-proc.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case <-reg.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}

	for _, r := range arg.ctr.remoteReceivers {
		if uint32(arg.ctr.remoteToIdx[r.Uid]) == regIndex {
			if bat.RowCount() != 0 {
				data, err := types.Encode(bat)
				if err != nil {
					return err
				}
				if err = sendBatchToClientSession(proc.Ctx, data, r); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func sendToLocalMatchedRegs(arg *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	cnt := uint32(arg.ctr.localRegsCnt)

	for i, reg := range arg.LocalRegs {
		if regIndex%cnt == uint32(arg.ShuffleRegIdxLocal[i])%cnt {
			if bat.RowCount() != 0 {
				bat.AddCnt(1)
				select {
				case <-proc.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case <-reg.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}

	return nil
}

func sendToAllMatchedRegs(arg *Argument, proc *process.Process, bat *batch.Batch, regIndex uint32) error {
	cnt := uint32(arg.ctr.localRegsCnt)

	for i, reg := range arg.LocalRegs {
		if uint32(arg.ShuffleRegIdxLocal[i])%cnt == regIndex%cnt {
			if bat.RowCount() != 0 {
				bat.AddCnt(1)
				select {
				case <-proc.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case <-reg.Ctx.Done():
					bat.AddCnt(-1)
					arg.ctr.stopSending()
					return nil

				case reg.Ch <- bat:
				}
			}
		}
	}

	for _, r := range arg.ctr.remoteReceivers {
		if uint32(arg.ctr.remoteToIdx[r.Uid])%cnt == regIndex%cnt {
			if bat.RowCount() != 0 {
				data, err := types.Encode(bat)
				if err != nil {
					return err
				}
				if err = sendBatchToClientSession(proc.Ctx, data, r); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (arg *Argument) shuffleToAllReceivers(proc *process.Process, bat *batch.Batch) error {
	if bat == nil {
		return nil
	}
	arg.ctr.batchCnt[bat.ShuffleIDX]++
	arg.ctr.rowCnt[bat.ShuffleIDX] += bat.RowCount()
	if arg.ShuffleType == plan2.ShuffleToRegIndex {
		return sendToRegs(arg, proc, bat, uint32(bat.ShuffleIDX))
	} else if arg.ShuffleType == plan2.ShuffleToLocalMatchedReg {
		return sendToLocalMatchedRegs(arg, proc, bat, uint32(bat.ShuffleIDX))
	} else {
		return sendToAllMatchedRegs(arg, proc, bat, uint32(bat.ShuffleIDX))
	}
}

func dealRefers(
	bat *batch.Batch,
	localReceiverCnt int, isRecSink bool,
	proc *process.Process) (batchList []*batch.Batch, err error) {
	defer func() {
		if err != nil {
			for _, b := range batchList {
				proc.PutBatch(b)
			}
			if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
				jm.SetRef(1)
				jm.SetDupCount(1)
			}
		}
	}()

	// todo: there is a bug about CTE, some operators will change the batch's data even if its
	//       reference cnt is not 1, so we need to copy the batch here
	if isRecSink {
		batchList = make([]*batch.Batch, localReceiverCnt)
		if len(batchList) > 0 {
			bat.AddCnt(1)
			batchList[0] = bat
		}

		mp := proc.Mp()
		for i := 1; i < len(batchList); i++ {
			batchList[i], err = bat.Dup(mp)
			if err != nil {
				return nil, err
			}
		}
		return batchList, err
	}

	bat.AddCnt(localReceiverCnt)
	if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
		// set the reference of join map as the count of local receivers.
		jm.SetRef(int64(localReceiverCnt))
		jm.SetDupCount(int64(localReceiverCnt))
	}

	return batchList, err
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
		return wcs.Cs.Write(ctx, msg)
	}

	for start, end := 0, 0; start < len(encodeBatData); start = end {
		end = start + maxMessageSizeToMoRpc
		sid := pipeline.Status_WaitingNext
		if end >= len(encodeBatData) {
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

		if err := wcs.Cs.Write(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func handleUnsentRefers(
	bat *batch.Batch,
	isRecSink bool,
	localReceiverCnt int, succeedCnt int) {
	if isRecSink {
		return
	}

	cntSub := succeedCnt - localReceiverCnt
	bat.AddCnt(cntSub)
	if jm, ok := bat.AuxData.(*hashmap.JoinMap); ok {
		jm.IncRef(int64(cntSub))
		jm.IncDupCount(int64(cntSub))
	}
}
