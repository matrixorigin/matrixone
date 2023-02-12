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
	"fmt"
	"sync/atomic"

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

	SendToAllFunc = iota
	SendToAnyFunc
)

// common sender: send to all receiver
func sendToAllFunc(bat *batch.Batch, bid int64, localReceivers []*process.WaitRegister, remoteReceivers []*WrapperClientSession, proc *process.Process) error {
	if remoteReceivers != nil {
		encodeData, errEncode := types.Encode(bat)
		if errEncode != nil {
			return errEncode
		}
		for _, r := range remoteReceivers {
			if err := sendBatchToClientSession(encodeData, bid, r); err != nil {
				return err
			}
		}
	}

	refCountAdd := int64(len(localReceivers) - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(len(localReceivers)))
	}

	for _, reg := range localReceivers {
		select {
		case <-reg.Ctx.Done():
			return moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
		case reg.Ch <- bat:
		}
	}

	return nil
}

// common sender: send to any receiver
func sendToAnyFunc(bat *batch.Batch, bid int64, localReceivers []*process.WaitRegister, remoteReceivers []*WrapperClientSession, proc *process.Process) error {
	// bid is a continuously increasing number
	// so use it to decide which chan to send.
	//
	// Treate all reg like: [localReg0, localReg1, localReg2, remoteReg0, remoteReg1]
	// And choose one from then by mod Bid with its length
	localLen := int64(len(localReceivers))
	remoteLen := int64(len(remoteReceivers))
	sendId := bid % (localLen + remoteLen)

	// send to remote receiver
	if sendId >= localLen {
		sendId = sendId - localLen
		encodeData, errEncode := types.Encode(bat)
		if errEncode != nil {
			return errEncode
		}
		if err := sendBatchToClientSession(encodeData, bid, remoteReceivers[sendId]); err != nil {
			return err
		}
		return nil
	}
	fmt.Printf("[dispatchdispatch] send to any with sendId = %d\n", sendId)

	// send to local receiver
	reg := localReceivers[sendId]
	select {
	case <-reg.Ctx.Done():
		fmt.Printf("[dispatchdispatch] local[%d] done.\n", sendId)
		for len(reg.Ch) > 0 { // free memory
			bat := <-reg.Ch
			if bat == nil {
				break
			}
			bat.Clean(proc.Mp())
		}
		// TODO: how to remove the done-receiver?
		// localReceivers = append(localReceivers[:sendId], localReceivers[sendId+1:]...)
		return nil
	case reg.Ch <- bat:
	}

	return nil
}

func sendBatchToClientSession(encodeBatData []byte, bid int64, wcs *WrapperClientSession) error {
	fmt.Printf("[dispatchdispatch] write msg into cs ... uuid = %s\n", wcs.uuid)
	if len(encodeBatData) <= maxMessageSizeToMoRpc {
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = pipeline.BatchEnd
		}
		if err := wcs.cs.Write(wcs.ctx, msg); err != nil {
			fmt.Printf("[dispatchdispatch] write whole message (%d, %s) in cs %p failed\n", bid, wcs.uuid, &wcs.cs)
			return err
		}
		fmt.Printf("[dispatchdispatch] write whole message (%d, %s) in cs %p success\n", bid, wcs.uuid, &wcs.cs)
		return nil
	}

	start := 0
	for start < len(encodeBatData) {
		end := start + maxMessageSizeToMoRpc
		sid := pipeline.BatchWaitingNext
		if end > len(encodeBatData) {
			end = len(encodeBatData)
			sid = pipeline.BatchEnd
		}
		msg := cnclient.AcquireMessage()
		{
			msg.Id = wcs.msgId
			msg.Data = encodeBatData[start:end]
			msg.Cmd = pipeline.BatchMessage
			msg.Sid = uint64(sid)
		}

		if err := wcs.cs.Write(wcs.ctx, msg); err != nil {
			fmt.Printf("[dispatchdispatch] write part message (%d, %s) in cs %p failed\n", bid, wcs.uuid, &wcs.cs)
			return err
		}
		fmt.Printf("[dispatchdispatch] write part message (%d, %s) in cs %p success\n", bid, wcs.uuid, &wcs.cs)
		start = end
	}
	return nil
}
