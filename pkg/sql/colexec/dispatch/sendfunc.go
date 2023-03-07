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

	SendToAllFunc = iota
	SendToAllLocalFunc
	SendToAnyLocalFunc
)

// common sender: send to any LocalReceiver
func sendToAllLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	refCountAdd := int64(len(ap.LocalRegs) - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(len(ap.LocalRegs)))
	}

	for _, reg := range ap.LocalRegs {
		select {
		case <-reg.Ctx.Done():
			return moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
		case reg.Ch <- bat:
		}
	}

	return nil
}

// common sender: send to any LocalReceiver
func sendToAnyLocalFunc(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	// send to local receiver
	sendto := ap.sendCnt % len(ap.LocalRegs)
	reg := ap.LocalRegs[sendto]
	select {
	case <-reg.Ctx.Done():
		for len(reg.Ch) > 0 { // free memory
			bat := <-reg.Ch
			if bat == nil {
				break
			}
			bat.Clean(proc.Mp())
		}
		ap.LocalRegs = append(ap.LocalRegs[:sendto], ap.LocalRegs[sendto+1:]...)
		return nil
	case reg.Ch <- bat:
		ap.sendCnt++
	}

	return nil
}

// common sender: send to all receiver
func sendToAllFunc(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	if !ap.prepared {
		ap.waitRemoteRegsReady(proc)
	}

	{ // send to remote regs
		encodeData, errEncode := types.Encode(bat)
		if errEncode != nil {
			return errEncode
		}
		for _, r := range ap.ctr.remoteReceivers {
			if err := sendBatchToClientSession(encodeData, r); err != nil {
				return err
			}
		}
	}

	refCountAdd := int64(len(ap.LocalRegs) - 1)
	atomic.AddInt64(&bat.Cnt, refCountAdd)
	if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
		jm.IncRef(refCountAdd)
		jm.SetDupCount(int64(len(ap.LocalRegs)))
	}

	for _, reg := range ap.LocalRegs {
		select {
		case <-reg.Ctx.Done():
			return moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
		case reg.Ch <- bat:
		}
	}

	return nil
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
