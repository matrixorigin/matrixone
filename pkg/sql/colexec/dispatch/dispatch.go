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
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	if ap.CrossCN {
		fmt.Printf("Prepare cross-cn dispatch, length of RemoteReg = %d, proc = %p\n", len(ap.RemoteRegs), proc)
		ap.ctr.streams = make([]*WrapperStream, 0, len(ap.RemoteRegs))
		ap.ctr.c = make([]context.Context, 0, len(ap.RemoteRegs))
		for i := range ap.RemoteRegs {
			stream, errStream := cnclient.GetStreamSender(ap.RemoteRegs[i].NodeAddr)
			if errStream != nil {
				return errStream
			}
			fmt.Printf("stream[%d] get success. streamid = %d\n", i, stream.ID())
			ap.ctr.streams = append(ap.ctr.streams, &WrapperStream{
				Stream: stream,
				Uuids:  ap.RemoteRegs[i].Uuids})
			cnt := make([]uint, len(ap.RemoteRegs[i].Uuids))
			ap.ctr.cnts = append(ap.ctr.cnts, cnt)

			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			_ = cancel
			ap.ctr.c = append(ap.ctr.c, timeoutCtx)
		}
	} else {
		fmt.Printf("Prepare normal dispatch, length of LocalReg = %d, proc = %p\n", len(ap.LocalRegs), proc)
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	if ap.CrossCN {
		if err := ap.SendFunc(ap.ctr.streams, bat, ap.LocalRegs, ap.ctr.c, ap.ctr.cnts, proc); err != nil {
			return false, err
		}
		return false, nil
	}

	// source vectors should be cloned and instead before it was sent.
	for i, vec := range bat.Vecs {
		if vec.IsOriginal() {
			cloneVec, err := vector.Dup(vec, proc.Mp())
			if err != nil {
				bat.Clean(proc.Mp())
				return false, err
			}
			bat.Vecs[i] = cloneVec
		}
	}

	// send to each one
	if ap.All {
		refCountAdd := int64(len(ap.LocalRegs) - 1)
		atomic.AddInt64(&bat.Cnt, refCountAdd)
		if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
			jm.IncRef(refCountAdd)
			jm.SetDupCount(int64(len(ap.LocalRegs)))
		}

		for i, reg := range ap.LocalRegs {
			select {
			case <-reg.Ctx.Done():
				return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
			case reg.Ch <- bat:
				fmt.Printf("[dispatch] send to all. ch[%d] done. proc = %p\n", i, proc)
			}
		}
		fmt.Printf("[dispatch] send to all done. proc = %p\n", proc)
		return false, nil
	}
	// send to any one
	for len(ap.LocalRegs) > 0 {
		if ap.ctr.i == len(ap.LocalRegs) {
			ap.ctr.i = 0
		}
		reg := ap.LocalRegs[ap.ctr.i]
		select {
		case <-reg.Ctx.Done():
			for len(reg.Ch) > 0 { // free memory
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				bat.Clean(proc.Mp())
			}
			ap.LocalRegs = append(ap.LocalRegs[:ap.ctr.i], ap.LocalRegs[ap.ctr.i+1:]...)
			if ap.ctr.i >= len(ap.LocalRegs) {
				ap.ctr.i = 0
			}
		case reg.Ch <- bat:
			proc.SetInputBatch(nil)
			fmt.Printf("[dispatch] send to ch[%d] donw. proc = %p\n", ap.ctr.i, proc)
			ap.ctr.i++
			return false, nil
		}
	}
	return true, nil
}

func CloseStreams(streams []*WrapperStream, proc *process.Process, ctr container) error {
	fmt.Printf("[CloseStreams] close streams. stream len = %d\n", len(streams))
	c, cancel := context.WithTimeout(context.Background(), time.Second*10000)
	_ = cancel

	for i, stream := range streams {
		if len(stream.Uuids) == 0 {
			fmt.Printf("no uuid in stream[%d]\n", i)
			return moerr.NewInternalErrorNoCtx("no uuid in stream[%d]", i)
		}
		for j, uuid := range stream.Uuids {
			message := cnclient.AcquireMessage()
			{
				message.Id = streams[i].Stream.ID()
				message.Cmd = 12345
				message.Sid = pipeline.DirectBatchEndMessage
				message.Uuid = uuid[:]
				message.BatchCnt = uint64(ctr.cnts[i][j])
			}
			fmt.Printf("[CloseStreams] uuid %s close message begin to send. batCnt = %d\n", uuid, ctr.cnts[i][j])
			if err := streams[i].Stream.Send(c, message); err != nil {
				fmt.Printf("[CloseStreams] uuid %s close message send failed: %s\n", uuid, err)
				return err
			}

			fmt.Printf("[CloseStreams] uuid %s close message send\n", uuid)
		}
	}
	for i := range streams {
		if err := streams[i].Stream.Close(); err != nil {
			fmt.Printf("[CloseStreams] stream[%d] close failed. err: %s\n", i, err)
			return err
		}
		fmt.Printf("[CloseStreams] stream[%d] close success\n", i)
	}
	return nil
}
