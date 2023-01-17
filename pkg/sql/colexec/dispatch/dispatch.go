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
	"sync/atomic"

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
	if ap.crossCN {
		ap.ctr.streams = make([]*WrapperStream, 0, len(ap.nodes))
		for i := range ap.ctr.streams {
			if ap.nodes[i].Node.Addr == "" {
				ap.ctr.streams = append(ap.ctr.streams, nil)
				continue
			}
			stream, errStream := cnclient.GetStreamSender(ap.nodes[i].Node.Addr)
			if errStream != nil {
				return errStream
			}
			ap.ctr.streams = append(ap.ctr.streams, &WrapperStream{stream, ap.nodes[i].Uuid})
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if ap.crossCN {
		if bat == nil {
			return true, nil
		}
		if err := ap.sendFunc(ap.ctr.streams, ap.localIndex, bat, ap.Regs[ap.localIndex], proc); err != nil {
			return false, err
		}
		return false, nil
	}
	if bat == nil {
		return true, nil
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
		refCountAdd := int64(len(ap.Regs) - 1)
		atomic.AddInt64(&bat.Cnt, refCountAdd)
		if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
			jm.IncRef(refCountAdd)
			jm.SetDupCount(int64(len(ap.Regs)))
		}

		for _, reg := range ap.Regs {
			select {
			case <-reg.Ctx.Done():
				return false, moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
			case reg.Ch <- bat:
			}
		}
		return false, nil
	}
	// send to any one
	for len(ap.Regs) > 0 {
		if ap.ctr.i == len(ap.Regs) {
			ap.ctr.i = 0
		}
		reg := ap.Regs[ap.ctr.i]
		select {
		case <-reg.Ctx.Done():
			for len(reg.Ch) > 0 { // free memory
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				bat.Clean(proc.Mp())
			}
			ap.Regs = append(ap.Regs[:ap.ctr.i], ap.Regs[ap.ctr.i+1:]...)
			if ap.ctr.i >= len(ap.Regs) {
				ap.ctr.i = 0
			}
		case reg.Ch <- bat:
			proc.SetInputBatch(nil)
			ap.ctr.i++
			return false, nil
		}
	}
	return true, nil
}

func CloseStreams(streams []*WrapperStream, localIndex uint64, proc *process.Process) error {
	for i := range streams {
		if i == int(localIndex) {
			continue
		} else {
			message := cnclient.AcquireMessage()
			message.Id = streams[i].Stream.ID()
			message.Cmd = 1
			message.Sid = pipeline.MessageEnd
			message.Uuid = streams[i].Uuid[:]
			if err := streams[i].Stream.Send(proc.Ctx, message); err != nil {
				return err
			}
		}
	}
	for i := range streams {
		if streams[i] == nil {
			continue
		}
		if err := streams[i].Stream.Close(); err != nil {
			return err
		}
	}
	return nil
}
