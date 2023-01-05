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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type WrapperStream struct {
	Stream morpc.Stream
	Uuid   uuid.UUID
}
type container struct {
	i       int
	streams []*WrapperStream
}

type Argument struct {
	ctr  *container
	All  bool // dispatch batch to each consumer
	Regs []*process.WaitRegister

	// crossCN is used to treat dispatch operator as a distributed operator
	crossCN bool
	// nodes[IBucket].Node.Address == ""
	nodes      []colexec.WrapperNode
	localIndex uint64
	// streams is the stream which connect local CN with remote CN, so
	// but streams[localIndex] is nil, because you need to send batch locally
	// by localChan
	sendFunc func(streams []*WrapperStream, localIndex uint64, bat *batch.Batch, localChan *process.WaitRegister, proc *process.Process) error
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.crossCN {
		arg.FreeCrossCN(proc, pipelineFailed)
		return
	}

	if pipelineFailed {
		for i := range arg.Regs {
			for len(arg.Regs[i].Ch) > 0 {
				bat := <-arg.Regs[i].Ch
				if bat == nil {
					break
				}
				bat.Clean(proc.Mp())
			}
		}
	}

	for i := range arg.Regs {
		select {
		case <-arg.Regs[i].Ctx.Done():
		case arg.Regs[i].Ch <- nil:
		}
		close(arg.Regs[i].Ch)
	}
}

func (arg *Argument) FreeCrossCN(proc *process.Process, pipelineFailed bool) {
	// closeStreams will send nil to reciever and close streams
	// but it won't send nil to localChan, because when pipelineFailed,
	// and our chan buffer size is only one, if we send a nil,it will result dead lock.
	CloseStreams(arg.ctr.streams, arg.localIndex, proc)
	if pipelineFailed {
		for len(arg.Regs[arg.localIndex].Ch) > 0 {
			bat := <-arg.Regs[arg.localIndex].Ch
			if bat == nil {
				break
			}
			bat.Clean(proc.Mp())
		}
	}

	for i := range arg.Regs {
		// only localChan need to send nil, other chans will never be used,
		// so ignore them
		if i == int(arg.localIndex) {
			select {
			case <-arg.Regs[i].Ctx.Done():
			case arg.Regs[i].Ch <- nil:
			}
		}
		// every chan need to close no matter whether it will be used
		close(arg.Regs[i].Ch)
	}
}
