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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type WrapperStream struct {
	Stream morpc.Stream
	Uuids  []uuid.UUID
}
type container struct {
	i       int
	streams []*WrapperStream

	c []context.Context

	cnts [][]uint
}

type Argument struct {
	ctr *container
	// dispatch batch to each consumer
	All bool
	// CrossCN is used to treat dispatch operator as a distributed operator
	CrossCN bool

	// LocalRegs means the local register you need to send to.
	LocalRegs []*process.WaitRegister

	// RemoteRegs specific the remote reg you need to send to.
	// RemoteRegs[IBucket].Node.Address == ""
	RemoteRegs []colexec.WrapperNode

	// streams is the stream which connect local CN with remote CN.
	SendFunc func(streams []*WrapperStream, bat *batch.Batch, localChans []*process.WaitRegister, ctxs []context.Context, cnts [][]uint, proc *process.Process) error
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.CrossCN {
		CloseStreams(arg.ctr.streams, proc, *arg.ctr)
	}

	if pipelineFailed {
		for i := range arg.LocalRegs {
			for len(arg.LocalRegs[i].Ch) > 0 {
				bat := <-arg.LocalRegs[i].Ch
				if bat == nil {
					break
				}
				bat.Clean(proc.Mp())
			}
		}
	}

	for i := range arg.LocalRegs {
		select {
		case <-arg.LocalRegs[i].Ctx.Done():
		case arg.LocalRegs[i].Ch <- nil:
		}
		close(arg.LocalRegs[i].Ch)
	}
}
