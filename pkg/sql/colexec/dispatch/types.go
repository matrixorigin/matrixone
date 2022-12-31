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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
	// for deletion operation, we need to ensure every Block
	// will be deleted by only one CN, so We need to use bucket
	// to process this

	// crossCN is used to treat dispatch operator as a distributed operator
	crossCN bool
	IBucket uint64
	// NBucket == len(nodes)
	NBucket uint64
	// nodes[IBucket].Node.Address == ""
	nodes    []engine.WrapperNode
	sendFunc func(streams []*WrapperStream, IBucket uint64, NBucket uint64, bat *batch.Batch, localChan *process.WaitRegister, proc *process.Process) error
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
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
