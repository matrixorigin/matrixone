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

package process

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// New creates a new Process.
// A process stores the execution context.
func New(m *mheap.Mheap) *Process {
	return &Process{
		Mp: m,
	}
}

// NewFromProc create a new Process based on another process.
func NewFromProc(m *mheap.Mheap, p *Process, regNumber int) *Process {
	proc := &Process{Mp: m}
	ctx, cancel := context.WithCancel(context.Background())
	proc.Id = p.Id
	proc.Lim = p.Lim
	proc.Snapshot = p.Snapshot
	proc.AnalInfos = p.AnalInfos
	proc.SessionInfo = p.SessionInfo

	// reg and cancel
	proc.Cancel = cancel
	proc.Reg.MergeReceivers = make([]*WaitRegister, regNumber)
	for i := 0; i < regNumber; i++ {
		proc.Reg.MergeReceivers[i] = &WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
	}
	return proc
}

func GetSels(proc *Process) []int64 {
	if len(proc.Reg.Ss) == 0 {
		return make([]int64, 0, 16)
	}
	sels := proc.Reg.Ss[0]
	proc.Reg.Ss = proc.Reg.Ss[1:]
	return sels[:0]
}

func PutSels(sels []int64, proc *Process) {
	proc.Reg.Ss = append(proc.Reg.Ss, sels)
}

func (proc *Process) OperatorMemoryLimit() int64 {
	return proc.Lim.Size
}

func (proc *Process) SetInputBatch(bat *batch.Batch) {
	proc.Reg.InputBatch = bat
}

func (proc *Process) InputBatch() *batch.Batch {
	return proc.Reg.InputBatch
}

func (proc *Process) GetSels() []int64 {
	if len(proc.Reg.Ss) == 0 {
		return make([]int64, 0, 16)
	}
	sels := proc.Reg.Ss[0]
	proc.Reg.Ss = proc.Reg.Ss[1:]
	return sels[:0]
}

func (proc *Process) GetAnalyze(idx int) Analyze {
	if idx >= len(proc.AnalInfos) {
		return &analyze{analInfo: nil}
	}
	return &analyze{analInfo: proc.AnalInfos[idx]}
}

func (proc *Process) PutSels(sels []int64) {
	proc.Reg.Ss = append(proc.Reg.Ss, sels)
}

func (proc *Process) GetBoolTyp(typ types.Type) (typ2 types.Type) {
	typ.Oid = types.T_bool
	return typ
}

func (proc *Process) AllocVector(typ types.Type, size int64) (*vector.Vector, error) {
	data, err := mheap.Alloc(proc.Mp, size)
	if err != nil {
		return nil, err
	}
	vec := vector.New(typ)
	vec.Data = data[:size]
	return vec, nil
}

func (proc *Process) AllocScalarVector(typ types.Type) *vector.Vector {
	return vector.NewConst(typ, 1)
}

func (proc *Process) AllocScalarNullVector(typ types.Type) *vector.Vector {
	vec := vector.NewConst(typ, 1)
	nulls.Add(vec.Nsp, 0)
	return vec
}

func (proc *Process) AllocConstNullVector(typ types.Type, cnt int) *vector.Vector {
	vec := vector.NewConstNull(typ, cnt)
	nulls.Add(vec.Nsp, 0)
	return vec
}
