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

func NewWithAnalyze(p *Process, ctx context.Context, regNumber int, anals []*AnalyzeInfo) *Process {
	proc := NewFromProc(p, ctx, regNumber)
	proc.AnalInfos = make([]*AnalyzeInfo, len(anals))
	copy(proc.AnalInfos, anals)
	return proc
}

// NewFromProc create a new Process based on another process.
func NewFromProc(p *Process, ctx context.Context, regNumber int) *Process {
	proc := new(Process)
	newctx, cancel := context.WithCancel(ctx)
	proc.Id = p.Id
	proc.Mp = p.Mp
	proc.Lim = p.Lim
	proc.Snapshot = p.Snapshot
	proc.AnalInfos = p.AnalInfos
	proc.SessionInfo = p.SessionInfo

	// reg and cancel
	proc.Cancel = cancel
	proc.Reg.MergeReceivers = make([]*WaitRegister, regNumber)
	for i := 0; i < regNumber; i++ {
		proc.Reg.MergeReceivers[i] = &WaitRegister{
			Ctx: newctx,
			Ch:  make(chan *batch.Batch, 1),
		}
	}
	return proc
}

func (wreg *WaitRegister) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (wreg *WaitRegister) UnmarshalBinary(_ []byte) error {
	return nil
}

func (proc *Process) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (proc *Process) UnmarshalBinary(_ []byte) error {
	return nil
}

func (proc *Process) QueryId() string {
	return proc.Id
}

func (proc *Process) SetQueryId(id string) {
	proc.Id = id
}

func (proc *Process) GetMheap() *mheap.Mheap {
	return proc.Mp
}

func (proc *Process) OperatorOutofMemory(size int64) bool {
	return proc.Lim.Size < size
}

func (proc *Process) SetInputBatch(bat *batch.Batch) {
	proc.Reg.InputBatch = bat
}

func (proc *Process) InputBatch() *batch.Batch {
	return proc.Reg.InputBatch
}

func (proc *Process) GetAnalyze(idx int) Analyze {
	if idx >= len(proc.AnalInfos) {
		return &analyze{analInfo: nil}
	}
	return &analyze{analInfo: proc.AnalInfos[idx]}
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

func (proc *Process) AllocBoolScalarVector(v bool) *vector.Vector {
	typ := types.T_bool.ToType()
	vec := proc.AllocScalarVector(typ)
	bvec := make([]bool, 1)
	bvec[0] = v
	vec.Col = bvec
	return vec
}

func (proc *Process) AllocInt64ScalarVector(v int64) *vector.Vector {
	typ := types.T_int64.ToType()
	vec := proc.AllocScalarVector(typ)
	ivec := make([]int64, 1)
	ivec[0] = v
	vec.Col = ivec
	return vec
}
