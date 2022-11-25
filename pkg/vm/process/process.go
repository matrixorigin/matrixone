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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// New creates a new Process.
// A process stores the execution context.
func New(
	ctx context.Context,
	m *mpool.MPool,
	txnClient client.TxnClient,
	txnOperator client.TxnOperator,
	fileService fileservice.FileService,
	getClusterDetails engine.GetClusterDetailsFunc,
) *Process {
	return &Process{
		mp:                m,
		Ctx:               ctx,
		TxnClient:         txnClient,
		TxnOperator:       txnOperator,
		FileService:       fileService,
		GetClusterDetails: getClusterDetails,
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
	proc.mp = p.Mp()
	proc.Lim = p.Lim
	proc.TxnClient = p.TxnClient
	proc.TxnOperator = p.TxnOperator
	proc.AnalInfos = p.AnalInfos
	proc.SessionInfo = p.SessionInfo
	proc.FileService = p.FileService
	proc.GetClusterDetails = p.GetClusterDetails

	// reg and cancel
	proc.Ctx = newctx
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

// XXX MPOOL
// Some times we call an expr eval function without a proc (test only?)
// in that case, all expr eval code get an nil mp which is wrong.
// so far the most cases come from
// plan.ConstantFold -> colexec.EvalExpr, busted.
// hack in a fall back mpool.  This is by design a Zero MP so that there
// will not be real leaks, except we leak counters in globalStats
var xxxProcMp = mpool.MustNewZeroWithTag("fallback_proc_mp")

func (proc *Process) GetMPool() *mpool.MPool {
	if proc == nil {
		return xxxProcMp
	}
	return proc.mp
}

func (proc *Process) Mp() *mpool.MPool {
	return proc.GetMPool()
}

func (proc *Process) OperatorOutofMemory(size int64) bool {
	return proc.Mp().Cap() < size
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
	return proc.AllocVectorOfRows(typ, size/int64(typ.TypeSize()), nil)
}

func (proc *Process) AllocVectorOfRows(typ types.Type, nele int64, nsp *nulls.Nulls) (*vector.Vector, error) {
	vec := vector.New(typ)
	vector.PreAlloc(vec, int(nele), int(nele), proc.Mp())
	if nsp != nil {
		nulls.Set(vec.Nsp, nsp)
	}
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

func (proc *Process) WithSpanContext(sc trace.SpanContext) {
	proc.Ctx = trace.ContextWithSpanContext(proc.Ctx, sc)
}
