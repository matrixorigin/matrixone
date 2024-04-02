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
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/logservice"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const DefaultBatchSize = 8192

// New creates a new Process.
// A process stores the execution context.
func New(
	ctx context.Context,
	m *mpool.MPool,
	txnClient client.TxnClient,
	txnOperator client.TxnOperator,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	aicm *defines.AutoIncrCacheManager) *Process {
	return &Process{
		mp:           m,
		Ctx:          ctx,
		TxnClient:    txnClient,
		TxnOperator:  txnOperator,
		FileService:  fileService,
		IncrService:  incrservice.GetAutoIncrementService(ctx),
		UnixTime:     time.Now().UnixNano(),
		LastInsertID: new(uint64),
		LockService:  lockService,
		Aicm:         aicm,
		vp: &vectorPool{
			vecs:  make(map[uint8][]*vector.Vector),
			Limit: VectorLimit,
		},
		valueScanBatch: make(map[[16]byte]*batch.Batch),
		QueryClient:    queryClient,
		Hakeeper:       hakeeper,
		UdfService:     udfService,
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
	proc.vp = p.vp
	proc.mp = p.Mp()
	proc.prepareBatch = p.prepareBatch
	proc.prepareExprList = p.prepareExprList
	proc.Lim = p.Lim
	proc.TxnClient = p.TxnClient
	proc.TxnOperator = p.TxnOperator
	proc.AnalInfos = p.AnalInfos
	proc.SessionInfo = p.SessionInfo
	proc.FileService = p.FileService
	proc.IncrService = p.IncrService
	proc.QueryClient = p.QueryClient
	proc.Hakeeper = p.Hakeeper
	proc.UdfService = p.UdfService
	proc.UnixTime = p.UnixTime
	proc.LastInsertID = p.LastInsertID
	proc.LockService = p.LockService
	proc.Aicm = p.Aicm
	proc.LoadTag = p.LoadTag
	proc.MessageBoard = p.MessageBoard

	proc.prepareParams = p.prepareParams
	proc.resolveVariableFunc = p.resolveVariableFunc

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
	proc.DispatchNotifyCh = make(chan WrapCs)
	proc.LoadLocalReader = p.LoadLocalReader
	proc.WaitPolicy = p.WaitPolicy
	return proc
}

func (wreg *WaitRegister) CleanChannel(m *mpool.MPool) {
	for len(wreg.Ch) > 0 {
		bat := <-wreg.Ch
		if bat != nil {
			bat.Clean(m)
		}
	}
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
var xxxProcMp = mpool.MustNewNoFixed("fallback_proc_mp")

func (proc *Process) GetMPool() *mpool.MPool {
	if proc == nil {
		return xxxProcMp
	}
	return proc.mp
}

func (proc *Process) Mp() *mpool.MPool {
	return proc.GetMPool()
}

func (proc *Process) GetPrepareParams() *vector.Vector {
	return proc.prepareParams
}

func (proc *Process) SetPrepareParams(prepareParams *vector.Vector) {
	proc.prepareParams = prepareParams
}

func (proc *Process) SetPrepareBatch(bat *batch.Batch) {
	proc.prepareBatch = bat
}

func (proc *Process) GetPrepareBatch() *batch.Batch {
	return proc.prepareBatch
}

func (proc *Process) SetPrepareExprList(exprList any) {
	proc.prepareExprList = exprList
}

func (proc *Process) GetPrepareExprList() any {
	return proc.prepareExprList
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

func (proc *Process) ResetContextFromParent(parent context.Context) context.Context {
	newctx, cancel := context.WithCancel(parent)

	proc.Ctx = newctx
	proc.Cancel = cancel

	for i := range proc.Reg.MergeReceivers {
		proc.Reg.MergeReceivers[i].Ctx = newctx
	}
	return newctx
}

func (proc *Process) GetAnalyze(idx, parallelIdx int, parallelMajor bool) Analyze {
	if idx >= len(proc.AnalInfos) || idx < 0 {
		return &analyze{analInfo: nil, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
	}
	return &analyze{analInfo: proc.AnalInfos[idx], wait: 0, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
}

func (proc *Process) AllocVectorOfRows(typ types.Type, nele int, nsp *nulls.Nulls) (*vector.Vector, error) {
	vec := proc.GetVector(typ)
	err := vec.PreExtend(nele, proc.Mp())
	if err != nil {
		return nil, err
	}
	vec.SetLength(nele)
	if nsp != nil {
		nulls.Set(vec.GetNulls(), nsp)
	}
	return vec, nil
}

func (proc *Process) WithSpanContext(sc trace.SpanContext) {
	proc.Ctx = trace.ContextWithSpanContext(proc.Ctx, sc)
}

func (proc *Process) CopyValueScanBatch(src *Process) {
	proc.valueScanBatch = src.valueScanBatch
}

func (proc *Process) SetVectorPoolSize(limit int) {
	proc.vp.Limit = limit
}

func (proc *Process) CopyVectorPool(src *Process) {
	proc.vp = src.vp
}

func (proc *Process) NewBatchFromSrc(src *batch.Batch, preAllocSize int) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(src.Vecs))
	bat.SetAttributes(src.Attrs)
	bat.Recursive = src.Recursive
	for i := range bat.Vecs {
		v := proc.GetVector(*src.Vecs[i].GetType())
		if v.Capacity() < preAllocSize {
			err := v.PreExtend(preAllocSize, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		bat.Vecs[i] = v
	}
	return bat, nil
}

func (proc *Process) AppendToFixedSizeFromOffset(dst *batch.Batch, src *batch.Batch, offset int) (*batch.Batch, int, error) {
	var err error
	if dst == nil {
		dst, err = proc.NewBatchFromSrc(src, 0)
		if err != nil {
			return nil, 0, err
		}
	}
	if dst.RowCount() >= DefaultBatchSize {
		panic("can't call AppendToFixedSizeFromOffset when batch is full!")
	}
	if len(dst.Vecs) != len(src.Vecs) {
		return nil, 0, moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
	}
	length := DefaultBatchSize - dst.RowCount()
	if length+offset > src.RowCount() {
		length = src.RowCount() - offset
	}
	for i := range dst.Vecs {
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, proc.Mp()); err != nil {
			return dst, 0, err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return dst, length, nil
}

func (proc *Process) PutBatch(bat *batch.Batch) {
	if bat == batch.EmptyBatch {
		return
	}
	if atomic.LoadInt64(&bat.Cnt) == 0 {
		panic("put batch with zero cnt")
	}
	if atomic.AddInt64(&bat.Cnt, -1) > 0 {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			// very large vectors should not put back into pool, which cause these memory can not release.
			// XXX I left the old logic here. But it's unreasonable to use the number of rows to determine if a vector's size.
			// use Allocated() may suitable.
			if vec.IsConst() || vec.NeedDup() || vec.Allocated() > 8192*64 {
				vec.Free(proc.mp)
				bat.ReplaceVector(vec, nil)
				continue
			}

			if !proc.vp.putVector(vec) {
				vec.Free(proc.mp)
			}
			bat.ReplaceVector(vec, nil)
		}
	}
	for _, agg := range bat.Aggs {
		if agg != nil {
			agg.Free(proc.Mp())
		}
	}
	bat.Vecs = nil
	bat.Attrs = nil
	bat.SetRowCount(0)
}

func (proc *Process) FreeVectors() {
	proc.vp.freeVectors(proc.Mp())
}

func (proc *Process) PutVector(vec *vector.Vector) {
	if vec.NeedDup() {
		vec.Free(proc.Mp())
		return
	}
	if !proc.vp.putVector(vec) {
		vec.Free(proc.Mp())
	}
}

func (proc *Process) GetVector(typ types.Type) *vector.Vector {
	if vec := proc.vp.getVector(typ); vec != nil {
		vec.Reset(typ)
		return vec
	}
	return vector.NewVec(typ)
}

func (vp *vectorPool) freeVectors(mp *mpool.MPool) {
	vp.Lock()
	defer vp.Unlock()
	for k, vecs := range vp.vecs {
		for _, vec := range vecs {
			vec.Free(mp)
		}
		delete(vp.vecs, k)
	}
}

func (vp *vectorPool) putVector(vec *vector.Vector) bool {
	vp.Lock()
	defer vp.Unlock()
	key := uint8(vec.GetType().Oid)
	if len(vp.vecs[key]) >= vp.Limit {
		return false
	}
	for _, v := range vp.vecs[key] {
		if v == vec {
			logutil.Errorf("AllocMsg=%s\n\nFreeMsg=%s\n\nGetMsg=%s\n\nPutMsg=%s\n\n",
				v.AllocMsg,
				v.FreeMsg,
				v.GetMsg,
				v.PutMsg,
			)
			panic("+++++++++++  double put")
		}
	}
	vec.PutMsg = time.Now().String() + " : typ=" + vec.GetType().DescString() + " " + string(debug.Stack())
	vp.vecs[key] = append(vp.vecs[key], vec)
	return true
}

func (vp *vectorPool) getVector(typ types.Type) *vector.Vector {
	vp.Lock()
	defer vp.Unlock()
	key := uint8(typ.Oid)
	if vecs := vp.vecs[key]; len(vecs) > 0 {
		vec := vecs[len(vecs)-1]
		vp.vecs[key] = vecs[:len(vecs)-1]
		if !vec.OnUsed {
			logutil.Errorf("AllocMsg=%s\n\nFreeMsg=%s\n\nGetMsg=%s\n\nPutMsg=%s\n\n",
				vec.AllocMsg,
				vec.FreeMsg,
				vec.GetMsg,
				vec.PutMsg,
			)
			panic("+++++++++++  use free vec")
		}
		vec.GetMsg = time.Now().String() + " : " + string(debug.Stack())
		return vec
	}
	return nil
}
