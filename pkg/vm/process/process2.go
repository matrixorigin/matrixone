// Copyright 2024 Matrix Origin
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

// NewTopProcess creates a new top process for the query.
// It is used to store all the query information, including pool, txn, file service, lock service, etc.
//
// Each developer should watch that, do not call this function twice during the query execution.
// Use Process.NewContextChildProc() and Process.NewNoContextChildProc() to create a new child process instead.
//
// The returning Process will hold a top context, which is session-client level context.
// It can be modified by calling Process.ReplaceTopCtx() method, but should be careful to avoid modifying it after the query starts.
//
// There should be announced that the returning Process owns a hack query context field and pipeline context to avoid nil pointer panic.
// These two filed will be really created and refreshed when the pipeline was going to start, by calling process.BuildQueryCtx() and process.BuildPipelineContext() method.
func NewTopProcess(
	topContext context.Context, // this should be a query-lifecycle or session-lifecycle context.
	mp *mpool.MPool,
	txnClient client.TxnClient, txnOperator client.TxnOperator,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	queryClient qclient.QueryClient, HAKeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	autoIncrease *defines.AutoIncrCacheManager) *Process {

	// get needed attributes from input parameters.
	sid := ""
	if lockService != nil {
		sid = lockService.GetConfig().ServiceID
	}

	Base := &BaseProcess{
		sqlContext: QueryBaseContext{
			outerContext: topContext,
		},

		// 1. fields from outer
		mp:          mp,
		TxnClient:   txnClient,
		TxnOperator: txnOperator,
		FileService: fileService,
		IncrService: incrservice.GetAutoIncrementService(sid),
		LockService: lockService,
		Aicm:        autoIncrease,
		QueryClient: queryClient,
		Hakeeper:    HAKeeper,
		UdfService:  udfService,

		// 2. fields from make.
		LastInsertID:   new(uint64),
		valueScanBatch: make(map[[16]byte]*batch.Batch),

		// 3. other fields.
		logger:   util.GetLogger(sid),
		UnixTime: time.Now().UnixNano(),
	}

	proc := &Process{
		Base: Base,
	}
	proc.doPrepareForRunningWithoutPipeline()
	return proc
}

// NewNoContextChildProc make a new child process without a context field.
// This is used for the compile-process, which doesn't need to pass the context.
func (proc *Process) NewNoContextChildProc(dataEntryCount int) *Process {
	child := &Process{
		Base: proc.Base,
	}

	if dataEntryCount > 0 {
		child.Reg.MergeReceivers = make([]*WaitRegister, dataEntryCount)
		for i := range child.Reg.MergeReceivers {
			child.Reg.MergeReceivers[i] = &WaitRegister{
				Ch2: make(chan PipelineSignal, 1),
			}
		}
	}

	// todo: if there is no dispatch operation, we don't need to create the following channel. but OK for now.
	child.DispatchNotifyCh = make(chan *WrapCs)
	return child
}

// NewNoContextChildProc make a new child process without a context field.
// This is used for the compile-process, which doesn't need to pass the context.
func (proc *Process) NewNoContextChildProcWithChannel(dataEntryCount int, channelBufferSize []int32, nilbatchCnt []int32) *Process {
	child := &Process{
		Base: proc.Base,
	}

	if dataEntryCount > 0 {
		child.Reg.MergeReceivers = make([]*WaitRegister, dataEntryCount)
		for i := range child.Reg.MergeReceivers {
			child.Reg.MergeReceivers[i] = &WaitRegister{
				Ch2:         make(chan PipelineSignal, channelBufferSize[i]),
				NilBatchCnt: int(nilbatchCnt[i]),
			}
		}
	}

	// todo: if there is no dispatch operation, we don't need to create the following channel. but OK for now.
	child.DispatchNotifyCh = make(chan *WrapCs)
	return child
}

// NewContextChildProc make a new child and init its context field.
// This is used for parallel execution, which will make a new child process to run a pipeline directly.
// todo: I will remove this method next day, it's a waste to create a new context.
func (proc *Process) NewContextChildProc(dataEntryCount int) *Process {
	child := proc.NewNoContextChildProc(dataEntryCount)
	child.BuildPipelineContext(proc.Ctx)
	return child
}

// BuildPipelineContext cleans the old pipeline context and creates a new one from the input parent context.
func (proc *Process) BuildPipelineContext(parentContext context.Context) context.Context {
	if proc.Cancel != nil {
		proc.Cancel()
	}
	proc.Ctx, proc.Cancel = context.WithCancel(parentContext)
	return proc.Ctx
}

func (proc *Process) GetTopContext() context.Context {
	return proc.Base.sqlContext.outerContext
}

// ReplaceTopCtx sets the new top context.
func (proc *Process) ReplaceTopCtx(topCtx context.Context) {
	proc.Base.sqlContext.outerContext = topCtx
}

// SaveToTopContext for easy access to change the top context.
// it's same to a combined operator list like `GetTopContext() + ReplaceTopCtx() + GetTopContext()`.
func (proc *Process) SaveToTopContext(key, value any) context.Context {
	proc.Base.sqlContext.outerContext = context.WithValue(proc.Base.sqlContext.outerContext, key, value)
	return proc.Base.sqlContext.outerContext
}

// doPrepareForRunningWithoutPipeline will hack the query context and pipeline context for the process.
// It's used for:
// 1.some DDL has no need to build a pipeline.
// 2.call expression evaluation outside the pipeline.
//
// All these situations require the process to hold a Ctx field,
// to avoid large-scale code modifications or many introductions of new `isPipelineStage` flags;
// we support this method to make the process ready for doing any operations without a pipeline.
//
// Everyone should be careful to call this method.
func (proc *Process) doPrepareForRunningWithoutPipeline() {
	proc.Base.sqlContext.queryContext = proc.Base.sqlContext.outerContext
	proc.Ctx = proc.Base.sqlContext.outerContext
}

// GetQueryCtxFromProc returns the query context and its cancel function.
// just for easy access.
func GetQueryCtxFromProc(proc *Process) (context.Context, context.CancelFunc) {
	return proc.Base.sqlContext.queryContext, proc.Base.sqlContext.queryCancel
}

// ReplacePipelineCtx replaces the pipeline context and cancel function for the process.
// It's a very dangerous operation, should be used with caution.
// And we only use it for the newly built pipeline by the pipeline's ParallelRun method.
func ReplacePipelineCtx(proc *Process, ctx context.Context, cancel context.CancelFunc) {
	proc.Ctx = ctx
	proc.Cancel = cancel
}

// GetQueryContextError return error once top context or query context with error.
func (proc *Process) GetQueryContextError() error {
	base := proc.Base.GetContextBase()
	if base.outerContext != nil && base.outerContext.Err() != nil {
		return base.outerContext.Err()
	}
	if base.queryContext != nil && base.queryContext.Err() != nil {
		return base.queryContext.Err()
	}
	return nil
}

// ResetQueryContext cleans the context and cancel function for process reuse.
func (proc *Process) ResetQueryContext() {
	if proc.Base.sqlContext.queryCancel != nil {
		proc.Base.sqlContext.queryCancel()
		proc.Base.sqlContext.queryCancel = nil
	}
	proc.doPrepareForRunningWithoutPipeline()
}

// Free do memory clean for the process.
func (proc *Process) Free() {
	if proc == nil {
		return
	}
	proc.CleanValueScanBatchs()
}

type QueryBaseContext struct {
	// outerContext represents the top context for a query that originates from the session client directly.
	// It encompasses a wealth of information, including accountID, userID and more.
	// Additionally, we use this context to reconstruct the query context once query needs to run or rerun.
	outerContext context.Context

	// queryContext is the parent context for all pipeline contexts, with the queryCancel serving as its cancellation method.
	// we can terminate the whole query by calling this cancel function.
	//
	// Once query was began to run, the query context and query cancel will be refreshed by calling BuildQueryCtx() method.
	queryContext context.Context
	queryCancel  context.CancelFunc

	// TODO: this is a hack here for resolving pipeline loop of recursive cte.
	// 	we do a special clean-up for the first return pipeline in this pipeline-loop to get rid of deadlock.
	//
	// if pipelineLoopBreak is true, this means no need to do special cleanup yet.
	sync.RWMutex
	pipelineLoopBreak bool
	isOnMergeCTE      bool
}

func (bp *BaseProcess) GetContextBase() *QueryBaseContext {
	return &bp.sqlContext
}

func (qbCtx *QueryBaseContext) DoSpecialCleanUp(isMergeCTE bool) bool {
	qbCtx.Lock()
	defer qbCtx.Unlock()

	if qbCtx.pipelineLoopBreak {
		return qbCtx.isOnMergeCTE == isMergeCTE
	}
	qbCtx.pipelineLoopBreak = true
	qbCtx.isOnMergeCTE = isMergeCTE
	return true
}

// BuildQueryCtx refreshes the query context and cancellation method after the outer context was ready to run the query.
func (qbCtx *QueryBaseContext) BuildQueryCtx() context.Context {
	qbCtx.queryContext, qbCtx.queryCancel = context.WithCancel(qbCtx.outerContext)

	qbCtx.Lock()
	qbCtx.pipelineLoopBreak = false
	qbCtx.Unlock()
	return qbCtx.queryContext
}

// SaveToQueryContext saves the key-value pair to the query context.
// Every pipeline context can access the key-value pair by calling its own context.Value() method.
// But should be careful to avoid adding key-value pairs after the pipeline context has been created.
func (qbCtx *QueryBaseContext) SaveToQueryContext(key, value any) context.Context {
	qbCtx.queryContext = context.WithValue(qbCtx.queryContext, key, value)
	return qbCtx.queryContext
}

// WithCounterSetToQueryContext sets the counter set to the query context.
func (qbCtx *QueryBaseContext) WithCounterSetToQueryContext(sets ...*perfcounter.CounterSet) context.Context {
	qbCtx.queryContext = perfcounter.WithCounterSet(qbCtx.queryContext, sets...)
	return qbCtx.queryContext
}

// PutBatch updates the reference count of the batch.
// when this batch is no longer in use, places all vectors into the pool.
func (proc *Process) PutBatch(bat *batch.Batch) {
	// situations that batch was still in use.
	// we use `!= 0` but not `>0` to avoid the situation that the batch was cleaned more than required.
	if bat == batch.EmptyBatch || atomic.AddInt64(&bat.Cnt, -1) != 0 {
		return
	}

	for i, vec := range bat.Vecs {
		if vec != nil {
			bat.ReplaceVector(vec, nil, i)
			vec.Free(proc.GetMPool())
		}
	}
	for _, agg := range bat.Aggs {
		if agg != nil {
			agg.Free()
		}
	}
	bat.Aggs = nil
	bat.Vecs = nil
	bat.Attrs = nil
	bat.SetRowCount(0)
}
