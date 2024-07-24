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
	"time"
)

// NewTopProcess creates a new top process for the query.
// It is used to store all the query information, including pool, txn, file service, lock service, etc.
//
// Each developer should watch that, do not call this function twice during the query execution.
// Use Process.NewContextChildProc() and Process.NewNoContextChildProc() to create a new child process instead.
//
// the returning Process will hold a top context, which is session-client level context.
// It can be modified by calling Process.ReplaceTopCtx() method, but should be careful to avoid modifying it after the query starts.
//
// There should be announced that the returning Process owns an empty query context field.
// This will be created and refreshed by calling Process.BuildQueryCtx() method.
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
		vp:             initCachedVectorPool(),
		valueScanBatch: make(map[[16]byte]*batch.Batch),

		// 3. other fields.
		logger:   util.GetLogger(sid),
		UnixTime: time.Now().UnixNano(),
	}

	return &Process{
		Base: Base,
	}
}

// NewNoContextChildProc make a new child process without context field.
// This is used for the compile process, which doesn't need to pass the context.
func (proc *Process) NewNoContextChildProc(dataEntryCount int) *Process {
	child := &Process{
		Base: proc.Base,
	}

	if dataEntryCount > 0 {
		child.Reg.MergeReceivers = make([]*WaitRegister, dataEntryCount)
		for i := range child.Reg.MergeReceivers {
			child.Reg.MergeReceivers[i] = &WaitRegister{
				Ch: make(chan *RegisterMessage, 1),
			}
		}
	}

	// todo: if there is no dispatch operation, we don't need to create the following channel. but OK for now.
	child.DispatchNotifyCh = make(chan *WrapCs)
	return child
}

// NewContextChildProc make a new child and init its context field.
// This is used for parallel execution, which will make a new child process to run a pipeline directly.
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

	// update the context held by this process's data producers.
	mp := proc.Mp()
	for _, sender := range proc.Reg.MergeReceivers {
		sender.Ctx = proc.Ctx
		sender.CleanChannel(mp)
	}
	return proc.Ctx
}

func (proc *Process) GetLatestContext() context.Context {
	if proc.Ctx != nil {
		return proc.Ctx
	}
	return proc.Base.sqlContext.getLatestContext()
}

func (proc *Process) GetTopContext() context.Context {
	return proc.Base.sqlContext.outerContext
}

// GetQueryCtxFromProc returns the query context and its cancel function.
// just for easy access.
func GetQueryCtxFromProc(proc *Process) (context.Context, context.CancelFunc) {
	return proc.Base.sqlContext.queryContext, proc.Base.sqlContext.queryCancel
}

// GetErrorFromQueryStatus return error if top context or query context with error.
func (proc *Process) GetErrorFromQueryStatus() error {
	base := proc.Base.GetContextBase()
	if base.outerContext != nil && base.outerContext.Err() != nil {
		return base.outerContext.Err()
	}
	if base.queryContext != nil && base.queryContext.Err() != nil {
		return base.queryContext.Err()
	}
	return nil
}

// Free cleans the process.
// todo: consider to use it instead of other method like `FreeVectors` next day.
func (proc *Process) Free() {
	for _, bat := range proc.GetValueScanBatchs() {
		if bat != nil {
			bat.Clean(proc.Base.mp)
		}
	}

	// we should free the vector pool at the end,
	// avoid any free action before will put memories into the pool.
	proc.Base.vp.free(proc.Base.mp)
}

type QueryBaseContext struct {
	// outerContext represents the top context for a query that originates from the session client directly.
	// It encompasses a wealth of information, including accountID, userID and more.
	// Additionally, we use this context to reconstruct the query context once query needs rerun due to specific errors.
	outerContext context.Context

	// queryContext is the parent context for all pipeline contexts, with the queryCancel serving as its cancellation method.
	// we can terminate the whole query by calling this cancel function.
	//
	// This context was generated at the beginning of each query preparation to create a pipeline from the plan tree.
	// In the case of a prepared SQL statement, the generation of this context and cancellation method precedes the execution of the pipeline.
	queryContext context.Context
	queryCancel  context.CancelFunc
}

func (bp *BaseProcess) GetContextBase() *QueryBaseContext {
	return &bp.sqlContext
}

func (qbCtx *QueryBaseContext) GetTopCtx() context.Context {
	return qbCtx.outerContext
}

// ReplaceTopCtx sets the new top context.
func (qbCtx *QueryBaseContext) ReplaceTopCtx(topCtx context.Context) {
	qbCtx.outerContext = topCtx
}

// SaveToTopContext for easy access to change the top context.
func (qbCtx *QueryBaseContext) SaveToTopContext(key, value any) context.Context {
	qbCtx.outerContext = context.WithValue(qbCtx.outerContext, key, value)
	return qbCtx.outerContext
}

// BuildQueryCtx refreshes the query context and cancellation method after the outer context was ready to run the query.
func (qbCtx *QueryBaseContext) BuildQueryCtx() context.Context {
	qbCtx.queryContext, qbCtx.queryCancel = context.WithCancel(qbCtx.outerContext)
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

func (qbCtx *QueryBaseContext) getLatestContext() context.Context {
	if qbCtx.queryContext != nil {
		return qbCtx.queryContext
	}
	return qbCtx.outerContext
}
