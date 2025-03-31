// Copyright 2022 Matrix Origin
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

package frontend

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var CDCExectorError_QueryDaemonTaskTimeout = moerr.NewInternalErrorNoCtx("query daemon task timeout")

var CDCExeutorAllocator *mpool.MPool

func init() {
	var err error
	mpool.DeleteMPool(CDCExeutorAllocator)
	if CDCExeutorAllocator, err = mpool.NewMPool("cdc_executor", 0, mpool.NoFixed); err != nil {
		panic(err)
	}
}

func CDCTaskExecutorFactory(
	logger *zap.Logger,
	sqlExecutorFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cnUUID string,
	ts taskservice.TaskService,
	fs fileservice.FileService,
	txnClient client.TxnClient,
	txnEngine engine.Engine,
) taskservice.TaskExecutor {
	return func(ctx context.Context, spec task.Task) error {
		ctx1, cancel := context.WithTimeoutCause(
			ctx, time.Second*5, CDCExectorError_QueryDaemonTaskTimeout,
		)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(
			ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, spec.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalErrorf(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}

		cdcTask := NewCdcTask(
			logger,
			sqlExecutorFactory(),
			details.CreateCdc,
			cnUUID,
			fs,
			txnClient,
			txnEngine,
			CDCExeutorAllocator,
		)
		cdcTask.activeRoutine = cdc.NewCdcActiveRoutine()
		if err = attachToTask(ctx, spec.GetID(), cdcTask); err != nil {
			return err
		}
		return cdcTask.Start(ctx)
	}
}
