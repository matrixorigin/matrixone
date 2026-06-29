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

package publication

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var CheckLeaseWithRetry = func(
	ctx context.Context,
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) (ok bool, err error) {
	defer func() {
		if err != nil || !ok {
			logutil.Error(
				"Publication-Task check lease failed",
				zap.Error(err),
				zap.Bool("ok", ok),
				zap.String("cnUUID", cnUUID),
			)
		}
	}()
	err = retryPublication(
		ctx,
		func() error {
			ok, err = checkLease(ctx, cnUUID, txnEngine, cnTxnClient)
			return err
		},
		DefaultExecutorRetryOption(),
	)
	return
}

func checkLease(
	ctx context.Context,
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) (ok bool, err error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, err := getTxn(ctxWithTimeout, txnEngine, cnTxnClient, "publication check lease")
	if err != nil {
		return
	}
	defer txn.Commit(ctxWithTimeout)

	sql := `select task_runner from mo_task.sys_daemon_task where task_type = "Publication" and task_runner is not null`
	result, err := ExecWithResult(ctxWithTimeout, sql, cnUUID, txn)
	if err != nil {
		return
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 1 {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected rows count: %d", rows))
			return false
		}
		runner := cols[0].GetStringAt(0)
		if runner == "" {
			err = moerr.NewInternalErrorNoCtx("task runner is null")
			return false
		}
		if runner == cnUUID {
			ok = true
		} else {
			logutil.Errorf(
				"Publication-Task check lease failed, runner: %s, expected: %s",
				runner,
				cnUUID,
			)
		}
		return false
	})
	return
}
