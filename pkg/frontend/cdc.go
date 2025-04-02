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

package frontend

import (
	"context"
	"database/sql"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	CdcRunning   = "running"
	CdcPaused    = "paused"
	CdcFailed    = "failed"
	maxErrMsgLen = 256
)

var queryTable = func(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	callback func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
	var rows *sql.Rows
	var err error
	rows, err = tx.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	if rows.Err() != nil {
		return false, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret bool
	for rows.Next() {
		ret, err = callback(ctx, rows)
		if err != nil {
			return false, err
		}
		if ret {
			return true, nil
		}
	}
	return false, nil
}

var initAesKeyByInternalExecutor = func(
	ctx context.Context,
	exec *CDCTaskExecutor,
	accountId uint32,
) error {
	return exec.initAesKeyByInternalExecutor(ctx, accountId)
}

func deleteWatermark(ctx context.Context, tx taskservice.SqlExecutor, taskKeyMap map[taskservice.CDCTaskKey]struct{}) (int64, error) {
	tCount := int64(0)
	cnt := int64(0)
	var err error
	//deleting mo_cdc_watermark belongs to cancelled cdc task
	for tInfo := range taskKeyMap {
		deleteSql2 := cdc.CDCSQLBuilder.DeleteWatermarkSQL(tInfo.AccountId, tInfo.TaskId)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql2)
		if err != nil {
			return 0, err
		}
		tCount += cnt
	}
	return tCount, nil
}
