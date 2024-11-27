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

package cdc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	watermarkUpdateInterval = time.Second

	maxErrMsgLen = 256

	insertWatermarkFormat = "insert into mo_catalog.mo_cdc_watermark values (%d, '%s', '%s', '%s', '%s', '%s', '%s')"

	getWatermarkFormat = "select watermark from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s' and table_id = '%s'"

	getWatermarkCountFormat = "select count(1) from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s'"

	updateWatermarkFormat = "update mo_catalog.mo_cdc_watermark set watermark='%s' where account_id = %d and task_id = '%s' and table_id = '%s'"

	deleteWatermarkFormat = "delete from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s'"

	deleteWatermarkByTableFormat = "delete from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s' and table_id = '%s'"

	updateErrMsgFormat = "update mo_catalog.mo_cdc_watermark set err_msg='%s' where account_id = %d and task_id = '%s' and table_id = '%s'"
)

type WatermarkUpdater struct {
	accountId uint32
	taskId    uuid.UUID
	// sql executor
	ie ie.InternalExecutor
	// watermarkMap saves the watermark of each table
	watermarkMap *sync.Map
}

func NewWatermarkUpdater(accountId uint64, taskId string, ie ie.InternalExecutor) *WatermarkUpdater {
	u := &WatermarkUpdater{
		accountId:    uint32(accountId),
		ie:           ie,
		watermarkMap: &sync.Map{},
	}
	u.taskId, _ = uuid.Parse(taskId)
	return u
}

func (u *WatermarkUpdater) Run(ctx context.Context, ar *ActiveRoutine) {
	logutil.Info("cdc WatermarkUpdater.Run: start")
	defer func() {
		u.flushAll()
		logutil.Info("cdc WatermarkUpdater.Run: end")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		case <-time.After(watermarkUpdateInterval):
			u.flushAll()
		}
	}
}

func (u *WatermarkUpdater) InsertIntoDb(dbTableInfo *DbTableInfo, watermark types.TS) error {
	sql := fmt.Sprintf(insertWatermarkFormat,
		u.accountId, u.taskId,
		dbTableInfo.SourceTblIdStr, dbTableInfo.SourceDbName, dbTableInfo.SourceTblName,
		watermark.ToString(), "")
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) GetFromMem(tableIdStr string) types.TS {
	if value, ok := u.watermarkMap.Load(tableIdStr); ok {
		return value.(types.TS)
	}
	return types.TS{}
}

func (u *WatermarkUpdater) GetFromDb(tableIdStr string) (watermark types.TS, err error) {
	sql := fmt.Sprintf(getWatermarkFormat, u.accountId, u.taskId, tableIdStr)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	res := u.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		err = res.Error()
	} else if res.RowCount() < 1 {
		err = moerr.NewInternalErrorf(ctx, "no watermark found for task: %s, tableIdStr: %v\n", u.taskId, tableIdStr)
	} else if res.RowCount() > 1 {
		err = moerr.NewInternalErrorf(ctx, "duplicate watermark found for task: %s, tableIdStr: %v\n", u.taskId, tableIdStr)
	}
	if err != nil {
		return
	}

	watermarkStr, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return
	}
	return types.StringToTS(watermarkStr), nil
}

func (u *WatermarkUpdater) GetCountFromDb() (uint64, error) {
	sql := fmt.Sprintf(getWatermarkCountFormat, u.accountId, u.taskId)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	res := u.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return 0, res.Error()
	}
	return res.GetUint64(ctx, 0, 0)
}

func (u *WatermarkUpdater) UpdateMem(tableIdStr string, watermark types.TS) {
	u.watermarkMap.Store(tableIdStr, watermark)
}

func (u *WatermarkUpdater) DeleteFromMem(tableIdStr string) {
	u.watermarkMap.Delete(tableIdStr)
}

func (u *WatermarkUpdater) DeleteFromDb(tableIdStr string) error {
	sql := fmt.Sprintf(deleteWatermarkByTableFormat, u.accountId, u.taskId, tableIdStr)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) DeleteAllFromDb() error {
	sql := fmt.Sprintf(deleteWatermarkFormat, u.accountId, u.taskId)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) SaveErrMsg(tableIdStr string, errMsg string) error {
	if len(errMsg) > maxErrMsgLen {
		errMsg = errMsg[:maxErrMsgLen]
	}
	sql := fmt.Sprintf(updateErrMsgFormat, errMsg, u.accountId, u.taskId, tableIdStr)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) flushAll() {
	u.watermarkMap.Range(func(k, v any) bool {
		tableIdStr := k.(string)
		ts := v.(types.TS)
		if err := u.flush(tableIdStr, ts); err != nil {
			logutil.Errorf("flush table %s failed, current watermark: %s err: %v\n", tableIdStr, ts.ToString(), err)
		}
		return true
	})
}

func (u *WatermarkUpdater) flush(tableIdStr string, watermark types.TS) error {
	sql := fmt.Sprintf(updateWatermarkFormat, watermark.ToString(), u.accountId, u.taskId, tableIdStr)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}
