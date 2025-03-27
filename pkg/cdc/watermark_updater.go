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

	updateWatermarkFormat = "UPDATE " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"SET watermark='%s' " +
		"WHERE account_id = %d " +
		"AND task_id = '%s' " +
		"AND db_name = '%s' " +
		"AND table_name = '%s'"

	deleteWatermarkByTableFormat = "DELETE " +
		"FROM `mo_catalog`.`mo_cdc_watermark` " +
		"WHERE account_id = %d " +
		"AND task_id = '%s' " +
		"AND db_name = '%s' " +
		"AND table_name = '%s'"

	updateErrMsgFormat = "UPDATE " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"SET err_msg='%s' " +
		"WHERE account_id = %d " +
		"AND task_id = '%s' " +
		"AND db_name = '%s' " +
		"AND table_name = '%s'"
)

var _ IWatermarkUpdater = new(WatermarkUpdater)

type WatermarkUpdater struct {
	accountId uint32
	taskId    uuid.UUID
	// sql executor
	ie ie.InternalExecutor
	// watermarkMap saves the watermark of each table
	watermarkMap *sync.Map
}

func NewWatermarkUpdater(accountId uint32, taskId string, ie ie.InternalExecutor) *WatermarkUpdater {
	u := &WatermarkUpdater{
		accountId:    accountId,
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
	sql := CDCSQLBuilder.InsertWatermarkSQL(
		uint64(u.accountId),
		u.taskId.String(),
		dbTableInfo.SourceDbName,
		dbTableInfo.SourceTblName,
		watermark.ToString(),
	)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) GetFromMem(dbName, tblName string) types.TS {
	if value, ok := u.watermarkMap.Load(GenDbTblKey(dbName, tblName)); ok {
		return value.(types.TS)
	}
	return types.TS{}
}

func (u *WatermarkUpdater) GetFromDb(dbName, tblName string) (watermark types.TS, err error) {
	sql := CDCSQLBuilder.GetTableWatermarkSQL(
		uint64(u.accountId), u.taskId.String(), dbName, tblName,
	)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	res := u.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		err = res.Error()
	} else if res.RowCount() < 1 {
		err = moerr.NewErrNoWatermarkFoundNoCtx(dbName, tblName)
	} else if res.RowCount() > 1 {
		err = moerr.NewInternalErrorf(ctx, "duplicate watermark found for task: %s, table: %s.%s", u.taskId, dbName, tblName)
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

func (u *WatermarkUpdater) UpdateMem(dbName, tblName string, watermark types.TS) {
	u.watermarkMap.Store(GenDbTblKey(dbName, tblName), watermark)
}

func (u *WatermarkUpdater) DeleteFromMem(dbName, tblName string) {
	u.watermarkMap.Delete(GenDbTblKey(dbName, tblName))
}

func (u *WatermarkUpdater) DeleteFromDb(dbName, tblName string) error {
	sql := fmt.Sprintf(deleteWatermarkByTableFormat, u.accountId, u.taskId, dbName, tblName)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) SaveErrMsg(dbName, tblName string, errMsg string) error {
	if len(errMsg) > maxErrMsgLen {
		errMsg = errMsg[:maxErrMsgLen]
	}
	sql := fmt.Sprintf(updateErrMsgFormat, errMsg, u.accountId, u.taskId, dbName, tblName)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) flushAll() {
	u.watermarkMap.Range(func(k, v any) bool {
		key := k.(string)
		ts := v.(types.TS)
		if err := u.flush(key, ts); err != nil {
			logutil.Errorf("flush table %s failed, current watermark: %s err: %v", key, ts.ToString(), err)
		}
		return true
	})
}

func (u *WatermarkUpdater) flush(key string, watermark types.TS) error {
	dbName, tblName := SplitDbTblKey(key)
	sql := fmt.Sprintf(updateWatermarkFormat, watermark.ToString(), u.accountId, u.taskId, dbName, tblName)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}
