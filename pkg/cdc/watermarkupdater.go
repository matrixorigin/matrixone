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
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	watermarkUpdateInterval = time.Second

	insertWatermarkFormat = "insert into mo_catalog.mo_cdc_watermark values (%d, '%s', %d, '%s')"

	getWatermarkFormat = "select watermark from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s' and table_id = %d"

	getWatermarkCountFormat = "select count(1) from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s'"

	updateWatermarkFormat = "update mo_catalog.mo_cdc_watermark set watermark='%s' where account_id = %d and task_id = '%s' and table_id = %d"

	deleteWatermarkFormat = "delete from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s'"

	deleteWatermarkByTableFormat = "delete from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s' and table_id = %d"
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

func (u *WatermarkUpdater) Run(ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ WatermarkUpdater.Run: start\n")
	defer func() {
		u.flushAll()
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ WatermarkUpdater.Run: end\n")
	}()

	for {
		select {
		case <-ar.Cancel:
			return

		case <-time.After(watermarkUpdateInterval):
			u.flushAll()
		}
	}
}

func (u *WatermarkUpdater) InsertIntoDb(tableId uint64, watermark types.TS) error {
	sql := fmt.Sprintf(insertWatermarkFormat, u.accountId, u.taskId, tableId, watermark.ToString())
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) GetFromMem(tableId uint64) types.TS {
	if value, ok := u.watermarkMap.Load(tableId); ok {
		return value.(types.TS)
	}
	return types.TS{}
}

func (u *WatermarkUpdater) GetFromDb(tableId uint64) (watermark types.TS, err error) {
	sql := fmt.Sprintf(getWatermarkFormat, u.accountId, u.taskId, tableId)
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	res := u.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		err = res.Error()
	} else if res.RowCount() < 1 {
		err = moerr.NewInternalErrorf(ctx, "no watermark found for task: %s, tableId: %v\n", u.taskId, tableId)
	} else if res.RowCount() > 1 {
		err = moerr.NewInternalErrorf(ctx, "duplicate watermark found for task: %s, tableId: %v\n", u.taskId, tableId)
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
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	res := u.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return 0, res.Error()
	}
	return res.GetUint64(ctx, 0, 0)
}

func (u *WatermarkUpdater) UpdateMem(tableId uint64, watermark types.TS) {
	u.watermarkMap.Store(tableId, watermark)
}

func (u *WatermarkUpdater) DeleteFromMem(tableId uint64) {
	u.watermarkMap.Delete(tableId)
}

func (u *WatermarkUpdater) DeleteFromDb(tableId uint64) error {
	sql := fmt.Sprintf(deleteWatermarkByTableFormat, u.accountId, u.taskId, tableId)
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) DeleteAllFromDb() error {
	sql := fmt.Sprintf(deleteWatermarkFormat, u.accountId, u.taskId)
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (u *WatermarkUpdater) flushAll() {
	u.watermarkMap.Range(func(k, v any) bool {
		tableId := k.(uint64)
		ts := v.(types.TS)
		if err := u.updateDb(tableId, ts); err != nil {
			fmt.Fprintf(os.Stderr, "flush table %d failed, current watermark: %s\n", tableId, ts.ToString())
		}
		return true
	})
}

func (u *WatermarkUpdater) updateDb(tableId uint64, watermark types.TS) error {
	sql := fmt.Sprintf(updateWatermarkFormat, watermark.ToString(), u.accountId, u.taskId, tableId)
	ctx := defines.AttachAccountId(context.Background(), u.accountId)
	return u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}
