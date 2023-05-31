// Copyright 2023 Matrix Origin
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

package motrace

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ batchpipe.HasName = (*ItemSyncer)(nil)
var _ IBuffer2SqlItem = (*ItemSyncer)(nil)
var _ table.RowField = (*ItemSyncer)(nil)
var _ table.NeedCheckWrite = (*ItemSyncer)(nil)
var _ table.NeedSyncWrite = (*ItemSyncer)(nil)

type NamedItemRow interface {
	IBuffer2SqlItem
	table.RowField
}

type ItemSyncer struct {
	item NamedItemRow
	ch   chan struct{}
}

func NewItemSyncer(item NamedItemRow) *ItemSyncer {
	return &ItemSyncer{
		item: item,
		ch:   make(chan struct{}),
	}
}

// GetName implements IBuffer2SqlItem and batchpipe.HasName
func (s *ItemSyncer) GetName() string { return s.item.GetName() }

// Size implements IBuffer2SqlItem
func (s *ItemSyncer) Size() int64 { return s.item.Size() }

// Free implements IBuffer2SqlItem
func (s *ItemSyncer) Free() {
	if s.item != nil {
		s.item.Free()
		s.item = nil
	}
}

// GetTable implements table.RowField
func (s *ItemSyncer) GetTable() *table.Table { return s.item.GetTable() }

// FillRow implements table.RowField
func (s *ItemSyncer) FillRow(ctx context.Context, row *table.Row) { s.item.FillRow(ctx, row) }

// NeedCheckWrite implements NeedCheckWrite
func (s *ItemSyncer) NeedCheckWrite() bool { return true }

// NeedSyncWrite implements NeedSyncWrite
func (s *ItemSyncer) NeedSyncWrite() bool { return true }

// GetCheckWriteHook implements NeedCheckWrite and NeedSyncWrite
func (s *ItemSyncer) GetCheckWriteHook() table.CheckWriteHook {
	return func(_ context.Context) {
		close(s.ch)
	}
}

// Wait cooperate with NeedSyncWrite and NeedSyncWrite
func (s *ItemSyncer) Wait() {
	select {
	case t := <-time.After(time.Minute):
		logutil.Warn(fmt.Sprintf("LogSyncer wait timeout at: %s", table.Time2DatetimeString(t)), logutil.NoReportFiled())
	case <-s.ch:
		logutil.Info("Wait signal done.")
	}
}
