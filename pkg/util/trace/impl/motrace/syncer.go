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
