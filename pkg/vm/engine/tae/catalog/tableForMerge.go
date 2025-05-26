// Copyright 2025 Matrix Origin
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

package catalog

import (
	"context"
	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type MergeNotifierOnCatalog interface {
	// OnCreateTableCommit is called when a table is created and committed.
	// The table is not visible to users until it is committed.
	OnCreateTableCommit(table MergeTable)
	// OnCreateNonAppendObject is called when a non-appendable object is created, committed is not required
	// CreateNonAppendObject is just used for trigger merge task, it is not a big deal if the object is rollbacked
	OnCreateNonAppendObject(table MergeTable)
}

type CatalogEventSource interface {
	InitSource() iter.Seq[MergeTable]
	SetMergeNotifier(MergeNotifierOnCatalog)
	GetMergeSettingsBatchFn() func() (*batch.Batch, func())
}

type MergeDataItem interface {
	GetCreatedAt() types.TS
	GetObjectStats() *objectio.ObjectStats
}

type MergeTombstoneItem interface {
	MergeDataItem
	// IO
	ForeachRowid(
		ctx context.Context,
		reuseBatch any,
		each func(rowid types.Rowid, isNull bool, rowIdx int) error,
	) error
	// make a buffer batch for IO, reuse for every tombstone item
	// the second return value is the cleanup function
	MakeBufferBatch() (any, func())
}

type MergeTable interface {
	IterDataItem() iter.Seq[MergeDataItem]
	IterTombstoneItem() iter.Seq[MergeTombstoneItem]
	GetNameDesc() string
	ID() uint64
	HasDropCommitted() bool

	IsSpecialBigTable() bool // upgrade: old objects in big table is not merged by default
}

type TNTombstoneItem struct {
	*ObjectEntry
}

func (catalog *Catalog) SetMergeNotifier(notifier MergeNotifierOnCatalog) {
	catalog.mergeNotifier = notifier
}

func (catalog *Catalog) InitSource() iter.Seq[MergeTable] {
	return func(yield func(MergeTable) bool) {
		p := new(LoopProcessor)
		p.TableFn = func(table *TableEntry) error {
			if table.IsActive() {
				yield(ToMergeTable(table))
			}
			return moerr.GetOkStopCurrRecur()
		}
		catalog.RecurLoop(p)
	}
}

func (t TNTombstoneItem) MakeBufferBatch() (any, func()) {
	bat := containers.BuildBatchWithPool(
		[]string{objectio.TombstoneAttr_Rowid_Attr},
		[]types.Type{types.T_Rowid.ToType()},
		8192,
		t.GetObjectData().GetRuntime().VectorPool.Transient,
	)
	return bat, bat.Close
}

func (t TNTombstoneItem) ForeachRowid(
	ctx context.Context,
	reuseBatch any,
	each func(rowid types.Rowid, isNull bool, rowIdx int) error,
) error {
	bufferBatch := reuseBatch.(*containers.Batch)

	obj := t.GetObjectData()
	for blk := range t.BlockCnt() {
		if err := obj.Scan(
			ctx,
			&bufferBatch,
			txnbase.MockTxnReaderWithNow(),
			t.GetSchema(),
			uint16(blk),
			[]int{0}, // only the rowid column
			common.MergeAllocator,
		); err != nil {
			return err
		}
		err := containers.ForeachVector(bufferBatch.Vecs[0], each, nil)
		if err != nil {
			return err
		}

		bufferBatch.CleanOnlyData()
	}
	return nil
}

func ToMergeTable(tbl *TableEntry) MergeTable {
	return TNMergeTable{TableEntry: tbl}
}

type TNMergeTable struct {
	*TableEntry
}

func (t TNMergeTable) ID() uint64 {
	return t.TableEntry.ID
}

func (t TNMergeTable) IsSpecialBigTable() bool {
	name := t.GetLastestSchema(false).Name
	dbName := t.GetDB().GetName()
	if (name == "statement_info" || name == "rawlog") && dbName == "system" {
		return true
	}
	if name == "metric" && dbName == "system_metrics" {
		return true
	}
	return false
}

func (t TNMergeTable) IterDataItem() iter.Seq[MergeDataItem] {
	return func(yield func(MergeDataItem) bool) {
		it := t.TableEntry.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
		defer it.Release()
		for it.Next() {
			item := it.Item()
			if !ObjectValid(item) {
				continue
			}
			if !yield(item) {
				break
			}
		}
	}
}

func (t TNMergeTable) IterTombstoneItem() iter.Seq[MergeTombstoneItem] {
	return func(yield func(MergeTombstoneItem) bool) {
		it := t.TableEntry.MakeTombstoneVisibleObjectIt(txnbase.MockTxnReaderWithNow())
		defer it.Release()
		for it.Next() {
			item := it.Item()
			if !ObjectValid(item) {
				continue
			}
			if !yield(TNTombstoneItem{ObjectEntry: item}) {
				break
			}
		}
	}
}

func ObjectValid(objectEntry *ObjectEntry) bool {
	if objectEntry.IsAppendable() {
		return false
	}
	if !objectEntry.IsActive() {
		return false
	}
	if !objectEntry.IsCommitted() {
		return false
	}
	if objectEntry.IsCreatingOrAborted() {
		return false
	}
	return true
}
