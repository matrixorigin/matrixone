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
	"encoding/base64"
	"fmt"
	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"go.uber.org/zap"
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

type LevelDist struct {
	Lv               int
	ObjCnt           int     // hitted object count
	ObjCntProportion float64 // hitted object count / total object count
	DelAvg           float64 // average deleted row proportion of hitted objects
	DelVar           float64 // variance of deleted row proportion of hitted objects
}

type levelRaw struct {
	Hit   int
	Total int
	Props []float64
}

func LevelDistToZapFields(levelDist []LevelDist) []zap.Field {
	fields := make([]zap.Field, 0, len(levelDist)*4)
	for i, dist := range levelDist {
		prefix := fmt.Sprintf("l%d", i)
		fields = append(fields,
			zap.Int(prefix+"ObjCnt", dist.ObjCnt),
			zap.Float64(prefix+"ObjCntProportion", dist.ObjCntProportion),
			zap.Float64(prefix+"DelAvg", dist.DelAvg),
			zap.Float64(prefix+"DelVar", dist.DelVar),
		)
	}
	return fields
}

func InputObjectZapFields(
	tid uint64,
	stats *objectio.ObjectStats,
	createTime types.TS,
	isTombstone bool,
) []zap.Field {
	fields := make([]zap.Field, 0, 4)
	fields = append(fields, zap.Uint64("tid", tid))
	fields = append(fields, zap.Bool("isTombstone", isTombstone))
	fields = append(fields, zap.String("stats", base64.StdEncoding.EncodeToString(stats[:])))
	fields = append(fields, zap.String("createTime", createTime.ToString()))
	return fields
}

func LogInputDataObject(
	table *TableEntry,
	stats *objectio.ObjectStats,
	createTime types.TS,
) {
	fields := InputObjectZapFields(table.ID, stats, createTime, false)
	logutil.Info("InputObject", fields...)
}

func LogInputTombstoneObjectWithExistingBatches(
	table *TableEntry,
	stats *objectio.ObjectStats,
	createTime types.TS,
	existingBatches []*batch.Batch,
) {
	rowids := make(map[objectio.ObjectId]int)
	for _, bat := range existingBatches {
		rids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
		for i := range len(rids) {
			rowids[*rids[i].BorrowObjectID()]++
		}
	}
	dist := CalcTombstoneDist(table, rowids)
	fields := InputObjectZapFields(table.ID, stats, createTime, true)
	fields = append(fields, LevelDistToZapFields(dist)...)
	logutil.Info("InputObject", fields...)
}

func LogInputTombstoneObjectAsync(
	table *TableEntry,
	stats *objectio.ObjectStats,
	createTime types.TS,
	rt *dbutils.Runtime,
) {
	job := func() error {
		ctx := context.Background()
		rowids := make(map[objectio.ObjectId]int)
		for i := range stats.BlkCnt() {
			loc := stats.BlockLocation(uint16(i), objectio.BlockMaxRows)
			vectors, closeFunc, err := ioutil.LoadColumns2(
				ctx,
				[]uint16{uint16(0)},
				nil,
				rt.Fs,
				loc,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				continue
			}
			cnVec := vectors[0].GetDownstreamVector()
			rids := vector.MustFixedColNoTypeCheck[types.Rowid](cnVec)
			for j := range len(rids) {
				rowids[*rids[j].BorrowObjectID()]++
			}
			closeFunc()
		}
		dist := CalcTombstoneDist(table, rowids)
		fields := InputObjectZapFields(table.ID, stats, createTime, true)
		fields = append(fields, LevelDistToZapFields(dist)...)
		logutil.Info("InputObject", fields...)
		return nil
	}
	rt.Scheduler.ScheduleFn(nil, tasks.IOTask, job)
}

func CalcTombstoneDist(
	table *TableEntry,
	rowids map[objectio.ObjectId]int,
) []LevelDist {
	levelDist := make([]LevelDist, 8)
	levelRaw := make([]levelRaw, 8)

	for id, cnt := range rowids {
		meta, _ := table.GetObjectByID(&id, false)
		if meta == nil || meta.IsAppendable() {
			continue
		}

		lv := meta.GetLevel()
		levelRaw[lv].Hit++
		proportion := float64(cnt) / float64(meta.Rows())
		levelRaw[lv].Props = append(levelRaw[lv].Props, proportion)
	}

	it := table.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	defer it.Release()
	for it.Next() {
		item := it.Item()
		if !ObjectValid(item) {
			continue
		}
		lv := item.GetLevel()
		levelRaw[lv].Total++
	}

	for lv := range levelRaw {
		levelDist[lv].Lv = lv
		levelDist[lv].ObjCnt = levelRaw[lv].Hit
		if levelRaw[lv].Total > 0 {
			p := float64(levelRaw[lv].Hit) / float64(levelRaw[lv].Total)
			levelDist[lv].ObjCntProportion = p
		}

		// Calculate average and variance of deleted row proportion
		cnt := len(levelRaw[lv].Props)
		if cnt > 0 {
			// Calculate average
			sum := 0.0
			for _, prop := range levelRaw[lv].Props {
				sum += prop
			}
			avg := sum / float64(cnt)
			levelDist[lv].DelAvg = avg

			// Calculate variance
			if cnt > 1 {
				varSum := 0.0
				for _, prop := range levelRaw[lv].Props {
					diff := prop - avg
					varSum += diff * diff
				}
				levelDist[lv].DelVar = varSum / float64(cnt)
			}
		}
	}
	return levelDist
}
