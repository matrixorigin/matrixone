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

package aoedb

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/stretchr/testify/assert"
)

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        ?     ]
// 2.  Create a table 1-1                                 [   1,        ?     ]
// 3.  Append 6 block rows into table 1-1                 [   2,        ?     ]
func TestSplit1(t *testing.T) {
	initTestEnv(t)
	inst1, gen, database := initTestDB1(t)

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err := inst1.CreateTable(createCtx)
	assert.Nil(t, err)

	rows := inst1.Store.Catalog.Cfg.BlockMaxRows * 12
	ck := mock.MockBatch(schema.Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schema.Name, ck)
	err = inst1.Append(appendCtx)
	assert.Nil(t, err)

	err = inst1.FlushDatabase(database.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, database.UncheckpointedCnt())
	coarseSize := database.GetSize()
	size := coarseSize * 7 / 8

	prepareCtx := &PrepareSplitCtx{
		DB:   database.Name,
		Size: uint64(size),
	}
	_, _, keys, ctx, err := inst1.PrepareSplitDatabase(prepareCtx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))

	newNames := make([]string, len(keys))
	for i, _ := range newNames {
		newNames[i] = fmt.Sprintf("splitted-%d", i)
	}
	renameTable := func(oldName, newDBName string) string {
		return oldName
	}
	execCtx := &ExecSplitCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		NewNames:      newNames,
		RenameTable:   renameTable,
		SplitKeys:     keys,
		SplitCtx:      ctx,
	}
	err = inst1.ExecSplitDatabase(execCtx)
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, database.UncheckpointedCnt())
	t.Log(inst1.Store.Catalog.PString(metadata.PPL1, 0))

	inst1.Close()
}

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        0     ]
// 2.  Disable upgrade meta segment                       [   0,        0     ]
// 3.  Create 2 tables: 1-1 [1,?], 1-2 [2,?]              [   2,        ?     ]
// 4.  Append 35/10 (MaxBlockRows) rows into (1-1)        [   3,        2     ]
// 5.  Append 35/10 (MaxBlockRows) rows into (1-2)        [   4,        2     ]
// 6.  FlushTable (1-1) (1-2)                             [   -,        ?     ]
// 7.  Split index should be [4]                          [   -,        4     ]
// 8.  Create another db instance
// 9.  Apply previous created snapshot
// 10. Check:
//     1) database exists 2) database checkpoint id is
//     3) check segment flushed
// 11. Restart new db instance and check again
func TestSplit2(t *testing.T) {
	initTestEnv(t)
	prepareSnapshotPath(defaultSnapshotPath, t)
	// 1. Create a db instance and a database
	inst, gen, database := initTestDB1(t)
	defer inst.Close()
	// shardId := database.GetShardId()
	// idxGen := gen.Shard(shardId)

	// 2. Disable upgrade segment
	inst.Scheduler.ExecCmd(sched.TurnOffUpgradeSegmentMetaCmd)

	// 3. Create 2 tables
	schemas := make([]*metadata.Schema, 2)
	var createCtx *CreateTableCtx
	for i, _ := range schemas {
		schema := metadata.MockSchema(i + 1)
		createCtx = &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
		assert.Nil(t, err)
		schemas[i] = schema
	}

	// 4. Append rows to 1-1
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 35 / 10
	ck0 := mock.MockBatch(schemas[0].Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schemas[0].Name, ck0)
	err := inst.Append(appendCtx)
	assert.Nil(t, err)

	// 5. Append rows to 1-2
	ck1 := mock.MockBatch(schemas[1].Types(), rows)
	appendCtx = CreateAppendCtx(database, gen, schemas[1].Name, ck1)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)

	t0 := database.SimpleGetTableByName(schemas[0].Name)
	data0, _ := inst.GetTableData(t0)
	defer data0.Unref()
	assert.Equal(t, ck0.Length(), int(data0.GetRowCount()))

	t1 := database.SimpleGetTableByName(schemas[1].Name)
	data1, _ := inst.GetTableData(t1)
	defer data1.Unref()
	assert.Equal(t, ck1.Length(), int(data1.GetRowCount()))

	// 6. Flush
	err = inst.FlushDatabase(database.Name)
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	coarseSize := database.GetSize()
	size := coarseSize

	// 7. Split
	prepareCtx := &PrepareSplitCtx{
		DB:   database.Name,
		Size: uint64(size),
	}
	_, _, keys, ctx, err := inst.PrepareSplitDatabase(prepareCtx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))

	newNames := make([]string, len(keys))
	for i, _ := range newNames {
		newNames[i] = fmt.Sprintf("splitted-%d", i)
	}
	renameTable := func(oldName, newDBName string) string {
		return oldName
	}
	execCtx := &ExecSplitCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		NewNames:      newNames,
		RenameTable:   renameTable,
		SplitKeys:     keys,
		SplitCtx:      ctx,
	}
	err = inst.ExecSplitDatabase(execCtx)
	assert.Nil(t, err)

	// inst.Scheduler.ExecCmd(sched.TurnOnUpgradeSegmentMetaCmd)

	tables := make([]iface.ITableData, 0)
	totalRows := uint64(0)
	processor := new(metadata.LoopProcessor)
	processor.TableFn = func(tbl *metadata.Table) error {
		tbl.RLock()
		defer tbl.RUnlock()
		td, err := inst.Store.DataTables.WeakRefTable(tbl.Id)
		assert.Nil(t, err)
		totalRows += td.GetRowCount()
		tables = append(tables, td)
		return nil
	}

	var dbs []*metadata.Database
	for _, name := range newNames {
		db, err := inst.Store.Catalog.SimpleGetDatabaseByName(name)
		assert.Nil(t, err)
		dbs = append(dbs, db)
		db.RLock()
		err = db.RecurLoopLocked(processor)
		assert.Nil(t, err)
		db.RUnlock()
	}
	assert.Equal(t, rows*2, totalRows)
	testutils.WaitExpect(500, func() bool {
		return t0.IsHardDeleted() && t1.IsHardDeleted()
	})
	assert.True(t, t0.IsHardDeleted() && t1.IsHardDeleted())
	inst.ForceCompactCatalog()
	t.Log(database.Catalog.PString(metadata.PPL0, 0))
	assert.Equal(t, 2, len(database.Catalog.SimpleGetDatabaseNames()))

	active := tables[0]
	activeMeta := active.GetMeta()
	rows1 := active.GetRowCount()

	ck := mock.MockBatch(activeMeta.Schema.Types(), rows)
	appendCtx = CreateAppendCtx(activeMeta.Database, gen, activeMeta.Schema.Name, ck)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)
	assert.Equal(t, rows+rows1, active.GetRowCount())

	// data0, _ := aoedb2.GetTableData(t0)
	// defer data0.Unref()
	// assert.Equal(t, 2*ck0.Length(), int(data0.GetRowCount()))

	// t1 := db2.SimpleGetTableByName(schemas[1].Name)
	// assert.Equal(t, 2, t1.SimpleGetSegmentCount())

	// data1, _ := aoedb2.GetTableData(t1)
	// defer data1.Unref()
	// assert.Equal(t, ck1.Length(), int(data1.GetRowCount()))
	// t.Log(t1.PString(metadata.PPL1, 0))

	// aoedb2.Close()
}
