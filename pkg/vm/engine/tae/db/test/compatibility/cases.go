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

package compatibility

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	PrepareCaseRegister(makePrepare1())
	TestCaseRegister(makeTest1())

	PrepareCaseRegister(prepareAppend())
	TestCaseRegister(testAppend())

	PrepareCaseRegister(prepareDDL())
	TestCaseRegister(testDDL())

	PrepareCaseRegister(prepareCompact())
	TestCaseRegister(testCompact())

	PrepareCaseRegister(prepareDelete())
	TestCaseRegister(testDelete())
}

func initPrepareTest(pc PrepareCase, opts *options.Options, t *testing.T) *testutil.TestEngine {
	dir, err := InitPrepareDirByType(pc.typ)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	return tae
}

func makePrepare1() PrepareCase {
	getSchema := func(tc PrepareCase, t *testing.T) *catalog.Schema {
		schema := catalog.MockSchemaAll(18, 13)
		schema.Name = "test-1"
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		return schema
	}
	getBatch := func(tc PrepareCase, t *testing.T) *containers.Batch {
		schema := getSchema(tc, t)
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
		return bat
	}
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}
	getEngine := func(tc PrepareCase, t *testing.T) *testutil.TestEngine {
		opts := tc.getOptions(tc, t)
		e := initPrepareTest(tc, opts, t)
		e.BindSchema(getSchema(tc, t))
		return e
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.getEngine(tc, t)
		defer tae.Close()

		schema := tc.getSchema(tc, t)
		bat := tc.getBatch(tc, t)
		defer bat.Close()
		bats := bat.Split(4)

		tae.CreateRelAndAppend(bats[0], true)
		txn, rel := tae.GetRelation()
		v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		window := bat.CloneWindow(2, 1)
		defer window.Close()
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())
	}
	return PrepareCase{
		desc:       "prepare-1",
		typ:        1,
		prepareFn:  prepareFn,
		getSchema:  getSchema,
		getBatch:   getBatch,
		getEngine:  getEngine,
		getOptions: getOptions,
	}
}

func prepareAppend() PrepareCase {
	getSchema := func(tc PrepareCase, t *testing.T) *catalog.Schema {
		schema := catalog.MockSchemaAll(18, 13)
		schema.Name = "test-1"
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		return schema
	}
	getBatch := func(tc PrepareCase, t *testing.T) *containers.Batch {
		schema := getSchema(tc, t)
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1)*2)
		return bat
	}
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}
	getEngine := func(tc PrepareCase, t *testing.T) *testutil.TestEngine {
		opts := tc.getOptions(tc, t)
		e := initPrepareTest(tc, opts, t)
		e.BindSchema(getSchema(tc, t))
		return e
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.getEngine(tc, t)
		defer tae.Close()

		bat := tc.getBatch(tc, t)
		defer bat.Close()
		bats := bat.Split(2)

		// checkpoint
		tae.CreateRelAndAppend(bats[0], true)
		err := tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// wal
		tae.DoAppend(bats[1])
	}
	return PrepareCase{
		desc:       "prepare-2",
		typ:        2,
		prepareFn:  prepareFn,
		getSchema:  getSchema,
		getBatch:   getBatch,
		getEngine:  getEngine,
		getOptions: getOptions,
	}
}

func prepareDDL() PrepareCase {
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}
	getEngine := func(tc PrepareCase, t *testing.T) *testutil.TestEngine {
		opts := tc.getOptions(tc, t)
		e := initPrepareTest(tc, opts, t)
		return e
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.getEngine(tc, t)
		defer tae.Close()

		// ckp1: create db1, create table1
		txn, err := tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err := txn.CreateDatabase("db1", "sql", "type")
		assert.NoError(t, err)
		schema1 := catalog.MockSchema(20, 2)
		schema1.Name = "table1"
		_, err = db.CreateRelation(schema1)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
		err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// ckp2-1: create db2, create table2
		// ckp2-2: drop db2, drop table2
		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db2", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db_of_table2", "sql", "type")
		assert.NoError(t, err)
		schema2 := catalog.MockSchema(20, 2)
		schema2.Name = "table2"
		_, err = db.CreateRelation(schema2)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
		err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table2")
		assert.NoError(t, err)
		_, err = db.DropRelationByName(schema2.Name)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.DropDatabase("db2")
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// ckp3: create and drop db3, table3
		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db3", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db_of_table3", "sql", "type")
		assert.NoError(t, err)
		schema3 := catalog.MockSchema(20, 2)
		schema3.Name = "table3"
		_, err = db.CreateRelation(schema3)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table3")
		assert.NoError(t, err)
		_, err = db.DropRelationByName(schema3.Name)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.DropDatabase("db3")
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// WAL: create db4, create table4
		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db4", "sql", "type")
		assert.NoError(t, err)
		schema4 := catalog.MockSchema(20, 2)
		schema4.Name = "table4"
		_, err = db.CreateRelation(schema4)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		// WAL: create and drop db5, create ane drop table5
		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db5", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.CreateDatabase("db_of_table5", "sql", "type")
		assert.NoError(t, err)
		schema5 := catalog.MockSchema(20, 2)
		schema5.Name = "table5"
		_, err = db.CreateRelation(schema5)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table5")
		assert.NoError(t, err)
		_, err = db.DropRelationByName(schema5.Name)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		db, err = txn.DropDatabase("db5")
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	return PrepareCase{
		desc:       "prepare-3",
		typ:        3,
		prepareFn:  prepareFn,
		getSchema:  nil,
		getBatch:   nil,
		getEngine:  getEngine,
		getOptions: getOptions,
	}
}

func prepareCompact() PrepareCase {
	getSchema := func(tc PrepareCase, t *testing.T) *catalog.Schema {
		schema := catalog.MockSchemaAll(18, 13)
		schema.Name = "test-1"
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		return schema
	}
	getBatch := func(tc PrepareCase, t *testing.T) *containers.Batch {
		schema := getSchema(tc, t)
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1)*2)
		return bat
	}
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}
	getEngine := func(tc PrepareCase, t *testing.T) *testutil.TestEngine {
		opts := tc.getOptions(tc, t)
		e := initPrepareTest(tc, opts, t)
		e.BindSchema(getSchema(tc, t))
		return e
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.getEngine(tc, t)
		defer tae.Close()

		bat := tc.getBatch(tc, t)
		defer bat.Close()
		bats := bat.Split(2)

		// checkpoint
		tae.CreateRelAndAppend(bats[0], true)
		tae.CompactBlocks(false)
		err := tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// wal
		tae.DoAppend(bats[1])
		tae.CompactBlocks(false)
	}
	return PrepareCase{
		desc:       "prepare-4",
		typ:        4,
		prepareFn:  prepareFn,
		getSchema:  getSchema,
		getBatch:   getBatch,
		getEngine:  getEngine,
		getOptions: getOptions,
	}
}

func prepareDelete() PrepareCase {
	getSchema := func(tc PrepareCase, t *testing.T) *catalog.Schema {
		schema := catalog.MockSchemaAll(18, 13)
		schema.Name = "test-1"
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		return schema
	}
	getBatch := func(tc PrepareCase, t *testing.T) *containers.Batch {
		schema := getSchema(tc, t)
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1)*3)
		return bat
	}
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}
	getEngine := func(tc PrepareCase, t *testing.T) *testutil.TestEngine {
		opts := tc.getOptions(tc, t)
		e := initPrepareTest(tc, opts, t)
		e.BindSchema(getSchema(tc, t))
		return e
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.getEngine(tc, t)
		defer tae.Close()

		bat := tc.getBatch(tc, t)
		defer bat.Close()
		bats := bat.Split(3)

		// compact and checkpoint
		tae.CreateRelAndAppend(bats[0], true)

		schema := tc.getSchema(tc, t)
		for i := 0; i < int(schema.BlockMaxRows+1); i++ {
			txn, rel := tae.GetRelation()
			v := testutil.GetSingleSortKeyValue(bats[0], schema, i)
			filter := handle.NewEQFilter(v)
			err := rel.DeleteByFilter(context.Background(), filter)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}

		tae.CompactBlocks(false)
		err := tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.StatMaxCommitTS())
		assert.NoError(t, err)

		// compact
		tae.DoAppend(bats[1])
		for i := 0; i < int(schema.BlockMaxRows+1); i++ {
			txn, rel := tae.GetRelation()
			v := testutil.GetSingleSortKeyValue(bats[1], schema, i)
			filter := handle.NewEQFilter(v)
			err := rel.DeleteByFilter(context.Background(), filter)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
		tae.CompactBlocks(false)

		// not compact
		tae.DoAppend(bats[2])
		for i := 0; i < int(schema.BlockMaxRows+1); i++ {
			txn, rel := tae.GetRelation()
			v := testutil.GetSingleSortKeyValue(bats[2], schema, i)
			filter := handle.NewEQFilter(v)
			err := rel.DeleteByFilter(context.Background(), filter)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
	}
	return PrepareCase{
		desc:       "prepare-5",
		typ:        5,
		prepareFn:  prepareFn,
		getSchema:  getSchema,
		getBatch:   getBatch,
		getEngine:  getEngine,
		getOptions: getOptions,
	}
}

func initTestEngine(tc TestCase, t *testing.T) *testutil.TestEngine {
	pc := GetPrepareCase(tc.dependsOn)
	opts := pc.getOptions(pc, t)
	dir, err := InitTestCaseExecuteDir(tc.name)
	assert.NoError(t, err)
	err = CopyDir(GetPrepareDirByType(pc.typ), dir)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	tae.BindSchema(pc.getSchema(pc, t))
	return tae
}

func makeTest1() TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		pc := GetPrepareCase(tc.dependsOn)
		tae := initTestEngine(tc, t)
		defer tae.Close()

		bat := pc.getBatch(pc, t)
		defer bat.Close()
		bats := bat.Split(4)
		window := bat.CloneWindow(2, 1)
		defer window.Close()

		txn, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
		err := rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())

		schema := pc.getSchema(pc, t)
		txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())
	}
	return TestCase{
		name:      "test-1",
		desc:      "test-1",
		dependsOn: 1,
		testFn:    testFn,
	}
}

func testAppend() TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		pc := GetPrepareCase(tc.dependsOn)
		tae := initTestEngine(tc, t)
		defer tae.Close()

		bat := pc.getBatch(pc, t)
		defer bat.Close()
		bats := bat.Split(bat.Length())

		txn, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
		for _, b := range bats {
			err := rel.Append(context.Background(), b)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		}
		assert.NoError(t, txn.Commit(context.Background()))

		tae.CheckRowsByScan(bat.Length(), true)
		tae.CheckRowsByScan(bat.Length(), false)
	}
	return TestCase{
		name:      "testAppend",
		desc:      "test-2",
		dependsOn: 2,
		testFn:    testFn,
	}
}

func testDDL() TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		tae := initTestEngine(tc, t)
		defer tae.Close()

		txn, err := tae.StartTxn(nil)
		assert.NoError(t, err)

		db, err := txn.GetDatabase("db1")
		assert.NoError(t, err)
		_, err = txn.CreateDatabase("db1", "sql", "type")
		assert.Error(t, err)
		_, err = db.GetRelationByName("table1")
		assert.NoError(t, err)
		schema1 := catalog.MockSchema(20, 2)
		schema1.Name = "table1"
		_, err = db.CreateRelation(schema1)
		assert.Error(t, err)

		_, err = txn.GetDatabase("db2")
		assert.Error(t, err)
		_, err = txn.CreateDatabase("db2", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table2")
		assert.NoError(t, err)
		_, err = db.GetRelationByName("table2")
		assert.Error(t, err)
		schema2 := catalog.MockSchema(20, 2)
		schema2.Name = "table2"
		_, err = db.CreateRelation(schema2)
		assert.NoError(t, err)

		_, err = txn.GetDatabase("db3")
		assert.Error(t, err)
		_, err = txn.CreateDatabase("db3", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table3")
		assert.NoError(t, err)
		_, err = db.GetRelationByName("table3")
		assert.Error(t, err)
		schema3 := catalog.MockSchema(20, 2)
		schema3.Name = "table3"
		_, err = db.CreateRelation(schema3)
		assert.NoError(t, err)

		db, err = txn.GetDatabase("db4")
		assert.NoError(t, err)
		_, err = txn.CreateDatabase("db4", "sql", "type")
		assert.Error(t, err)
		_, err = db.GetRelationByName("table4")
		assert.NoError(t, err)
		schema4 := catalog.MockSchema(20, 2)
		schema4.Name = "table4"
		_, err = db.CreateRelation(schema4)
		assert.Error(t, err)

		_, err = txn.GetDatabase("db5")
		assert.Error(t, err)
		_, err = txn.CreateDatabase("db5", "sql", "type")
		assert.NoError(t, err)
		db, err = txn.GetDatabase("db_of_table5")
		assert.NoError(t, err)
		_, err = db.GetRelationByName("table5")
		assert.Error(t, err)
		schema5 := catalog.MockSchema(20, 2)
		schema5.Name = "table5"
		_, err = db.CreateRelation(schema5)
		assert.NoError(t, err)

		assert.NoError(t, txn.Commit(context.Background()))
	}
	return TestCase{
		name:      "testDDL",
		desc:      "test-3",
		dependsOn: 3,
		testFn:    testFn,
	}
}

func testCompact() TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		pc := GetPrepareCase(tc.dependsOn)
		tae := initTestEngine(tc, t)
		defer tae.Close()

		bat := pc.getBatch(pc, t)
		defer bat.Close()
		bats := bat.Split(bat.Length())

		txn, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
		for _, b := range bats {
			err := rel.Append(context.Background(), b)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		}
		assert.NoError(t, txn.Commit(context.Background()))

		tae.CheckRowsByScan(bat.Length(), true)
		tae.CheckRowsByScan(bat.Length(), false)
	}
	return TestCase{
		name:      "testCompact",
		desc:      "test-4",
		dependsOn: 4,
		testFn:    testFn,
	}
}

func testDelete() TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		pc := GetPrepareCase(tc.dependsOn)
		tae := initTestEngine(tc, t)
		defer tae.Close()

		bat := pc.getBatch(pc, t)
		defer bat.Close()
		bats := bat.Split(bat.Length())

		schema := pc.getSchema(pc, t)
		totalRows := bat.Length() - int(schema.BlockMaxRows+1)*3
		txn, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, totalRows, true)
		testutil.CheckAllColRowsByScan(t, rel, totalRows, false)
		rows := schema.BlockMaxRows*3 + 1
		for i := 0; i < 3; i++ {
			for j := 0; j < int(rows); j++ {
				err := rel.Append(context.Background(), bats[i*int(rows)+j])
				if j < int(schema.BlockMaxRows+1) {
					assert.NoError(t, err)
				} else {
					assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
				}
			}
		}
		assert.NoError(t, txn.Commit(context.Background()))

		tae.CheckRowsByScan(bat.Length(), true)
		tae.CheckRowsByScan(bat.Length(), false)
	}
	return TestCase{
		name:      "testDelete",
		desc:      "test-5",
		dependsOn: 5,
		testFn:    testFn,
	}
}
