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

import "testing"

func init() {
	//PrepareCaseRegister(MakePrepareCase(
	//	prepare1, "prepare-1", "prepare case 1",
	//	schemaCfg{10, 2, 18, 13}, 10*3+1, longOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(test1, "prepare-1", "test-1", "prepare-1=>test-1"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareDDL, "prepare-2", "prepare case ddl",
	//	schemaCfg{10, 2, 18, 13}, 10*3+1, longOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(testDDL, "prepare-2", "test-2", "prepare-2=>test-2"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareCompact, "prepare-3", "prepare case compact", schemaCfg{10, 2, 18, 13}, (10*3+1)*2, longOpt))
	//TestCaseRegister(
	//	MakeTestCase(testCompact, "prepare-3", "test-3", "prepare-3=>test-3"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareDelete, "prepare-4", "prepare case delete",
	//	schemaCfg{10, 2, 18, 13}, (10*3+1)*3, longOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(testDelete, "prepare-4", "test-4", "prepare-4=>test-4"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareAppend, "prepare-5", "prepare case append",
	//	schemaCfg{10, 2, 18, 13}, 10*3+1, longOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(testAppend, "prepare-5", "test-5", "prepare-5=>test-5"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareLSNCheck, "prepare-6", "prepare lsn check",
	//	schemaCfg{10, 2, 18, 13}, 10*3+1, quickOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(testLSNCheck, "prepare-6", "test-6", "prepare-6=>test-6"),
	//)
	//
	//PrepareCaseRegister(MakePrepareCase(
	//	prepareObjectInfo, "prepare-7", "prepare object info",
	//	schemaCfg{10, 2, 18, 13}, 51, longOpt,
	//))
	//TestCaseRegister(
	//	MakeTestCase(testObjectInfo, "prepare-7", "test-7", "prepare-7=>test-7"),
	//)

	PrepareCaseRegister(MakePrepareCase(
		prepareVector, "prepare-8", "prepare case append",
		schemaCfg{10, 2, 18, 13}, 10*3+1, longOpt,
	))
	TestCaseRegister(
		MakeTestCase(testVector, "prepare-8", "test-8", "prepare-8=>test-8"),
	)
}

//
//func prepare1(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	schema := tc.GetSchema(t)
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(4)
//
//	tae.CreateRelAndAppend(bats[0], true)
//	txn, rel := tae.GetRelation()
//	v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
//	filter := handle.NewEQFilter(v)
//	err := rel.DeleteByFilter(context.Background(), filter)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, rel = tae.GetRelation()
//	window := bat.CloneWindow(2, 1)
//	defer window.Close()
//	err = rel.Append(context.Background(), window)
//	assert.NoError(t, err)
//	_ = txn.Rollback(context.Background())
//}
//
//func prepareAppend(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(2)
//
//	// checkpoint
//	tae.CreateRelAndAppend(bats[0], true)
//	tae.ForceCheckpoint()
//
//	// wal
//	tae.DoAppend(bats[1])
//}
//
//func prepareDDL(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	// ckp1: create db1, create table1
//	txn, err := tae.StartTxn(nil)
//	assert.NoError(t, err)
//	db, err := txn.CreateDatabase("db1", "sql", "type")
//	assert.NoError(t, err)
//	schema1 := catalog.MockSchema(20, 2)
//	schema1.Name = "table1"
//	_, err = db.CreateRelation(schema1)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//	tae.ForceCheckpoint()
//
//	// ckp2-1: create db2, create table2
//	// ckp2-2: drop db2, drop table2
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.CreateDatabase("db2", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.CreateDatabase("db_of_table2", "sql", "type")
//	assert.NoError(t, err)
//	schema2 := catalog.MockSchema(20, 2)
//	schema2.Name = "table2"
//	_, err = db.CreateRelation(schema2)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//	tae.ForceCheckpoint()
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table2")
//	assert.NoError(t, err)
//	_, err = db.DropRelationByName(schema2.Name)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.DropDatabase("db2")
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	tae.ForceCheckpoint()
//
//	// ckp3: create and drop db3, table3
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.CreateDatabase("db3", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.CreateDatabase("db_of_table3", "sql", "type")
//	assert.NoError(t, err)
//	schema3 := catalog.MockSchema(20, 2)
//	schema3.Name = "table3"
//	_, err = db.CreateRelation(schema3)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table3")
//	assert.NoError(t, err)
//	_, err = db.DropRelationByName(schema3.Name)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.DropDatabase("db3")
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	tae.ForceCheckpoint()
//
//	// WAL: create db4, create table4
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	db, err = txn.CreateDatabase("db4", "sql", "type")
//	assert.NoError(t, err)
//	schema4 := catalog.MockSchema(20, 2)
//	schema4.Name = "table4"
//	_, err = db.CreateRelation(schema4)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	// WAL: create and drop db5, create ane drop table5
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.CreateDatabase("db5", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.CreateDatabase("db_of_table5", "sql", "type")
//	assert.NoError(t, err)
//	schema5 := catalog.MockSchema(20, 2)
//	schema5.Name = "table5"
//	_, err = db.CreateRelation(schema5)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table5")
//	assert.NoError(t, err)
//	_, err = db.DropRelationByName(schema5.Name)
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, err = tae.StartTxn(nil)
//	assert.NoError(t, err)
//	_, err = txn.DropDatabase("db5")
//	assert.NoError(t, err)
//	assert.NoError(t, txn.Commit(context.Background()))
//}
//
//func prepareCompact(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(2)
//
//	// checkpoint
//	tae.CreateRelAndAppend(bats[0], true)
//	tae.CompactBlocks(false)
//	tae.ForceCheckpoint()
//
//	// wal
//	tae.DoAppend(bats[1])
//	tae.CompactBlocks(false)
//}
//
//func prepareDelete(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(3)
//
//	// compact and checkpoint
//	tae.CreateRelAndAppend(bats[0], true)
//
//	schema := tc.GetSchema(t)
//	for i := 0; i < int(schema.Extra.BlockMaxRows+1); i++ {
//		txn, rel := tae.GetRelation()
//		v := testutil.GetSingleSortKeyValue(bats[0], schema, i)
//		filter := handle.NewEQFilter(v)
//		err := rel.DeleteByFilter(context.Background(), filter)
//		assert.NoError(t, err)
//		assert.NoError(t, txn.Commit(context.Background()))
//	}
//
//	tae.CompactBlocks(false)
//	tae.ForceCheckpoint()
//
//	// compact
//	tae.DoAppend(bats[1])
//	for i := 0; i < int(schema.Extra.BlockMaxRows+1); i++ {
//		txn, rel := tae.GetRelation()
//		v := testutil.GetSingleSortKeyValue(bats[1], schema, i)
//		filter := handle.NewEQFilter(v)
//		err := rel.DeleteByFilter(context.Background(), filter)
//		assert.NoError(t, err)
//		assert.NoError(t, txn.Commit(context.Background()))
//	}
//	tae.CompactBlocks(false)
//
//	// not compact
//	tae.DoAppend(bats[2])
//	for i := 0; i < int(schema.Extra.BlockMaxRows+1); i++ {
//		txn, rel := tae.GetRelation()
//		v := testutil.GetSingleSortKeyValue(bats[2], schema, i)
//		filter := handle.NewEQFilter(v)
//		err := rel.DeleteByFilter(context.Background(), filter)
//		assert.NoError(t, err)
//		assert.NoError(t, txn.Commit(context.Background()))
//	}
//}
//
//func prepareLSNCheck(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(3)
//
//	// incremental ckp
//	tae.CreateRelAndAppend(bats[0], true)
//
//	testutils.WaitExpect(10000, func() bool {
//		return tae.Wal.GetPenddingCnt() == 0
//	})
//
//	// force incremental ckp
//	tae.DoAppend(bats[1])
//	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
//	err := tae.DB.ForceCheckpoint(context.Background(), ts, time.Minute)
//	assert.NoError(t, err)
//}
//
//func test1(tc TestCase, t *testing.T) {
//	pc := GetPrepareCase(tc.dependsOn)
//	tae := tc.GetEngine(t)
//
//	bat := pc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(4)
//	window := bat.CloneWindow(2, 1)
//	defer window.Close()
//
//	txn, rel := tae.GetRelation()
//	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
//	err := rel.Append(context.Background(), bats[0])
//	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, rel = tae.GetRelation()
//	err = rel.Append(context.Background(), window)
//	assert.NoError(t, err)
//	_ = txn.Rollback(context.Background())
//
//	schema := pc.GetSchema(t)
//	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
//	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
//	err = rel.Append(context.Background(), window)
//	assert.NoError(t, err)
//	_ = txn.Rollback(context.Background())
//	tae.CheckReadCNCheckpoint()
//}
//
//func testAppend(tc TestCase, t *testing.T) {
//	pc := GetPrepareCase(tc.dependsOn)
//	tae := initTestEngine(tc, t)
//	defer tae.Close()
//
//	bat := pc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(bat.Length())
//
//	txn, rel := tae.GetRelation()
//	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
//	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
//	for _, b := range bats {
//		err := rel.Append(context.Background(), b)
//		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
//	}
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	tae.CheckRowsByScan(bat.Length(), true)
//	tae.CheckRowsByScan(bat.Length(), false)
//	tae.CheckReadCNCheckpoint()
//}
//
//func testDDL(tc TestCase, t *testing.T) {
//	tae := initTestEngine(tc, t)
//	defer tae.Close()
//
//	txn, err := tae.StartTxn(nil)
//	assert.NoError(t, err)
//
//	db, err := txn.GetDatabase("db1")
//	assert.NoError(t, err)
//	_, err = txn.CreateDatabase("db1", "sql", "type")
//	assert.Error(t, err)
//	_, err = db.GetRelationByName("table1")
//	assert.NoError(t, err)
//	schema1 := catalog.MockSchema(20, 2)
//	schema1.Name = "table1"
//	_, err = db.CreateRelation(schema1)
//	assert.Error(t, err)
//
//	_, err = txn.GetDatabase("db2")
//	assert.Error(t, err)
//	_, err = txn.CreateDatabase("db2", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table2")
//	assert.NoError(t, err)
//	_, err = db.GetRelationByName("table2")
//	assert.Error(t, err)
//	schema2 := catalog.MockSchema(20, 2)
//	schema2.Name = "table2"
//	_, err = db.CreateRelation(schema2)
//	assert.NoError(t, err)
//
//	_, err = txn.GetDatabase("db3")
//	assert.Error(t, err)
//	_, err = txn.CreateDatabase("db3", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table3")
//	assert.NoError(t, err)
//	_, err = db.GetRelationByName("table3")
//	assert.Error(t, err)
//	schema3 := catalog.MockSchema(20, 2)
//	schema3.Name = "table3"
//	_, err = db.CreateRelation(schema3)
//	assert.NoError(t, err)
//
//	db, err = txn.GetDatabase("db4")
//	assert.NoError(t, err)
//	_, err = txn.CreateDatabase("db4", "sql", "type")
//	assert.Error(t, err)
//	_, err = db.GetRelationByName("table4")
//	assert.NoError(t, err)
//	schema4 := catalog.MockSchema(20, 2)
//	schema4.Name = "table4"
//	_, err = db.CreateRelation(schema4)
//	assert.Error(t, err)
//
//	_, err = txn.GetDatabase("db5")
//	assert.Error(t, err)
//	_, err = txn.CreateDatabase("db5", "sql", "type")
//	assert.NoError(t, err)
//	db, err = txn.GetDatabase("db_of_table5")
//	assert.NoError(t, err)
//	_, err = db.GetRelationByName("table5")
//	assert.Error(t, err)
//	schema5 := catalog.MockSchema(20, 2)
//	schema5.Name = "table5"
//	_, err = db.CreateRelation(schema5)
//	assert.NoError(t, err)
//
//	assert.NoError(t, txn.Commit(context.Background()))
//	tae.CheckReadCNCheckpoint()
//}
//
//func testCompact(tc TestCase, t *testing.T) {
//	pc := GetPrepareCase(tc.dependsOn)
//	tae := initTestEngine(tc, t)
//	defer tae.Close()
//
//	bat := pc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(bat.Length())
//
//	txn, rel := tae.GetRelation()
//	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
//	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
//	for _, b := range bats {
//		err := rel.Append(context.Background(), b)
//		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
//	}
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	tae.CheckRowsByScan(bat.Length(), true)
//	tae.CheckRowsByScan(bat.Length(), false)
//	tae.CheckReadCNCheckpoint()
//}
//
//func testDelete(tc TestCase, t *testing.T) {
//	pc := GetPrepareCase(tc.dependsOn)
//	tae := initTestEngine(tc, t)
//	defer tae.Close()
//
//	bat := pc.GetBatch(t)
//	defer bat.Close()
//	bats := bat.Split(bat.Length())
//
//	schema := pc.GetSchema(t)
//	totalRows := bat.Length() - int(schema.Extra.BlockMaxRows+1)*3
//	txn, rel := tae.GetRelation()
//	testutil.CheckAllColRowsByScan(t, rel, totalRows, true)
//	rows := schema.Extra.BlockMaxRows*3 + 1
//	for i := 0; i < 3; i++ {
//		for j := 0; j < int(rows); j++ {
//			err := rel.Append(context.Background(), bats[i*int(rows)+j])
//			if j < int(schema.Extra.BlockMaxRows+1) {
//				assert.NoError(t, err)
//			} else {
//				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
//			}
//		}
//	}
//	assert.NoError(t, txn.Commit(context.Background()))
//
//	tae.CheckRowsByScan(bat.Length(), true)
//
//	for i := 0; i < bat.Length(); i++ {
//		txn, rel = tae.GetRelation()
//		v := testutil.GetSingleSortKeyValue(bats[i], schema, 0)
//		filter := handle.NewEQFilter(v)
//		err := rel.DeleteByFilter(context.Background(), filter)
//		assert.NoError(t, err)
//		assert.NoError(t, txn.Commit(context.Background()))
//	}
//
//	tae.CheckCollectTombstoneInRange()
//
//	tae.CheckRowsByScan(0, true)
//	tae.CompactBlocks(false)
//
//	tae.CheckRowsByScan(0, true)
//	tae.CheckReadCNCheckpoint()
//}
//
//func testLSNCheck(tc TestCase, t *testing.T) {
//	tae := initTestEngine(tc, t)
//	tae.Close()
//}
//
//func prepareObjectInfo(tc PrepareCase, t *testing.T) {
//	tae := tc.GetEngine(t)
//	defer tae.Close()
//	bat := tc.GetBatch(t)
//	defer bat.Close()
//
//	// checkpoint
//	tae.CreateRelAndAppend(bat, true)
//	tae.CompactBlocks(false)
//	tae.MergeBlocks(false)
//}
//
//func testObjectInfo(tc TestCase, t *testing.T) {
//	tae := initTestEngine(tc, t)
//	defer tae.Close()
//	t.Log(tae.Catalog.SimplePPString(3))
//
//	tae.ForceCheckpoint()
//	tae.Restart(context.TODO())
//	t.Log(tae.Catalog.SimplePPString(3))
//}

func prepareVector(tc PrepareCase, t *testing.T) {
	tae := tc.GetEngine(t)
	defer tae.Close()

	bat := tc.GetBatch(t)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
}

func testVector(tc TestCase, t *testing.T) {
	pc := GetPrepareCase(tc.dependsOn)
	tae := initTestEngine(tc, t)
	defer tae.Close()

	bat := pc.GetBatch(t)
	defer bat.Close()

	tae.CheckRowsByScan(bat.Length(), true)
}
