// Copyright 2022 Matrix Origin
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

package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

const (
	Rows = 10
)

func TestWithTableVersionHoldsCatalogChangeLockThroughCallback(t *testing.T) {
	cc := NewCatalog()
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 2, Id: 3, Name: "events", Version: 7,
	}, true)

	entered := make(chan struct{})
	release := make(chan struct{})
	type versionResult struct {
		actual         uint32
		found, matched bool
	}
	done := make(chan versionResult, 1)
	go func() {
		actual, found, matched := cc.WithTableVersion(1, 2, 3, 7, func() {
			close(entered)
			<-release
		})
		done <- versionResult{actual: actual, found: found, matched: matched}
	}()

	<-entered
	require.False(t, cc.tableChange.TryLock(),
		"catalog writers must not cross the version-check/publication boundary")
	close(release)
	result := <-done
	require.Equal(t, uint32(7), result.actual)
	require.True(t, result.found)
	require.True(t, result.matched)
	require.True(t, cc.tableChange.TryLock())
	cc.tableChange.Unlock()

	called := false
	actual, found, matched := cc.WithTableVersion(1, 2, 3, 6, func() { called = true })
	require.Equal(t, uint32(7), actual)
	require.True(t, found)
	require.False(t, matched)
	require.False(t, called)

	_, found, matched = cc.WithTableVersion(1, 2, 4, 7, func() { called = true })
	require.False(t, found)
	require.False(t, matched)
	require.False(t, called)
}

func TestCurrentTableLookupHonorsLatestTableIdentity(t *testing.T) {
	const (
		accountID  = uint32(1)
		databaseID = uint64(2)
		oldTableID = uint64(3)
		newTableID = uint64(4)
	)
	newItem := func(id uint64, version uint32, physicalTime int64, deleted bool) *TableItem {
		return &TableItem{
			AccountId: accountID, DatabaseId: databaseID, Id: id,
			Name: "events", Version: version, deleted: deleted,
			Ts: timestamp.Timestamp{PhysicalTime: physicalTime},
		}
	}

	t.Run("drop hides historical live row", func(t *testing.T) {
		cc := NewCatalog()
		cc.setTableItem(newItem(oldTableID, 7, 100, false), true)
		cc.setTableItem(newItem(oldTableID, 0, 200, true), false)

		require.Nil(t, cc.GetTableById(accountID, databaseID, oldTableID))
		require.Nil(t, cc.GetTableByName(accountID, databaseID, "events"))
		called := false
		_, found, matched := cc.WithTableVersion(
			accountID, databaseID, oldTableID, 7, func() { called = true })
		require.False(t, found)
		require.False(t, matched)
		require.False(t, called)
	})

	t.Run("truncate exposes only replacement identity", func(t *testing.T) {
		cc := NewCatalog()
		cc.setTableItem(newItem(oldTableID, 7, 100, false), true)
		cc.setTableItem(newItem(oldTableID, 0, 200, true), false)
		cc.setTableItem(newItem(newTableID, 0, 200, false), true)

		require.Nil(t, cc.GetTableById(accountID, databaseID, oldTableID))
		byName := cc.GetTableByName(accountID, databaseID, "events")
		byID := cc.GetTableById(accountID, databaseID, newTableID)
		require.NotNil(t, byName)
		require.NotNil(t, byID)
		require.Same(t, byName, byID)
		_, found, matched := cc.WithTableVersion(
			accountID, databaseID, newTableID, 0, nil)
		require.True(t, found)
		require.True(t, matched)
	})

	t.Run("alter exposes newest version of same identity", func(t *testing.T) {
		cc := NewCatalog()
		cc.setTableItem(newItem(oldTableID, 7, 100, false), true)
		cc.setTableItem(newItem(oldTableID, 0, 200, true), false)
		cc.setTableItem(newItem(oldTableID, 8, 200, false), true)

		current := cc.GetTableById(accountID, databaseID, oldTableID)
		require.NotNil(t, current)
		require.Equal(t, uint32(8), current.Version)
		_, found, matched := cc.WithTableVersion(
			accountID, databaseID, oldTableID, 7, nil)
		require.True(t, found)
		require.False(t, matched)
	})
}

func TestGetTableDefRestoresChecksFromSchemaExtra(t *testing.T) {
	check := &plan.CheckDef{Name: "t_chk_1", Check: &plan.Expr{}}
	tableDef, _ := getTableDef(&TableItem{
		Name: "t",
		ExtraInfo: &api.SchemaExtra{
			Checks:         []*plan.CheckDef{check},
			DefaultCharset: uint32(types.CharsetBinary),
		},
	}, nil)
	require.Equal(t, []*plan.CheckDef{check}, tableDef.Checks)
	require.Equal(t, uint32(types.CharsetBinary), tableDef.DefaultCharset)
}

func TestGetTableDefKeepsTemporarySessionStateContextual(t *testing.T) {
	tableDef, _ := getTableDef(&TableItem{Kind: catalog.SystemTemporaryTable}, nil)
	require.NotNil(t, tableDef)
	require.Equal(t, catalog.SystemTemporaryTable, tableDef.TableType)
	require.False(t, tableDef.IsTemporary)
}

func TestCatalogCacheConcurrentGC(t *testing.T) {
	cc := NewCatalog()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(ts int64) {
			defer wg.Done()
			cc.GC(timestamp.Timestamp{PhysicalTime: ts})
		}(int64(i + 1))
	}
	wg.Wait()
}

func TestCrossDBGet(t *testing.T) {
	cc := NewCatalog()
	cc.tables.data.Set(&TableItem{
		AccountId:  1,
		DatabaseId: 272885,
		Name:       "customer",
		Ts:         timestamp.Timestamp{PhysicalTime: 100},
	})
	cc.tables.data.Set(&TableItem{
		AccountId:  1,
		DatabaseId: 272885,
		Name:       "date",
		Ts:         timestamp.Timestamp{PhysicalTime: 110},
	})
	cc.tables.data.Set(&TableItem{
		AccountId:  1,
		DatabaseId: 272885,
		Name:       "lineorder",
		Ts:         timestamp.Timestamp{PhysicalTime: 120},
	})
	require.False(t, cc.GetTable(&TableItem{
		AccountId:  1,
		DatabaseId: 272817,
		Name:       "customer",
		Ts:         timestamp.Timestamp{PhysicalTime: 200},
	}))
	require.True(t, cc.GetTable(&TableItem{
		AccountId:  1,
		DatabaseId: 272885,
		Name:       "customer",
		Ts:         timestamp.Timestamp{PhysicalTime: 200},
	}))
}

func TestCrossAccGet(t *testing.T) {
	cc := NewCatalog()
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1,
		Name:      "ssb_1g",
		Ts:        timestamp.Timestamp{PhysicalTime: 100},
	})
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1,
		Name:      "tpch_1g",
		Ts:        timestamp.Timestamp{PhysicalTime: 110},
	})
	require.False(t, cc.GetDatabase(&DatabaseItem{
		AccountId: 0,
		Name:      "ssb_1g",
		Ts:        timestamp.Timestamp{PhysicalTime: 200},
	}))
	require.True(t, cc.GetDatabase(&DatabaseItem{
		AccountId: 1,
		Name:      "ssb_1g",
		Ts:        timestamp.Timestamp{PhysicalTime: 200},
	}))
}

func TestHasNewerVersion(t *testing.T) {
	cc := NewCatalog()
	cc.tables.data.Set(&TableItem{
		AccountId:  1,
		DatabaseId: 2,
		Name:       "t",
		Id:         3,
		Version:    2,
		Ts:         timestamp.Timestamp{PhysicalTime: 200},
	})

	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 2, Name: "t", TableId: 3, Version: 1,
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 2, Name: "t", TableId: 3, Version: 2,
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 2, Name: "t", TableId: 3, Version: 1,
		Ts: timestamp.Timestamp{PhysicalTime: 300},
	}))
}

func TestHasNewerVersionDetectsDatabaseRecreation(t *testing.T) {
	cc := NewCatalog()
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1, Name: "db", Id: 20, Ts: timestamp.Timestamp{PhysicalTime: 200},
	})
	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "missing",
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 20, DatabaseName: "db", Name: "missing",
		Ts: timestamp.Timestamp{PhysicalTime: 300},
	}))
}

func TestHasNewerVersionDetectsAnyTableChange(t *testing.T) {
	cc := NewCatalog()
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "new_child", Id: 20,
		Ts: timestamp.Timestamp{PhysicalTime: 200},
	}, true)
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 300},
	}))
	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 2, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))

	// Account-level dependencies use the high-watermark rather than walking
	// retained table versions. A direct BTree insertion intentionally bypasses
	// the production update path and therefore must not affect the result.
	cc.tables.data.Set(&TableItem{
		AccountId: 2, DatabaseId: 20, Name: "retained", Id: 30,
		Ts: timestamp.Timestamp{PhysicalTime: 500},
	})
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 2, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))

	// Deletes advance the same account high-watermark.
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "new_child", Id: 20, deleted: true,
		Ts: timestamp.Timestamp{PhysicalTime: 400},
	}, false)
	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 300},
	}))
}

func TestAccountTableChangeHighWatermarkConcurrent(t *testing.T) {
	cc := NewCatalog()
	var wg sync.WaitGroup
	for i := 1; i <= 64; i++ {
		wg.Add(1)
		go func(physicalTime int64) {
			defer wg.Done()
			cc.setTableItem(&TableItem{
				AccountId: 1, DatabaseId: uint64(physicalTime), Name: "t",
				Ts: timestamp.Timestamp{PhysicalTime: physicalTime},
			}, true)
		}(int64(i))
	}
	wg.Wait()

	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 63},
	}))
	require.False(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1, DatabaseId: 0, Name: "", Ts: timestamp.Timestamp{PhysicalTime: 64},
	}))
}

func TestPreparedMetadataHighWatermark(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := newTestTableBatch(mp)
	defer bat.Clean(mp)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))

	cc := NewCatalog()
	cc.UpdatePreparedMetadata(bat)

	var expected timestamp.Timestamp
	for _, ts := range timestamps {
		value := ts.ToTimestamp()
		if value.Greater(expected) {
			expected = value
		}
	}
	require.Equal(t, expected, cc.GetPreparedMetadataTS())
}

func TestAccountTableChangeHighWatermarkCollisionIsConservative(t *testing.T) {
	cc := NewCatalog()
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t",
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}, true)

	// A colliding account may rebuild conservatively, but it must never miss
	// the bucket's latest table change.
	require.True(t, cc.HasNewerVersion(&TableChangeQuery{
		AccountId: 1 + tableChangeBucketCount, DatabaseId: 0,
		Ts: timestamp.Timestamp{PhysicalTime: 1},
	}))
}

func TestTables(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestTableBatch(mp)
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	extraInfos := vector.MustFixedColWithTypeCheck[types.Varlena](bat.GetVector(catalog.MO_TABLES_EXTRA_INFO_IDX + MO_OFF))
	{ // reset account id
		for i := range accounts {
			accounts[i] = 1
		}
	}
	{ // reset database id
		for i := range databaseIds {
			databaseIds[i] = 12
		}
	}
	empty, _, _ := types.BuildVarlena([]byte{}, nil, nil)
	{
		for i := range extraInfos {
			extraInfos[i] = empty
		}
	}
	cc.InsertTable(bat)
	tblList, tblIdList := cc.Tables(1, 12, timestamp.Timestamp{
		PhysicalTime: 100,
	})
	require.Equal(t, 10, len(tblList))
	require.Equal(t, 10, len(tblIdList))
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDatabases(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestDatabaseBatch(mp)
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	{ // reset account id
		for i := range accounts {
			accounts[i] = 0
		}
	}
	cc.InsertDatabase(bat)
	// test get
	dbList := cc.Databases(0, timestamp.Timestamp{
		PhysicalTime: 100,
	})
	require.Equal(t, 10, len(dbList))
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDatabasesWithMultiVersion(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestDatabaseBatch(mp)
	names := vector.MustFixedColWithTypeCheck[types.Varlena](bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	{ // reset account id
		for i := range accounts {
			accounts[i] = 0
		}
	}
	{ // reset names
		name := []byte{'0'}
		for i := range names {
			names[i], _, _ = types.BuildVarlena(name, nil, nil)
		}
	}
	cc.InsertDatabase(bat)
	// test get
	dbList := cc.Databases(0, timestamp.Timestamp{
		PhysicalTime: 100,
	})
	require.Equal(t, 1, len(dbList)) // only one version can be see
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDatabaseCache(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestDatabaseBatch(mp)
	cc.InsertDatabase(bat)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	names := vector.InefficientMustStrCol(bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	key := new(DatabaseItem)
	// test get
	for i, account := range accounts {
		key.Name = names[i]
		key.AccountId = account
		key.Ts = timestamps[i].ToTimestamp()
		ok := cc.GetDatabase(key)
		require.Equal(t, true, ok)
	}
	{ // set the deletion time
		for i := range timestamps {
			timestamps[i] = types.BuildTS(timestamps[i].Physical()+10, timestamps[i].Logical())
		}
	}
	{
		delBat := batch.NewWithSize(3)
		delBat.Vecs[0] = bat.Vecs[0]
		delBat.Vecs[1] = bat.Vecs[1]
		delBat.Vecs[2] = bat.Vecs[catalog.MO_DATABASE_CPKEY_IDX+MO_OFF]
		cc.DeleteDatabase(delBat)
	}

	// test delete
	for i, account := range accounts {
		key.Name = names[i]
		key.AccountId = account
		key.Ts = timestamps[i].ToTimestamp()
		ok := cc.GetDatabase(key)
		require.Equal(t, false, ok)
	}
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestTableInsert(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestTableBatch(mp)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.InefficientMustStrCol(bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
	databaseIds := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))

	cstrs := vector.MustFixedColWithTypeCheck[types.Varlena](bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + MO_OFF))
	partitioned := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + MO_OFF))
	extras := vector.MustFixedColWithTypeCheck[types.Varlena](bat.GetVector(catalog.MO_TABLES_EXTRA_INFO_IDX + MO_OFF))
	empty, _, _ := types.BuildVarlena([]byte{}, nil, nil)
	for i := range accounts {
		// avoid unmarshal error
		cstrs[i] = empty
		extras[i] = empty
		partitioned[i] = 0
	}

	colBat := newTestColumnBatch(t, bat, mp)
	cc.InsertTable(bat)
	cc.InsertColumns(colBat)

	key := new(TableItem)
	// test get
	for i, account := range accounts {
		key.Name = names[i]
		key.AccountId = account
		key.DatabaseId = databaseIds[i]
		key.Ts = timestamps[i].ToTimestamp()
		ok := cc.GetTable(key)
		require.Equal(t, true, ok)
		require.Equal(t, 13, len(key.Defs), i)
	}
	{ // set the deletion time
		for i := range timestamps {
			timestamps[i] = types.BuildTS(timestamps[i].Physical()+10, timestamps[i].Logical())
		}
	}

	{
		delBat := batch.NewWithSize(3)
		delBat.Vecs[0] = bat.Vecs[0]
		delBat.Vecs[1] = bat.Vecs[1]
		delBat.Vecs[2] = bat.Vecs[catalog.MO_TABLES_CPKEY_IDX+MO_OFF]
		cc.DeleteTable(delBat)
	}

	{ // set the query time
		for i := range timestamps {
			timestamps[i] = types.BuildTS(timestamps[i].Physical()+10, timestamps[i].Logical())
		}
	}
	// test delete
	for i, account := range accounts {
		key.Name = names[i]
		key.AccountId = account
		key.DatabaseId = databaseIds[i]
		key.Ts = timestamps[i].ToTimestamp()
		ok := cc.GetTable(key)
		require.Equal(t, false, ok)
	}
	bat.Clean(mp)
	colBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestParseColumnsBatchPreservesUnsignedFlag(t *testing.T) {
	mp := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	typ := types.T_uint64.ToType()
	typBytes, err := types.Encode(&typ)
	require.NoError(t, err)

	columnBatch, err := catalog.GenCreateColumnTuples([]catalog.Column{{
		AccountId:    7,
		DatabaseId:   8,
		TableId:      9,
		DatabaseName: "issue_27661",
		TableName:    "unsigned_flags",
		Name:         "unsigned_bigint",
		Typ:          typBytes,
		TypLen:       int32(len(typBytes)),
		Num:          1,
		IsUnsigned:   1,
	}}, mp, packer)
	require.NoError(t, err)

	logtailBatch := batch.NewWithSize(len(columnBatch.Vecs) + MO_OFF)
	logtailBatch.Vecs[MO_ROWID_IDX] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(logtailBatch.Vecs[MO_ROWID_IDX], types.Rowid{}, false, mp))
	logtailBatch.Vecs[MO_TIMESTAMP_IDX] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(logtailBatch.Vecs[MO_TIMESTAMP_IDX], types.BuildTS(1, 0), false, mp))
	copy(logtailBatch.Vecs[MO_OFF:], columnBatch.Vecs)
	logtailBatch.SetRowCount(1)
	defer logtailBatch.Clean(mp)

	ParseColumnsBatchAnd(logtailBatch, func(columnsByTable map[TableItemKey]Columns) {
		require.Len(t, columnsByTable, 1)
		for _, columns := range columnsByTable {
			require.Len(t, columns, 1)
			require.Equal(t, int8(1), columns[0].IsUnsigned)
			var decoded types.Type
			require.NoError(t, types.Decode(columns[0].Typ, &decoded))
			require.Equal(t, types.T_uint64, decoded.Oid)
		}
	})
}

func newTestTableBatch(mp *mpool.MPool) *batch.Batch {
	var typs []types.Type

	typs = append(typs, types.New(types.T_Rowid, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0))
	typs = append(typs, catalog.MoTablesTypes...)
	return testutil.NewBatch(typs, false, Rows, mp)
}

func newTestColumnBatch(t *testing.T, ibat *batch.Batch, mp *mpool.MPool) *batch.Batch {
	var typs []types.Type
	var vec *vector.Vector

	typs = append(typs, types.New(types.T_Rowid, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0))
	typs = append(typs, catalog.MoColumnsTypes...)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](ibat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedColWithTypeCheck[uint32](ibat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.InefficientMustBytesCol(ibat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
	ids := vector.MustFixedColWithTypeCheck[uint64](ibat.GetVector(catalog.MO_TABLES_REL_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedColWithTypeCheck[uint64](ibat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	bat := batch.NewWithSize(len(typs))
	bat.SetRowCount(Rows)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(typs[i])
	}
	for i, account := range accounts {
		for j, typ := range typs {
			switch j {
			case MO_TIMESTAMP_IDX:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, timestamps[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ACCOUNT_ID_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, account, false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, databaseIds[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, ids[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_RELNAME_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendBytes(vec, names[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATTTYP_IDX + MO_OFF:
				data, err := types.Encode(&typ) // reuse the type for test
				require.NoError(t, err)
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendBytes(vec, data, false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATTHASDEF_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, int8(0), false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, int8(0), false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendFixed(vec, int8(0), false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_GENERATED_IDX + MO_OFF:
				vec = vector.NewVec(typ)
				for k := 0; k < Rows; k++ {
					err := vector.AppendBytes(vec, []byte(""), false, mp)
					require.NoError(t, err)
				}
			default:
				vec = testutil.NewVector(Rows, typ, mp, false, nil)
			}
			for k := 0; k < Rows; k++ {
				err := bat.Vecs[j].UnionOne(vec, int64(k), mp)
				require.NoError(t, err)
			}
			vec.Free(mp)
		}
	}
	return bat
}

func newTestDatabaseBatch(mp *mpool.MPool) *batch.Batch {
	var typs []types.Type

	typs = append(typs, types.New(types.T_Rowid, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0))
	typs = append(typs, catalog.MoDatabaseTypes...)
	return testutil.NewBatch(typs, false, Rows, mp)
}
