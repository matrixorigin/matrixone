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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

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
