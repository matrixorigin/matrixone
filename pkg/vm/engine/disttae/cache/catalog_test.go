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

func TestGC(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	// insert databases
	dbBat := newTestDatabaseBatch(mp)
	cc.InsertDatabase(dbBat)
	// insert tables
	tblBat := newTestTableBatch(mp)
	colBat := newTestColumnBatch(t, tblBat, mp)
	cc.InsertTable(tblBat)
	cc.InsertColumns(colBat)
	cc.GC(timestamp.Timestamp{
		PhysicalTime: 100,
	})
	{
		timestamps := vector.MustTCols[types.TS](dbBat.GetVector(MO_TIMESTAMP_IDX))
		names := vector.MustStrCols(dbBat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
		accounts := vector.MustTCols[uint32](dbBat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
		key := new(DatabaseItem)
		for i, account := range accounts {
			key.Name = names[i]
			key.AccountId = account
			key.Ts = timestamps[i].ToTimestamp()
			ok := cc.GetDatabase(key)
			require.Equal(t, false, ok)
		}
	}
	{

		timestamps := vector.MustTCols[types.TS](tblBat.GetVector(MO_TIMESTAMP_IDX))
		accounts := vector.MustTCols[uint32](tblBat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
		names := vector.MustStrCols(tblBat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
		databaseIds := vector.MustTCols[uint64](tblBat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
		key := new(TableItem)
		for i, account := range accounts {
			key.Name = names[i]
			key.AccountId = account
			key.DatabaseId = databaseIds[i]
			key.Ts = timestamps[i].ToTimestamp()
			ok := cc.GetTable(key)
			require.Equal(t, false, ok)
		}
	}
	dbBat.Clean(mp)
	tblBat.Clean(mp)
	colBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestTables(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestTableBatch(mp)
	accounts := vector.MustTCols[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	databaseIds := vector.MustTCols[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
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
	cc.InsertTable(bat)
	tblList := cc.Tables(1, 12, timestamp.Timestamp{
		PhysicalTime: 100,
	})
	require.Equal(t, 10, len(tblList))
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDatabases(t *testing.T) {
	mp := mpool.MustNewZero()
	cc := NewCatalog()
	bat := newTestDatabaseBatch(mp)
	accounts := vector.MustTCols[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
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
	names := vector.MustTCols[types.Varlena](bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
	accounts := vector.MustTCols[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
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
	timestamps := vector.MustTCols[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	names := vector.MustStrCols(bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
	accounts := vector.MustTCols[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
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
	cc.DeleteDatabase(bat)
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
	colBat := newTestColumnBatch(t, bat, mp)
	cc.InsertTable(bat)
	cc.InsertColumns(colBat)
	timestamps := vector.MustTCols[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustTCols[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.MustStrCols(bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
	databaseIds := vector.MustTCols[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	key := new(TableItem)
	// test get
	for i, account := range accounts {
		key.Name = names[i]
		key.AccountId = account
		key.DatabaseId = databaseIds[i]
		key.Ts = timestamps[i].ToTimestamp()
		ok := cc.GetTable(key)
		require.Equal(t, true, ok)
		require.Equal(t, 11, len(key.Defs))
	}
	{ // set the deletion time
		for i := range timestamps {
			timestamps[i] = types.BuildTS(timestamps[i].Physical()+10, timestamps[i].Logical())
		}
	}
	cc.DeleteTable(bat)
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

	typs = append(typs, types.New(types.T_Rowid, 0, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0, 0))
	typs = append(typs, catalog.MoTablesTypes...)
	return testutil.NewBatch(typs, false, Rows, mp)
}

func newTestColumnBatch(t *testing.T, ibat *batch.Batch, mp *mpool.MPool) *batch.Batch {
	var typs []types.Type
	var vec *vector.Vector

	typs = append(typs, types.New(types.T_Rowid, 0, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0, 0))
	typs = append(typs, catalog.MoColumnsTypes...)
	timestamps := vector.MustTCols[types.TS](ibat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustTCols[uint32](ibat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.MustBytesCols(ibat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
	databaseIds := vector.MustTCols[uint64](ibat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	bat := batch.NewWithSize(len(typs))
	bat.SetZs(Rows, mp)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(typs[i])
	}
	for i, account := range accounts {
		for j, typ := range typs {
			switch j {
			case MO_TIMESTAMP_IDX:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(timestamps[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ACCOUNT_ID_IDX + MO_OFF:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(account, false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX + MO_OFF:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(databaseIds[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_RELNAME_IDX + MO_OFF:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(names[i], false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATTTYP_IDX + MO_OFF:
				data, err := types.Encode(typ) // reuse the type for test
				require.NoError(t, err)
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(data, false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATTHASDEF_IDX + MO_OFF:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(int8(0), false, mp)
					require.NoError(t, err)
				}
			case catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX + MO_OFF:
				vec = vector.New(typ)
				for k := 0; k < Rows; k++ {
					err := vec.Append(int8(0), false, mp)
					require.NoError(t, err)
				}
			default:
				vec = testutil.NewVector(Rows, typ, mp, false, nil)
			}
			for k := 0; k < Rows; k++ {
				err := vector.UnionOne(bat.Vecs[j], vec, int64(k), mp)
				require.NoError(t, err)
			}
			vec.Free(mp)
		}
	}
	return bat
}

func newTestDatabaseBatch(mp *mpool.MPool) *batch.Batch {
	var typs []types.Type

	typs = append(typs, types.New(types.T_Rowid, 0, 0, 0))
	typs = append(typs, types.New(types.T_TS, 0, 0, 0))
	typs = append(typs, catalog.MoDatabaseTypes...)
	return testutil.NewBatch(typs, false, Rows, mp)
}
