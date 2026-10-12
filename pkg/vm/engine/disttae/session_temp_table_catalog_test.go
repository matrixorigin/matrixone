// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

const (
	tempCatalogAccountID  = uint32(7)
	tempCatalogDatabaseID = uint64(10)
	tempCatalogDatabase   = "db_with_parts"
	tempCatalogParentID   = uint64(9000001)
	tempCatalogChildID    = uint64(9000002)
)

var (
	tempCatalogSnapshot = types.BuildTS(100, 0)
	tempCatalogCommit   = types.BuildTS(200, 0)
	tempCatalogSession  = uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	tempCatalogSession2 = uuid.MustParse("ffeeddcc-bbaa-9988-7766-554433221100")
)

type tempCatalogWorkspaceState struct {
	snapshot      timestamp.Timestamp
	writes        int
	workspaceSize uint64
	statementID   int
	tableOps      int
	createdTables int
	databaseOps   int
}

func newTempCatalogFixture(t *testing.T) (*Engine, client.TxnOperator, *Transaction, context.Context) {
	t.Helper()
	eng := &Engine{}
	eng.catalog.Store(cache.NewCatalog())
	op, txn := newResetTxnForTest(t, eng)
	op.SetSnapshotTS(tempCatalogSnapshot.ToTimestamp())
	eng.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())
	require.True(t, eng.GetLatestCatalogCache().CanServe(tempCatalogSnapshot))
	return eng, op, txn, defines.AttachAccountId(context.Background(), tempCatalogAccountID)
}

func insertTempCatalogDatabase(t *testing.T, eng *Engine, txn *Transaction, ts types.TS) {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	bat, err := catalog.GenCreateDatabaseTuple(
		"create database "+tempCatalogDatabase,
		tempCatalogAccountID, 1, 1, tempCatalogDatabase, tempCatalogDatabaseID, "",
		txn.proc.Mp(), packer,
	)
	require.NoError(t, err)
	_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
	require.NoError(t, err)
	vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))[0] = ts
	eng.GetLatestCatalogCache().InsertDatabase(bat)
	t.Cleanup(func() { bat.Clean(txn.proc.Mp()) })
}

func insertTempCatalogTable(t *testing.T, eng *Engine, txn *Transaction, item catalog.Table, ts types.TS) {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	bat, err := catalog.GenCreateTableTuple(item, txn.proc.Mp(), packer)
	require.NoError(t, err)
	_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
	require.NoError(t, err)
	vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))[0] = ts
	eng.GetLatestCatalogCache().InsertTable(bat)
	t.Cleanup(func() { bat.Clean(txn.proc.Mp()) })
}

func tempCatalogRoot(session uuid.UUID, childID uint64) catalog.Table {
	return catalog.Table{
		AccountId: tempCatalogAccountID, DatabaseId: tempCatalogDatabaseID,
		DatabaseName: tempCatalogDatabase, TableId: tempCatalogParentID,
		TableName: defines.GenTempTableName(session, tempCatalogDatabase, "base_table"),
		Kind:      catalog.SystemTemporaryTable,
		ExtraInfo: api.MustMarshalTblExtra(&api.SchemaExtra{IndexTables: []uint64{childID}}),
	}
}

func tempCatalogIndex(session uuid.UUID, id uint64, alias, kind string) catalog.Table {
	return catalog.Table{
		AccountId: tempCatalogAccountID, DatabaseId: tempCatalogDatabaseID,
		DatabaseName: tempCatalogDatabase, TableId: id,
		TableName: defines.GenTempTableName(session, tempCatalogDatabase, alias),
		Kind:      kind,
		ExtraInfo: api.MustMarshalTblExtra(&api.SchemaExtra{ParentTableID: tempCatalogParentID}),
	}
}

func captureTempCatalogWorkspaceState(op client.TxnOperator, txn *Transaction) tempCatalogWorkspaceState {
	return tempCatalogWorkspaceState{
		snapshot: op.SnapshotTS(), writes: len(txn.writes), workspaceSize: txn.workspaceSize,
		statementID: txn.statementID, tableOps: len(txn.tableOps.names),
		createdTables: len(txn.tableOps.creatdInTxn), databaseOps: len(txn.databaseOps.names),
	}
}

func assertTempCatalogWorkspaceUnchanged(t *testing.T, before tempCatalogWorkspaceState, op client.TxnOperator, txn *Transaction) {
	t.Helper()
	require.Equal(t, before, captureTempCatalogWorkspaceState(op, txn))
}

func TestCommittedTemporaryIndexCatalogVisibility(t *testing.T) {
	eng, op, txn, ctx := newTempCatalogFixture(t)
	insertTempCatalogDatabase(t, eng, txn, tempCatalogSnapshot)
	indexName := defines.GenTempTableName(tempCatalogSession, tempCatalogDatabase, "unique_index")
	insertTempCatalogTable(t, eng, txn, tempCatalogRoot(tempCatalogSession, tempCatalogChildID), tempCatalogCommit)
	insertTempCatalogTable(t, eng, txn, tempCatalogIndex(tempCatalogSession, tempCatalogChildID, "unique_index", catalog.SystemIndexRel), tempCatalogCommit)

	db := &txnDatabase{
		op: op, accountId: tempCatalogAccountID,
		databaseId: tempCatalogDatabaseID, databaseName: tempCatalogDatabase,
	}
	before := captureTempCatalogWorkspaceState(op, txn)
	item, err := db.getTableItem(ctx, tempCatalogAccountID, indexName, eng)
	require.NoError(t, err)
	require.NotNil(t, item)
	require.Equal(t, tempCatalogChildID, item.Id)
	assertTempCatalogWorkspaceUnchanged(t, before, op, txn)

	_, _, rel, err := eng.GetRelationById(ctx, op, tempCatalogChildID)
	require.NoError(t, err)
	require.NotNil(t, rel)
	require.Equal(t, tempCatalogChildID, rel.GetTableID(ctx))
	assertTempCatalogWorkspaceUnchanged(t, before, op, txn)
}

func TestTemporaryIndexIDLookupRejectsSnapshotNameIdentityMismatch(t *testing.T) {
	eng, op, txn, ctx := newTempCatalogFixture(t)
	insertTempCatalogDatabase(t, eng, txn, tempCatalogSnapshot)
	name := defines.GenTempTableName(tempCatalogSession, tempCatalogDatabase, "unique_index")
	newID := tempCatalogChildID + 1

	insertTempCatalogTable(t, eng, txn, tempCatalogRoot(tempCatalogSession, tempCatalogChildID), tempCatalogSnapshot)
	insertTempCatalogTable(t, eng, txn, tempCatalogIndex(tempCatalogSession, tempCatalogChildID, "unique_index", catalog.SystemIndexRel), tempCatalogSnapshot)
	insertTempCatalogTable(t, eng, txn, tempCatalogRoot(tempCatalogSession, newID), tempCatalogCommit)
	insertTempCatalogTable(t, eng, txn, tempCatalogIndex(tempCatalogSession, newID, "unique_index", catalog.SystemIndexRel), tempCatalogCommit)

	db := &txnDatabase{
		op: op, accountId: tempCatalogAccountID,
		databaseId: tempCatalogDatabaseID, databaseName: tempCatalogDatabase,
	}
	before := captureTempCatalogWorkspaceState(op, txn)
	nameItem, err := db.getTableItem(ctx, tempCatalogAccountID, name, eng)
	require.NoError(t, err)
	require.NotNil(t, nameItem)
	require.Equal(t, tempCatalogChildID, nameItem.Id, "name lookup must retain the snapshot-visible ID")
	assertTempCatalogWorkspaceUnchanged(t, before, op, txn)

	_, _, rel, err := eng.GetRelationById(ctx, op, newID)
	require.ErrorContains(t, err, "latest catalog name resolved to table id")
	require.Nil(t, rel, "ID lookup must not return the relation with the snapshot-visible ID")
	assertTempCatalogWorkspaceUnchanged(t, before, op, txn)
}

func TestTemporaryIndexFallbackRejectsOutOfScopeItems(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parentSession uuid.UUID
		childKind     string
	}{
		{name: "non-temporary relation kind", parentSession: tempCatalogSession, childKind: catalog.SystemOrdinaryRel},
		{name: "parent from another session", parentSession: tempCatalogSession2, childKind: catalog.SystemIndexRel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, op, txn, ctx := newTempCatalogFixture(t)
			insertTempCatalogDatabase(t, eng, txn, tempCatalogSnapshot)
			name := defines.GenTempTableName(tempCatalogSession, tempCatalogDatabase, "unique_index")
			insertTempCatalogTable(t, eng, txn, tempCatalogRoot(tc.parentSession, tempCatalogChildID), tempCatalogCommit)
			insertTempCatalogTable(t, eng, txn, tempCatalogIndex(tempCatalogSession, tempCatalogChildID, "unique_index", tc.childKind), tempCatalogCommit)

			db := &txnDatabase{
				op: op, accountId: tempCatalogAccountID,
				databaseId: tempCatalogDatabaseID, databaseName: tempCatalogDatabase,
			}
			before := captureTempCatalogWorkspaceState(op, txn)
			item, err := db.getTableItem(ctx, tempCatalogAccountID, name, eng)
			require.NoError(t, err)
			require.Nil(t, item)
			assertTempCatalogWorkspaceUnchanged(t, before, op, txn)

			_, _, rel, err := eng.GetRelationById(ctx, op, tempCatalogChildID)
			require.Error(t, err)
			require.Nil(t, rel)
			assertTempCatalogWorkspaceUnchanged(t, before, op, txn)
		})
	}
}
