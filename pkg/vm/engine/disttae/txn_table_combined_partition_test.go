// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type recordingIndexPartitions struct {
	partitionservice.PartitionService
	ctx      context.Context
	op       client.TxnOperator
	metadata partition.PartitionMetadata
}

func (r *recordingIndexPartitions) GetPartitionMetadata(
	ctx context.Context, _ uint64, op client.TxnOperator,
) (partition.PartitionMetadata, error) {
	r.ctx, r.op = ctx, op
	return r.metadata, nil
}

type testIndexPartitionParent struct {
	engine.Relation
	tableID uint64
	dbID    uint64
}

func (p *testIndexPartitionParent) GetTableID(context.Context) uint64 { return p.tableID }
func (p *testIndexPartitionParent) GetDBID(context.Context) uint64    { return p.dbID }
func (*testIndexPartitionParent) GetExtraInfo() *api.SchemaExtra {
	return &api.SchemaExtra{IndexTables: []uint64{101}}
}

func TestPartitionIndexMembersUseRelationSnapshot(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 99)
	processTxn := &Transaction{proc: proc, engine: &Engine{}}
	processOp := newTxnOperatorForTestWithWorkspace(t, processTxn)
	proc.Base.TxnOperator = processOp
	snapshotTxn := &Transaction{proc: proc, engine: &Engine{}, tableOps: newTableOps(), databaseOps: newDbOps()}
	snapshotOp := newTxnOperatorForTestWithWorkspace(t, snapshotTxn)
	snapshotTxn.op = snapshotOp
	db := &txnDatabase{accountId: 42, databaseId: 7, databaseName: "db", op: snapshotOp}
	snapshotTxn.databaseOps.addCreateDatabase(genDatabaseKey(42, "db"), 0, db)
	partitionTable := &txnTable{accountId: 42, tableId: 100001, tableName: "p0", db: db,
		extraInfo: &api.SchemaExtra{IndexTables: []uint64{100002}}}
	indexTable := &txnTable{accountId: 42, tableId: 100002, tableName: "p0_index", db: db}
	snapshotTxn.tableOps.addCreateTable(genTableKey(42, "p0", 7, "db"), 0, partitionTable)
	snapshotTxn.tableOps.addCreateTable(genTableKey(42, "p0_index", 7, "db"), 0, indexTable)
	service := &recordingIndexPartitions{metadata: partition.PartitionMetadata{TableID: 10, Partitions: []partition.Partition{{PartitionID: 100001, PrimaryTableID: 10}}}}
	proc.Base.PartitionService = service
	parent := &testIndexPartitionParent{tableID: 10, dbID: 7}
	table := &txnTableDelegate{
		origin: &txnTable{accountId: 42, tableId: 101, extraInfo: &api.SchemaExtra{ParentTableID: 10}, db: &txnDatabase{op: snapshotOp, databaseId: 7}},
		parent: parent,
	}
	ctx, cancelRead := context.WithCancel(defines.AttachAccountId(proc.Ctx, 42))
	defer cancelRead()
	members, err := table.getPartitionIndexesTables(ctx, proc)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Same(t, indexTable, members[0], "partition and index must resolve through the snapshot workspace")
	require.Same(t, snapshotOp, service.op, "member metadata must use the relation transaction")
	account, err := defines.GetAccountId(service.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(42), account, "member metadata must use the relation owner")
	require.Equal(t, ctx.Done(), service.ctx.Done(), "member metadata must retain pipeline cancellation")
	_, err = table.getPartitionIndexesTables(proc.Ctx, proc)
	require.NoError(t, err)
	account, err = defines.GetAccountId(service.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(42), account, "the relation owner must take priority over the process account")
	require.Same(t, snapshotOp, service.op)
	if activeM0Enabled {
		table.origin.extraInfo.ParentTableID = 11
		_, err = table.getPartitionIndexesTables(ctx, proc)
		require.ErrorContains(t, err, "partition index parent identity mismatch")
		table.origin.extraInfo.ParentTableID = 10
		parent.dbID = 8
		_, err = table.getPartitionIndexesTables(ctx, proc)
		require.ErrorContains(t, err, "partition index parent identity mismatch")
		parent.dbID = 7
		table.parent = nil
		_, err = table.getPartitionIndexesTables(ctx, proc)
		require.ErrorContains(t, err, "partition index parent identity mismatch")
		table.parent = parent
		service.metadata.TableID = 11
		_, err = table.getPartitionIndexesTables(ctx, proc)
		require.ErrorContains(t, err, "partition metadata parent identity mismatch")
		service.metadata.TableID = 10
		service.metadata.Partitions[0].PrimaryTableID = 11
		_, err = table.getPartitionIndexesTables(ctx, proc)
		require.ErrorContains(t, err, "partition member identity mismatch")
		service.metadata.Partitions[0].PrimaryTableID = 10
	}
	partitionTable.extraInfo = &api.SchemaExtra{}
	_, err = table.getPartitionIndexesTables(ctx, proc)
	require.ErrorContains(t, err, "partition index member metadata is incomplete")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = table.getPartitionIndexesTables(canceled, proc)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCombinedTombstonesUseReadingContextForMembers(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 99)
	txn := &Transaction{proc: proc, engine: &Engine{}, tableOps: newTableOps()}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op
	db := &txnDatabase{op: op, databaseId: 7, databaseName: "db"}
	member := &txnTable{accountId: 42, tableId: 101, tableName: "p0", db: db}
	txn.tableOps.addCreateTable(genTableKey(42, "p0", 7, "db"), 0, member)
	txn.tableOps.addDeleteTable(genTableKey(99, "p0", 7, "db"), 0, 102)
	metadata := partition.PartitionMetadata{TableID: 10, Partitions: []partition.Partition{{
		PartitionID: 101, PrimaryTableID: 10, PartitionTableName: "p0",
		Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
	}}}
	combined := newCombinedTxnTable(nil, getPartitionTableFunc(proc, metadata, db, 42),
		getPruneTablesFunc(proc, metadata, db, 42), getPartitionPrunePKFunc(proc, metadata, db, 42))
	ctx := defines.AttachAccountId(proc.Ctx, 42)
	selected, err := combined.pruneFunc(proc.Ctx, engine.RangesParam{})
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Same(t, member, selected[0], "range selection must use the same relation owner")
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], true, false, proc.Mp()))
	input.SetRowCount(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	selected, err = combined.prunePKFunc(proc.Ctx, input, -1)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Same(t, member, selected[0], "PK selection must use the same relation owner")
	data, err := combined.CollectTombstones(ctx, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, data, "the member visible to the reading account must be used")
	data, err = combined.CollectTombstones(proc.Ctx, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, data, "the bound relation owner must take priority over the process account")
	data, err = combined.CollectTombstones(ctx, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, data, "a different process account must not poison the bound relation")
	concurrentCtx, concurrentCancel := context.WithTimeout(proc.Ctx, 5*time.Second)
	defer concurrentCancel()
	type collectionResult struct {
		data engine.Tombstoner
		err  error
	}
	start := make(chan struct{})
	results := make(chan collectionResult, 2)
	for _, account := range []uint32{42, 99} {
		readCtx := defines.AttachAccountId(concurrentCtx, account)
		go func() {
			select {
			case <-start:
				data, err := combined.CollectTombstones(readCtx, 0, 0)
				results <- collectionResult{data: data, err: err}
			case <-readCtx.Done():
				results <- collectionResult{err: readCtx.Err()}
			}
		}()
	}
	close(start)
	for range 2 {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			require.NotNil(t, result.data)
		case <-concurrentCtx.Done():
			t.Fatal("concurrent member selection did not complete")
		}
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = combined.CollectTombstones(canceled, 0, 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = combined.pruneFunc(canceled, engine.RangesParam{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = combined.prunePKFunc(canceled, input, -1)
	require.ErrorIs(t, err, context.Canceled)
	arrayMembers := []engine.Relation{member}
	array := newCombinedTxnTable(nil, getArrayTableFunc(arrayMembers),
		getArrayPruneTablesFunc(arrayMembers), getArrayPrunePKFunc(arrayMembers))
	_, err = array.CollectTombstones(canceled, 0, 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = array.pruneFunc(canceled, engine.RangesParam{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = array.prunePKFunc(canceled, input, -1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestM0PartitionMemberRejectsIdentityMismatch(t *testing.T) {
	if !activeM0Enabled {
		t.Skip("experimental member binding")
	}
	proc := testutil.NewProc(t)
	txn := &Transaction{proc: proc, engine: &Engine{}, tableOps: newTableOps()}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op
	db := &txnDatabase{op: op, databaseId: 7, databaseName: "db"}
	member := &txnTable{accountId: 42, tableId: 101, tableName: "p0", db: db}
	txn.tableOps.addCreateTable(genTableKey(42, "p0", 7, "db"), 0, member)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], true, false, proc.Mp()))
	input.SetRowCount(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	for _, tc := range []struct {
		name       string
		id, parent uint64
		wantError  bool
	}{
		{name: "bound", id: 101, parent: 10},
		{name: "wrong member", id: 102, parent: 10, wantError: true},
		{name: "missing member", parent: 10, wantError: true},
		{name: "wrong parent", id: 101, parent: 11, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := partition.PartitionMetadata{TableID: 10, Partitions: []partition.Partition{{
				PartitionID: tc.id, PrimaryTableID: tc.parent, PartitionTableName: "p0",
				Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			}}}
			checks := []func() ([]engine.Relation, error){
				func() ([]engine.Relation, error) { return getPartitionTableFunc(proc, metadata, db, 42)(t.Context()) },
				func() ([]engine.Relation, error) {
					return getPruneTablesFunc(proc, metadata, db, 42)(t.Context(), engine.RangesParam{})
				},
				func() ([]engine.Relation, error) {
					return getPartitionPrunePKFunc(proc, metadata, db, 42)(t.Context(), input, -1)
				},
			}
			for _, check := range checks {
				got, err := check()
				if tc.wantError {
					require.Error(t, err)
					require.Empty(t, got)
				} else {
					require.NoError(t, err)
					require.Equal(t, []engine.Relation{member}, got)
				}
			}
		})
	}
}

func TestCombinedTombstonesCancelMemberResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	result := make(chan error, 1)
	combined := &combinedTxnTable{tablesFunc: func(ctx context.Context) ([]engine.Relation, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	go func() {
		_, err := combined.CollectTombstones(ctx, 0, 0)
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("member resolution did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("member resolution did not stop after cancellation")
	}
}

func TestPartitionPKCheckUsesLockedIdentity(t *testing.T) {
	col := func(pos int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	for _, tc := range []struct {
		name       string
		partition  *plan.Expr
		composite  bool
		explicit   int32
		wantColumn int32
		wantAll    bool
		rowIDFirst bool
	}{
		{name: "resolved PK differs from incoming schema position", partition: col(0), explicit: -1, wantColumn: 5},
		{name: "partition expression", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0)}}}}, explicit: -1, wantColumn: 5},
		{name: "repeated PK references", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(0)}}}}, explicit: -1, wantColumn: 5},
		{name: "explicit mixed columns", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(1)}}}}, explicit: 3, wantAll: true},
		{name: "explicit partition column", partition: col(1), explicit: 3, wantColumn: 3},
		{name: "partition on another column", partition: col(1), explicit: -1, wantAll: true},
		{name: "composite PK is not a scalar partition value", partition: col(0), composite: true, explicit: -1, wantAll: true},
		{name: "hidden index array", explicit: -1, wantColumn: -1},
		{name: "leading row ID", partition: col(0), explicit: -1, wantColumn: 4, rowIDFirst: true},
		{name: "multi column partition expression", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(1)}}}}, explicit: -1, wantAll: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := &plan.TableDef{
				Name2ColIndex: map[string]int32{"id": 0, "uk": 1},
				Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			}
			if tc.partition != nil {
				def.Partition = &plan.Partition{PartitionDefs: []*plan.PartitionDef{{Def: tc.partition}}}
			}
			if tc.composite {
				def.Pkey = &plan.PrimaryKeyDef{PkeyColName: "__mo_cpkey_col", Names: []string{"id", "uk"}}
			}
			relation := &mockRelation{}
			pruned, all := 0, 0
			input := batch.NewWithSize(6)
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			if tc.rowIDFirst {
				input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			}
			table := &combinedTxnTable{
				primary: &txnTable{tableDef: def},
				prunePKFunc: func(_ context.Context, got *batch.Batch, index int32) ([]engine.Relation, error) {
					pruned++
					require.Same(t, input, got)
					require.Equal(t, tc.wantColumn, index)
					return []engine.Relation{relation}, nil
				},
				tablesFunc: func(context.Context) ([]engine.Relation, error) {
					all++
					return []engine.Relation{relation}, nil
				},
			}
			relations, err := table.relationsForPKCheck(context.Background(), input, 5, tc.explicit)
			require.NoError(t, err)
			require.Equal(t, []engine.Relation{relation}, relations)
			if tc.wantAll {
				require.Equal(t, 1, all)
				require.Zero(t, pruned)
			} else {
				require.Equal(t, 1, pruned)
				require.Zero(t, all)
			}
		})
	}
}

func TestPartitionPKCheckChecksEveryPartitionExpression(t *testing.T) {
	def := &plan.TableDef{
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Partition: &plan.Partition{PartitionDefs: []*plan.PartitionDef{
			{Def: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
			{Def: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}},
		}},
	}
	require.False(t, partitionUsesOnlyPrimaryKey(def))
	def.Partition.PartitionDefs = def.Partition.PartitionDefs[:1]
	require.True(t, partitionUsesOnlyPrimaryKey(def))
	delete(def.Name2ColIndex, "id")
	require.False(t, partitionUsesOnlyPrimaryKey(def))
}
