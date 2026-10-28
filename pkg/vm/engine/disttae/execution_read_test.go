// Copyright 2026 MatrixOne
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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func newExecutionReadTestTable(t *testing.T, op client.TxnOperator) (*txnDatabase, *txnTable) {
	t.Helper()
	db := &txnDatabase{
		accountId:    1,
		databaseId:   2,
		databaseName: "db",
		op:           op,
	}
	origin := &txnTable{
		accountId:     1,
		tableId:       3,
		version:       4,
		tableName:     "t",
		db:            db,
		primaryIdx:    -1,
		primarySeqnum: -1,
		clusterByIdx:  -1,
		viewdef:       "",
		comment:       "",
		partitioned:   0,
		relKind:       "r",
		tableDef:      &plan.TableDef{Name: "t", TableType: "r"},
		createdInTxn:  false,
		eng:           nil,
	}
	return db, origin
}

// Binding re-bases an EXCLUSIVE handle onto the execution-read carrier: the
// fresh origin resolves readTS through the carrier (every tbl.db.op.
// SnapshotTS() pin site reads F) while the ORIGINAL table's database keeps
// the session operator, so unmarked readers of the same transaction see the
// transaction snapshot exactly as before. Table identity and definition are
// carried unchanged and the handle exposes the engine capability.
func TestBindExecutionReadOpPinsReadTS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	// The carrier delegates workspace resolution to the wrapped operator, so
	// the binding contract requires a live workspace exactly like the
	// delegate's Reset path (txnIsValid).
	op.AddWorkspace(&Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	})

	db, origin := newExecutionReadTestTable(t, op)
	handle := &txnTableDelegate{origin: origin, reusableHandle: true}

	sessionTS := op.SnapshotTS()
	frontier := timestamp.Timestamp{
		PhysicalTime: sessionTS.PhysicalTime + 99,
		LogicalTime:  7,
	}
	carrier := client.ApplyExecutionReadTS(op, frontier)
	require.NoError(t, handle.BindExecutionReadOp(carrier))

	require.NotSame(t, origin, handle.origin, "fresh relation, never the cached/shared one")
	require.Equal(t, origin.tableId, handle.origin.tableId)
	require.Equal(t, origin.tableName, handle.origin.tableName)
	require.Equal(t, origin.tableDef, handle.origin.tableDef)
	require.NotSame(t, db, handle.origin.db)
	require.Equal(t, db.databaseId, handle.origin.db.databaseId)
	require.Equal(t, db.databaseName, handle.origin.db.databaseName)
	require.Same(t, carrier, handle.origin.db.op)
	require.Equal(t, frontier, handle.origin.db.op.SnapshotTS())
	require.True(t, origin.createdInTxn == handle.origin.createdInTxn)

	// The original is untouched: unmarked executions keep the session
	// operator's resolution.
	require.Same(t, op, origin.db.op)
	require.Equal(t, sessionTS, origin.db.op.SnapshotTS())

	// Workspace identity delegates to the wrapped SESSION operator: the
	// marked relation sees own writes, deletedBlocks and flushed tombstone
	// registry through the same workspace as an ordinary parent reader.
	require.Equal(t, op.GetWorkspace(), handle.origin.db.op.GetWorkspace())

	// The delegate exposes the engine capability.
	_, ok := engine.Relation(handle).(engine.ExecutionReadBinding)
	require.True(t, ok)
}

// Soft-unsupported refusal classes keep a statement on today's semantics as a
// whole: non-exclusive delegates, mock relations, combined/sharded kinds,
// partitioned-index parents and transaction-created tables never partially
// bind.
func TestBindExecutionReadOpUnsupportedKinds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	// The positive (plain-parent) subcase reaches the tombstone mask, whose
	// binding contract requires a live workspace exactly like the pin test.
	op.AddWorkspace(&Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	})
	_, origin := newExecutionReadTestTable(t, op)
	carrier := client.ApplyExecutionReadTS(op, op.SnapshotTS())

	// Non-exclusive (shared/canonical) delegates refuse without any change.
	sharedHandle := &txnTableDelegate{origin: origin}
	err := sharedHandle.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)
	require.Same(t, origin, sharedHandle.origin)

	// Transaction-created tables resolve through the tableOps payload today
	// (rev-11 R11-F1 view-local handles pending): soft refusal.
	created := origin
	created.createdInTxn = true
	exclusive := &txnTableDelegate{origin: created, reusableHandle: true}
	err = exclusive.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)

	// Mock relations, combined and sharded kinds refuse as well.
	mock := &txnTableDelegate{origin: origin, reusableHandle: true, isMock: true}
	err = mock.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)

	combined := &txnTableDelegate{origin: origin, reusableHandle: true}
	combined.combined.is = true
	err = combined.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)

	sharded := &txnTableDelegate{origin: origin, reusableHandle: true}
	sharded.shard.is = true
	err = sharded.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)

	// A PARTITIONED base's index relation refuses: the partition-service
	// parent link fans its reads out through getPartitionIndexesTables
	// (D-10.2 keeps partitioned bases outside the stage-1 surface).
	partitionedParent := &txnTable{extraInfo: &api.SchemaExtra{FeatureFlag: features.Partitioned}}
	parented := &txnTableDelegate{origin: origin, reusableHandle: true, parent: partitionedParent}
	err = parented.BindExecutionReadOp(carrier)
	require.Error(t, err)
	require.True(t, engine.IsExecutionReadUnsupported(err), err)

	// None of the refusals mutated the origin binding.
	require.Same(t, op, origin.db.op)

	// A plain (non-partitioned) base's covered index relation BINDS like any
	// ordinary surface (D-10.2/R13-F3): the parent link stays read-inert for a
	// local, non-sharded relation, and the bound handle re-bases onto a
	// cloned origin carrying the carrier while the original is untouched.
	_, plainOrigin := newExecutionReadTestTable(t, op)
	plainParent := &txnTable{extraInfo: &api.SchemaExtra{}}
	plainParented := &txnTableDelegate{origin: plainOrigin, reusableHandle: true, parent: plainParent}
	require.NoError(t, plainParented.BindExecutionReadOp(carrier))
	require.NotSame(t, plainOrigin, plainParented.origin)
}
