//go:build mo_m0

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

package sqlintegration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// This exercises real transaction workspaces and relations on one CN. It does
// not exercise the remote scope transport or concurrent SQL admission.
func TestM0HistoricalCloneWorkspaceDeleteIsolation(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_historical_ws_binding"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".t (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".t values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".t')")
		m0Exec(t, ctx, db, "alter table "+schema+".t add column c int")
		svc := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		rollback := errors.New("rollback workspace binding control")
		err = svc.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			parent := tx.Txn()
			historicalTS := parent.SnapshotTS().Prev()
			cloned := parent.CloneSnapshotOp(historicalTS)
			require.True(t, cloned.IsSnapOp())
			require.True(t, cloned.SnapshotTS().Less(parent.SnapshotTS()))
			parentWS := parent.GetWorkspace().(*disttae.Transaction)
			cloneWS := cloned.GetWorkspace().(*disttae.Transaction)
			require.NotSame(t, parentWS, cloneWS)
			require.Zero(t, cloneWS.GetSnapshotWriteOffset())
			stamper, ok := cloned.(interface {
				CatalogReadStamp() (client.CatalogReadStamp, error)
			})
			require.True(t, ok)
			beforeStamp, err := stamper.CatalogReadStamp()
			require.NoError(t, err)
			require.NotEmpty(t, beforeStamp.TransactionID)
			require.Equal(t, historicalTS, beforeStamp.Snapshot)
			require.Same(t, cloneWS, beforeStamp.Workspace)
			database, err := svc.GetEngine().Database(txnCtx, schema, cloned)
			require.NoError(t, err)
			historical, err := database.Relation(txnCtx, "t", nil)
			require.NoError(t, err)
			assertEmpty := func(rel engine.Relation) {
				t.Helper()
				tombstones, err := rel.CollectTombstones(txnCtx, 0, engine.Policy_CollectUncommittedTombstones)
				require.NoError(t, err)
				require.False(t, tombstones.HasAnyInMemoryTombstone())
				require.False(t, tombstones.HasAnyTombstoneFile())
			}
			assertEmpty(historical)
			beforeRanges, err := historical.Ranges(txnCtx, engine.DefaultRangesParam)
			require.NoError(t, err)
			require.Greater(t, beforeRanges.DataCnt(), 1, "a persisted block and workspace sentinel are expected")
			beforeBlocks := append([]byte(nil), beforeRanges.GetBlockInfoSlice()...)
			result, err := tx.Exec("delete from "+schema+".t where a=1", executor.StatementOption{})
			require.NoError(t, err)
			result.Close()
			parentWS.UpdateSnapshotWriteOffset()
			require.Positive(t, parentWS.GetSnapshotWriteOffset())
			currentDB, err := svc.GetEngine().Database(txnCtx, schema, parent)
			require.NoError(t, err)
			current, err := currentDB.Relation(txnCtx, "t", nil)
			require.NoError(t, err)
			require.Equal(t, current.GetTableID(txnCtx), historical.GetTableID(txnCtx))
			tombstones, err := current.CollectTombstones(txnCtx, parentWS.GetSnapshotWriteOffset(), engine.Policy_CollectUncommittedTombstones)
			require.NoError(t, err)
			require.True(t, tombstones.HasAnyInMemoryTombstone() || tombstones.HasAnyTombstoneFile())
			require.Zero(t, cloneWS.GetSnapshotWriteOffset())
			afterStamp, err := stamper.CatalogReadStamp()
			require.NoError(t, err)
			require.Equal(t, beforeStamp.TransactionID, afterStamp.TransactionID)
			require.Equal(t, beforeStamp.Snapshot, afterStamp.Snapshot)
			require.Equal(t, beforeStamp.Revision, afterStamp.Revision)
			require.Same(t, cloneWS, afterStamp.Workspace)
			assertEmpty(historical)
			afterRanges, err := historical.Ranges(txnCtx, engine.DefaultRangesParam)
			require.NoError(t, err)
			require.Equal(t, beforeBlocks, []byte(afterRanges.GetBlockInfoSlice()), "parent workspace changes must not change the cloned relation's selected blocks")
			return rollback
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, rollback)
	})
}

// Distinct scan snapshots in a single statement must not accidentally share
// the first snapshot transaction's relation or workspace.
func TestM0TwoHistoricalSnapshotsInOneStatement(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema, first, second = "m0_two_snapshot_binding", "m0_two_snapshot_first", "m0_two_snapshot_second"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema, "drop snapshot if exists "+first, "drop snapshot if exists "+second)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".t (id int not null, v int not null, primary key(id,v))")
		m0Exec(t, ctx, db, "insert into "+schema+".t values (1,10)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".t')")
		m0Exec(t, ctx, db, "alter table "+schema+".t add column c int")
		m0Exec(t, ctx, db, "create snapshot "+first+" for table "+schema+" t")
		m0Exec(t, ctx, db, "update "+schema+".t set v=20 where id=1")
		m0Exec(t, ctx, db, "create snapshot "+second+" for table "+schema+" t")
		var oldValue, newValue int
		require.NoError(t, db.QueryRowContext(ctx, "select v from "+schema+".t{snapshot='"+first+"'}").Scan(&oldValue))
		require.NoError(t, db.QueryRowContext(ctx, "select v from "+schema+".t{snapshot='"+second+"'}").Scan(&newValue))
		require.Equal(t, 10, oldValue)
		require.Equal(t, 20, newValue)
		query := "select l.v, r.v from " + schema + ".t{snapshot='" + first + "'} l cross join " + schema + ".t{snapshot='" + second + "'} r"
		planRows, err := db.QueryContext(ctx, "explain "+query)
		require.NoError(t, err)
		defer planRows.Close()
		for planRows.Next() {
			var line string
			require.NoError(t, planRows.Scan(&line))
			t.Log(line)
		}
		require.NoError(t, planRows.Err())
		require.NoError(t, planRows.Close())
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&oldValue, &newValue))
		require.Equal(t, 10, oldValue)
		require.Equal(t, 20, newValue)
		reverseQuery := "select l.v, r.v from " + schema + ".t{snapshot='" + second + "'} l cross join " + schema + ".t{snapshot='" + first + "'} r"
		require.NoError(t, db.QueryRowContext(ctx, reverseQuery).Scan(&newValue, &oldValue))
		require.Equal(t, 20, newValue)
		require.Equal(t, 10, oldValue)
	})
}

// A metadata-only non-column ALTER can advance the schema version. Its
// version must not be interpreted as evidence of a prior M0 append.
func TestM0CommentBeforeAddPreservesContract(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_comment_then_add"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		originalID, originalVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Zero(t, originalVersion)
		originalObjects := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, originalObjects)
		m0Exec(t, ctx, db, "alter table "+table+" comment='updated metadata'")
		commentID, commentVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, originalID, commentID)
		require.Greater(t, commentVersion, originalVersion)
		require.Equal(t, originalObjects, m0ObjectFingerprints(t, ctx, db, table))
		m0Exec(t, ctx, db, "alter table "+table+" add column post_comment int")
		afterID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, originalID, afterID, "COMMENT preserves the authorized layout generation")
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where post_comment is null"))
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		afterObjects := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, afterObjects)
		require.Equal(t, originalObjects, afterObjects, "metadata-only ADD preserves original objects")
	})
}

func TestM0RejectsNonAppendDDLAfterAdd(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_comment_after_add"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column first_added int")
		before := m0SchemaProof(t, ctx, db, schema, "customer")
		beforeObjects := m0ObjectFingerprints(t, ctx, db, table)
		_, err = db.ExecContext(ctx, "alter table "+table+" rename column first_added to renamed")
		require.ErrorContains(t, err, "M0 append-domain table only supports metadata-only ADD COLUMN")
		observer := openM0SQL(t, cn)
		defer observer.Close()
		observer.SetMaxIdleConns(0)
		require.Equal(t, before, m0SchemaProof(t, ctx, observer, schema, "customer"))
		require.Equal(t, beforeObjects, m0ObjectFingerprints(t, ctx, observer, table))
		require.Equal(t, 3, m0Count(t, ctx, observer, "select count(*) from "+table+" where first_added is null"))
		m0Exec(t, ctx, observer, "alter table "+table+" add column second_added int")
		require.Equal(t, 3, m0Count(t, ctx, observer, "select count(*) from "+table+" where first_added is null and second_added is null"))
	})
}

func TestM0WidenBeforeAddUsesCopy(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_widen_then_add"
		table := schema + ".customer"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		const omission = "fj/m0/evolution/omit-create"
		defer m0RemoveFaultPointQuiet(context.Background(), db, omission)
		m0AddFaultPoint(t, ctx, db, omission, "echo", "")
		m0Exec(t, ctx, db, "create table "+table+" (c_id int not null, b int not null, txt varchar(4), primary key(c_id,b))")
		m0RemoveFaultPoint(t, ctx, db, omission)
		m0Exec(t, ctx, db, "insert into "+table+" values(1,1,'abc')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		originalID, originalVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Zero(t, originalVersion)
		originalObjects := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, originalObjects)
		m0Exec(t, ctx, db, "alter table "+table+" modify txt varchar(8), algorithm=inplace")
		widenedID, widenedVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, originalID, widenedID)
		require.Greater(t, widenedVersion, originalVersion)
		require.Equal(t, originalObjects, m0ObjectFingerprints(t, ctx, db, table))
		// Inspect the actual persisted vector: an inplace width change keeps the
		// old object and may legitimately leave its original type descriptor.
		var locationText string
		require.NoError(t, db.QueryRowContext(ctx,
			"select obj_loc from metadata_scan(?, 'txt') g limit 1", table).Scan(&locationText))
		location, err := objectio.StringToLocation(locationText)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		engine, ok := service.GetEngine().(*disttae.Engine)
		require.True(t, ok)
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, engine.FS())
		require.NoError(t, err)
		data := meta.MustGetMeta(objectio.SchemaData)
		requested := types.T_varchar.ToType()
		requested.Width = 8
		ioVec, err := objectio.ReadOneBlock(ctx, &data, location.Name().String(), location.ID(),
			[]uint16{2}, []types.Type{requested}, mp, engine.FS(), fileservice.SkipAllCache)
		require.NoError(t, err)
		defer objectio.ReleaseIOVector(&ioVec)
		var persisted vector.Vector
		require.NoError(t, objectio.MustVectorToCached(&persisted, ioVec.Entries[0].CachedData))
		defer persisted.Free(nil)
		require.Equal(t, int32(4), persisted.GetType().Width)
		require.Equal(t, "abc", persisted.GetStringAt(0))
		m0Exec(t, ctx, db, "alter table "+table+" add column post_widen int")
		afterID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.NotEqual(t, originalID, afterID, "a non-M0 type transition must use COPY")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where txt='abc' and post_widen is null"))
	})
}
