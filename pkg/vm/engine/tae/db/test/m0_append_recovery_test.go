//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

// A malformed TN ADD must fail before mutating the transaction's schema.
func TestM0TNRejectsNonTailAdd(t *testing.T) {
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
	defer tae.Close()
	root := catalog.NewEmptySchema("m0_invalid_tn_add")
	for _, name := range []string{"a", "b"} {
		require.NoError(t, root.AppendCol(name, types.T_int32.ToType()))
	}
	require.NoError(t, root.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	root.ColDefs[2].Hidden = true
	require.NoError(t, root.Finalize(false))
	tae.BindSchema(root)
	bat := catalog.MockBatch(root, 2)
	defer bat.Close()
	tae.CreateRelAndAppend2(bat, true)

	txn, rel := tae.GetRelation()
	active := true
	defer func() {
		if active {
			_ = txn.Rollback(ctx)
		}
	}()
	before := rel.Schema(false).(*catalog.Schema)
	version, next := before.Version, before.Extra.NextColSeqnum
	for _, tc := range []struct {
		name   string
		change func(*api.AlterTableReq)
	}{
		{"non-tail", func(r *api.AlterTableReq) { r.GetAddColumn().InsertPosition = 0 }},
		{"duplicate", func(r *api.AlterTableReq) {
			r.GetAddColumn().Column.Name = "a"
			r.GetAddColumn().Column.OriginName = "a"
		}},
		{"origin-name-mismatch", func(r *api.AlterTableReq) { r.GetAddColumn().Column.OriginName = "a" }},
		{"not-null", func(r *api.AlterTableReq) { r.GetAddColumn().Column.Default.NullAbility = false }},
		{"generated", func(r *api.AlterTableReq) { r.GetAddColumn().Column.GeneratedCol = &plan.GeneratedCol{} }},
		{"backfill", func(r *api.AlterTableReq) {
			r.GetAddColumn().Column.Default.Expr = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
		}},
		{"invalid-collation", func(r *api.AlterTableReq) { r.GetAddColumn().Column.Typ.Charset = 999 }},
		{"unknown-type", func(r *api.AlterTableReq) { r.GetAddColumn().Column.Typ.Id = 255 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := api.NewAddColumnReq(0, 0, "new_col", plan.Type{Id: int32(types.T_int32)}, -1)
			tc.change(req)
			var alterErr error
			require.NotPanics(t, func() { alterErr = rel.AlterTable(ctx, req) })
			require.Error(t, alterErr)
			after := rel.Schema(false).(*catalog.Schema)
			require.Equal(t, version, after.Version)
			require.Equal(t, next, after.Extra.NextColSeqnum)
			_, exists := after.NameMap["new_col"]
			require.False(t, exists)
		})
	}
	require.NoError(t, txn.Rollback(ctx))
	active = false

	readTxn, reopened := tae.GetRelation()
	defer readTxn.Rollback(ctx)
	readSchema := reopened.Schema(false).(*catalog.Schema)
	require.Equal(t, version, readSchema.Version)
	_, exists := readSchema.NameMap["new_col"]
	require.False(t, exists)
}

// Simulate a recovered TN schema with an omitted generated expression while
// its transaction-visible catalog row retains the authoritative value.
func TestM0TNAddRestoresGeneratedColumnAtomically(t *testing.T) {
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
	defer tae.Close()
	root := catalog.NewEmptySchema("m0_tn_restore_generated")
	require.NoError(t, root.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, root.AppendCol("b", types.T_int32.ToType()))
	require.NoError(t, root.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	root.ColDefs[2].Hidden = true
	generated, err := types.Encode(&plan.GeneratedCol{OriginString: "b + 1", IsStored: true})
	require.NoError(t, err)
	root.ColDefs[0].GeneratedCol = generated
	require.NoError(t, root.Finalize(false))
	tae.BindSchema(root)
	bat := catalog.MockBatch(root, 2)
	defer bat.Close()
	tae.CreateRelAndAppend2(bat, true)

	txn, rel := tae.GetRelation()
	active := true
	defer func() {
		if active {
			_ = txn.Rollback(ctx)
		}
	}()
	old := rel.Schema(false).(*catalog.Schema)
	old.ColDefs[0].GeneratedCol = nil // test-only simulation of the missing field
	err = rel.AlterTable(ctx, api.NewAddColumnReq(0, 0, "new_col", plan.Type{Id: int32(types.T_int32)}, -1))
	require.NoError(t, err)
	updated := rel.Schema(false).(*catalog.Schema)
	require.NotSame(t, old, updated)
	require.Equal(t, generated, updated.ColDefs[0].GeneratedCol)
	require.Empty(t, old.ColDefs[0].GeneratedCol, "the old schema must not be mutated")
	require.NoError(t, txn.Rollback(ctx))
	active = false
	readTxn, reopened := tae.GetRelation()
	defer readTxn.Rollback(ctx)
	restored := reopened.Schema(false).(*catalog.Schema)
	require.Empty(t, restored.ColDefs[0].GeneratedCol)
	_, exists := restored.NameMap["new_col"]
	require.False(t, exists)
}

// This exercises real TN catalog transactions, shared-object registration,
// parent catalog pruning and database recovery. It deliberately uses the
// existing internal ADD/clone primitives, not a new CN SQL entry. The temporary
// read transaction is released; no SQL named-snapshot lifecycle is claimed.
func TestM0UncontractedSuffixRejectedAfterParentDropAndRecovery(t *testing.T) {
	for _, useCheckpoint := range []bool{false, true} {
		name := "wal"
		if useCheckpoint {
			name = "checkpoint-plus-wal"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tae := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
			defer tae.Close()
			root := catalog.NewEmptySchema("m0_parent")
			for _, col := range []string{"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance"} {
				require.NoError(t, root.AppendCol(col, types.T_int32.ToType()))
			}
			require.NoError(t, root.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
			root.ColDefs[6].Hidden = true
			require.NoError(t, root.Finalize(false))
			root.Extra.BlockMaxRows = 16
			root.Extra.ObjectMaxBlocks = 1
			tae.BindSchema(root)
			bat := catalog.MockBatch(root, 3)
			defer bat.Close()
			// Checkpoint replay rebuilds TN catalog entries from the three system
			// tables. The raw CreateRelAndAppend helper only creates TN entries.
			tae.CreateRelAndAppend2(bat, true)
			tae.CompactBlocks(false)

			txn, parent := tae.GetRelation()
			parentID := parent.ID()
			old := testutil.GetOneObject(parent).GetMeta().(*catalog.ObjectEntry)
			stats := old.ObjectStats
			require.False(t, stats.GetAppendable())
			require.Error(t, parent.AlterTable(ctx, api.NewAddColumnReq(0, 0, "loyalty_tier", plan.Type{Id: int32(types.T_varchar), Width: 8}, -1)), "uncontracted direct TN ADD cannot mint authority")
			require.NoError(t, txn.Rollback(ctx))
			// Pin then release a temporary source read view around the clone.
			readView, _ := tae.GetRelation()
			cloneTxn, db := tae.GetTestDB()
			parent, err := db.GetRelationByName(root.Name)
			require.NoError(t, err)
			cloneDef := parent.Schema(false).(*catalog.Schema).Clone()
			require.NoError(t, cloneDef.ApplyAlterTable(api.NewAddColumnReq(0, 0, "loyalty_tier", plan.Type{Id: int32(types.T_varchar), Width: 8}, -1)))
			require.NoError(t, cloneDef.ApplyAlterTable(api.NewAddColumnReq(0, 0, "credit_lim", plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}, -1)))
			cloneDef.Version = 2 // controlled metadata generation; ApplyAlterTable only edits the local fixture
			require.Equal(t, uint32(2), cloneDef.Version)
			cloneDef.Name = "m0_child"
			// Controlled uncontracted wide-schema/raw-object fixture. This is
			// deliberately not an authorized M0 clone or SQL ADD. Recovery
			// must not invent permission to read its absent suffix.
			child, err := testutil.CreateRelation2(ctx, cloneTxn, db, cloneDef)
			require.NoError(t, err)
			shared := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
			shared.Append(stats[:], false)
			err = child.AddDataFiles(ctx, shared)
			shared.Close()
			require.NoError(t, err)
			require.NoError(t, cloneTxn.Commit(ctx))
			require.NoError(t, readView.Rollback(ctx))

			// The checkpoint precedes both the child's ADD and the parent drop;
			// their recovered state must therefore also require WAL replay.
			if useCheckpoint {
				tae.ForceCheckpoint()
				require.NotNil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			} else {
				require.Nil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			}
			checkpoint := tae.BGCheckpointRunner.MaxCheckpoint()
			txn, db = tae.GetTestDB()
			child, err = db.GetRelationByName("m0_child")
			require.NoError(t, err)
			_, err = db.DropRelationByName(root.Name)
			require.NoError(t, err)
			dbID := db.GetID()
			require.NoError(t, txn.Commit(ctx))
			pruneParent := func() {
				tae.Catalog.GCByTS(ctx, tae.TxnMgr.Now())
				dbMeta, err := tae.Catalog.GetDatabaseByID(dbID)
				require.NoError(t, err)
				_, err = dbMeta.GetTableEntryByID(parentID)
				require.Error(t, err) // ancestor removed, not merely hidden
			}
			pruneParent()

			check := func() {
				txn, rel := testutil.GetDefaultRelation(t, tae.DB, "m0_child")
				s := rel.Schema(false).(*catalog.Schema)
				require.Equal(t, uint32(2), s.Version)
				obj := testutil.GetOneObject(rel)
				objMeta := obj.GetMeta().(*catalog.ObjectEntry)
				require.Equal(t, stats.ObjectName().String(), objMeta.ObjectStats.ObjectName().String())
				r, err := objectio.NewObjectReaderWithStr(objMeta.ObjectStats.ObjectName().String(), tae.Runtime.Fs)
				require.NoError(t, err)
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				h, err := r.ReadHeader(ctx, mp)
				require.NoError(t, err)
				meta, err := r.ReadAllMeta(ctx, mp)
				require.NoError(t, err)
				for _, seq := range []uint16{7, 8} {
					fixtureLayout, err := catalog.M0AppendLayoutFromSchema(s)
					require.NoError(t, err)
					absent, err := m0read.ProveM0AbsentColumn(fixtureLayout, h, meta.MustDataMeta(), seq)
					require.NoError(t, err)
					require.True(t, absent)
				}
				var view *containers.Batch
				defer func() {
					if view != nil {
						view.Close()
					}
				}()
				layout, err := catalog.M0AppendLayoutFromSchema(s)
				require.NoError(t, err)
				width, err := m0read.ValidateM0Object(layout, h, meta.MustDataMeta())
				require.NoError(t, err)
				_ = width // Geometry alone must not authorize the recovered relation.
				require.Nil(t, s.Extra.M0Evolution)
				require.Error(t, obj.Scan(ctx, &view, 0, []int{7, 8}, common.DefaultAllocator))

				require.NoError(t, txn.Commit(ctx))
			}
			check()
			tae.Restart(ctx)
			if checkpoint == nil {
				require.Nil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			} else {
				replayed := tae.BGCheckpointRunner.MaxCheckpoint()
				require.NotNil(t, replayed)
				require.Equal(t, checkpoint.GetEnd(), replayed.GetEnd())
			}
			pruneParent()
			check()
		})
	}
}
