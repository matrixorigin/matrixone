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

// This exercises real TN catalog transactions, shared-object registration,
// parent catalog pruning and database recovery. It deliberately uses the
// existing internal ADD/clone primitives, not a new CN SQL entry. The temporary
// read transaction is released; no SQL named-snapshot lifecycle is claimed.
func TestM0AppendProofAfterParentDropAndRecovery(t *testing.T) {
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
			require.NoError(t, parent.AlterTable(ctx, api.NewAddColumnReq(0, 0, "loyalty_tier", plan.Type{Id: int32(types.T_varchar), Width: 8}, -1)))
			require.NoError(t, txn.Commit(ctx))
			// Pin then release a temporary source read view around the clone.
			readView, _ := tae.GetRelation()
			cloneTxn, db := tae.GetTestDB()
			parent, err := db.GetRelationByName(root.Name)
			require.NoError(t, err)
			cloneDef := parent.Schema(false).(*catalog.Schema).Clone()
			require.Equal(t, uint32(1), cloneDef.Version)
			cloneDef.Name = "m0_child"
			// Persist the child's complete version-one definition as clone would
			// have to do. This helper does not implement/validate CN ADD catalog
			// publication; the second ADD below is deliberately replayed via WAL.
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
			require.NoError(t, child.AlterTable(ctx, api.NewAddColumnReq(0, 0, "credit_lim", plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}, -1)))
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
				_, err := catalog.M0AppendBase(s)
				require.NoError(t, err)
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
					absent, err := catalog.M0ProveAbsentColumn(s, h, meta.MustDataMeta(), seq)
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
				width, err := objectio.ValidateM0Object(layout, h, meta.MustDataMeta())
				require.NoError(t, err)
				scanCtx := objectio.WithM0ProvenObject(ctx, objectio.M0ObjectProof{
					ObjectName:  objMeta.ObjectStats.ObjectName().String(),
					WrittenSize: width,
					SchemaSize:  layout.NextColSeqnum,
				})
				require.NoError(t, obj.Scan(scanCtx, &view, 0, []int{7, 8}, common.DefaultAllocator))
				for i, v := range view.Vecs {
					require.Equal(t, s.ColDefs[7+i].Type, *v.GetType())
					require.Equal(t, 3, v.Length())
					nulls := 0
					require.NoError(t, v.Foreach(func(_ any, isNull bool, _ int) error {
						require.True(t, isNull)
						nulls++
						return nil
					}, nil))
					require.Equal(t, 3, nulls)
				}
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
