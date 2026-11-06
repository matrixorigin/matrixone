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

package test

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/stretchr/testify/require"
)

func TestGeneratedColumnCatalogRecovery(t *testing.T) {
	type recoveryCase struct {
		name                  string
		checkpointBeforeTable bool
		checkpointAfterTable  bool
		checkpointAfterAlter  bool
		alterAfterCreate      bool
		simulateGeneratedLoss bool
	}
	cases := []recoveryCase{
		{name: "wal"},
		{name: "checkpoint", checkpointAfterTable: true},
		{name: "checkpoint_then_wal", checkpointBeforeTable: true},
		{name: "checkpoint_then_alter", checkpointAfterTable: true, alterAfterCreate: true},
		{name: "checkpoint_after_alter", checkpointAfterAlter: true, alterAfterCreate: true},
	}
	if generatedRecoveryIncludeLossCases() {
		cases = append(cases,
			recoveryCase{name: "wal_alter_after_simulated_generated_loss", alterAfterCreate: true, simulateGeneratedLoss: true},
			recoveryCase{name: "checkpoint_alter_after_simulated_generated_loss", checkpointAfterTable: true, alterAfterCreate: true, simulateGeneratedLoss: true},
		)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var previousSnapshot types.TS
			tae := testutil.NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
			defer tae.Close()
			if tc.checkpointBeforeTable {
				seed := catalog.MockSchemaAll(2, 1)
				seed.Name = "checkpoint_seed"
				tae.BindSchema(seed)
				seedBatch := catalog.MockBatch(seed, 1)
				defer seedBatch.Close()
				tae.CreateRelAndAppend2(seedBatch, true)
				tae.ForceCheckpoint()
				require.NotNil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			}
			schema := catalog.MockSchemaAll(2, 1)
			if tc.alterAfterCreate {
				schema = generatedRecoveryAlterSchema(t)
			}
			schema.Name = "generated_target"
			generated, err := types.Encode(&plan.GeneratedCol{OriginString: "a + 1", IsStored: true})
			require.NoError(t, err)
			schema.ColDefs[0].GeneratedCol = generated
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 3)
			defer bat.Close()
			tae.CreateRelAndAppend2(bat, !tc.checkpointBeforeTable)
			if tc.checkpointAfterTable {
				tae.ForceCheckpoint()
				require.NotNil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			} else if !tc.checkpointBeforeTable {
				require.Nil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			}
			if tc.alterAfterCreate {
				oldTxn, oldRel := tae.GetRelation()
				previousSnapshot = oldTxn.GetStartTS()
				require.Equal(t, generated, oldRel.Schema(false).(*catalog.Schema).ColDefs[0].GeneratedCol)
				txn, db := tae.GetTestDB()
				rel, err := db.GetRelationByName(schema.Name)
				require.NoError(t, err)
				oldSchema := rel.Schema(false).(*catalog.Schema)
				entry := rel.GetMeta().(*catalog.TableEntry)
				if tc.simulateGeneratedLoss {
					// Test-only fault: leave the authoritative catalog row intact.
					entry.Lock()
					oldSchema.ColDefs[0].GeneratedCol = nil
					entry.Unlock()
				}
				catalogDB, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
				require.NoError(t, err)
				tableRows, err := catalogDB.GetRelationByID(pkgcatalog.MO_TABLES_ID)
				require.NoError(t, err)
				columnRows, err := catalogDB.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
				require.NoError(t, err)
				// Match CN precommit ordering: TN ALTER is applied while the old
				// catalog rows remain visible; their replacement follows it.
				require.NoError(t, rel.AlterTable(ctx, api.NewAddColumnReq(0, 0, "later", plan.Type{Id: int32(types.T_int32)}, -1)))
				require.Equal(t, generated, rel.Schema(false).(*catalog.Schema).ColDefs[0].GeneratedCol)
				packer := types.NewPacker()
				defer packer.Close()
				packer.EncodeUint32(txn.GetTenantID())
				packer.EncodeStringType([]byte(db.GetName()))
				packer.EncodeStringType([]byte(schema.Name))
				require.NoError(t, tableRows.DeleteByFilter(ctx, handle.NewEQFilter(packer.Bytes())))
				for _, col := range oldSchema.ColDefs {
					packer.Reset()
					packer.EncodeUint32(txn.GetTenantID())
					packer.EncodeStringType([]byte(db.GetName()))
					packer.EncodeStringType([]byte(schema.Name))
					packer.EncodeStringType([]byte(col.Name))
					require.NoError(t, columnRows.DeleteByFilter(ctx, handle.NewEQFilter(packer.Bytes())))
				}
				newSchema := rel.Schema(false).(*catalog.Schema)
				appendRows := func(systemSchema *catalog.Schema, relation handle.Relation, fill func(*catalog.ColDef, containers.Vector)) {
					t.Helper()
					batch := containers.NewBatch()
					defer batch.Close()
					for _, col := range systemSchema.ColDefs {
						if col.IsPhyAddr() {
							continue
						}
						v := containers.MakeVector(col.Type, common.CheckpointAllocator)
						batch.AddVector(col.Name, v)
						fill(col, v)
					}
					require.NoError(t, relation.Append(ctx, batch))
				}
				appendRows(catalog.SystemTableSchema, tableRows, func(col *catalog.ColDef, v containers.Vector) {
					txnimpl.FillTableRow(entry, newSchema, col.Name, v)
				})
				appendRows(catalog.SystemColumnSchema, columnRows, func(col *catalog.ColDef, v containers.Vector) {
					txnimpl.FillColumnRow(entry, newSchema, col.Name, v)
				})
				require.NoError(t, txn.Commit(ctx))
				if tc.simulateGeneratedLoss {
					require.Empty(t, oldRel.Schema(false).(*catalog.Schema).ColDefs[0].GeneratedCol)
				} else {
					require.Equal(t, generated, oldRel.Schema(false).(*catalog.Schema).ColDefs[0].GeneratedCol)
				}
				require.NoError(t, oldTxn.Rollback(ctx))
			}
			if tc.checkpointAfterAlter {
				checkTxn, checkRel := tae.GetRelation()
				entry := checkRel.GetMeta().(*catalog.TableEntry)
				require.NotNil(t, entry.GetVersionSchema(0), "prior schema must exist before checkpoint")
				require.NotNil(t, entry.GetVersionSchema(1), "new schema must exist before checkpoint")
				require.NoError(t, checkTxn.Rollback(ctx))
				tae.ForceCheckpoint()
				require.NotNil(t, tae.BGCheckpointRunner.MaxCheckpoint())
			}
			tae.Restart(ctx)
			txn, rel := tae.GetRelation()
			defer txn.Rollback(ctx)
			catalogDB, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
			require.NoError(t, err)
			columns, err := catalogDB.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
			require.NoError(t, err)
			rows := tables.ReadSysTableBatch(ctx, columns.GetMeta().(*catalog.TableEntry), txn)
			require.NotNil(t, rows)
			defer rows.Close()
			ids := vector.MustFixedColNoTypeCheck[uint64](rows.GetVectorByName(pkgcatalog.SystemColAttr_RelID).GetDownstreamVector())
			markers := vector.MustFixedColNoTypeCheck[int8](rows.GetVectorByName(pkgcatalog.SystemColAttr_HasGenerated).GetDownstreamVector())
			names := rows.GetVectorByName(pkgcatalog.SystemColAttr_Name).GetDownstreamVector()
			expressions := rows.GetVectorByName(pkgcatalog.SystemColAttr_Generated).GetDownstreamVector()
			found := false
			for i, id := range ids {
				if id == rel.ID() && names.GetStringAt(i) == schema.ColDefs[0].Name {
					require.False(t, found)
					require.Equal(t, int8(1), markers[i])
					require.Equal(t, generated, expressions.GetBytesAt(i))
					found = true
				}
			}
			require.True(t, found, "mo_columns must contain the generated definition")
			// The same transaction used by a TN reader can resolve the exact
			// catalog row; a scan of the latest catalog is not a snapshot binding.
			packer := types.NewPacker()
			defer packer.Close()
			packer.EncodeUint32(schema.AcInfo.TenantID)
			packer.EncodeStringType([]byte(testutil.DefaultTestDB))
			packer.EncodeStringType([]byte(schema.Name))
			packer.EncodeStringType([]byte(schema.ColDefs[0].Name))
			rowID, rowOffset, err := columns.GetByFilter(ctx, handle.NewEQFilter(packer.Bytes()))
			require.NoError(t, err)
			marker, isNull, err := columns.GetValue(rowID, rowOffset, pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX, false)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, int8(1), marker)
			expression, isNull, err := columns.GetValue(rowID, rowOffset, pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX, false)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, generated, expression)
			restored := rel.Schema(false).(*catalog.Schema)
			require.Equal(t, generated, restored.ColDefs[0].GeneratedCol)
			if tc.checkpointAfterAlter {
				entry := rel.GetMeta().(*catalog.TableEntry)
				require.Nil(t, entry.GetVersionSchema(0), "checkpoint replay must not invent a historical schema")
			}
			if tc.alterAfterCreate {
				require.Equal(t, uint32(1), restored.Version)
				if tc.checkpointAfterAlter {
					return
				}
				oldTxn, err := tae.StartTxnWithStartTSAndSnapshotTS(nil, previousSnapshot)
				require.NoError(t, err)
				defer oldTxn.Rollback(ctx)
				oldDB, err := oldTxn.GetDatabase(testutil.DefaultTestDB)
				require.NoError(t, err)
				oldRel, err := oldDB.GetRelationByName(schema.Name)
				require.NoError(t, err)
				oldSchema := oldRel.Schema(false).(*catalog.Schema)
				require.Equal(t, uint32(0), oldSchema.Version)
				require.Equal(t, generated, oldSchema.ColDefs[0].GeneratedCol)
			}
		})
	}
}
