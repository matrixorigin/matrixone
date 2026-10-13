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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestHistoricalRenameRollback(t *testing.T) {
	for _, lateDuplicate := range []bool{false, true} {
		name := "explicit_abort"
		if lateDuplicate {
			name = "later_duplicate"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
			defer tae.Close()
			pending := make(map[txnif.AsyncTxn]bool)
			defer func() {
				for txn := range pending {
					require.NoError(t, txn.Rollback(context.Background()), "transaction cleanup")
				}
			}()
			start := func() txnif.AsyncTxn {
				txn, err := tae.StartTxn(nil)
				require.NoError(t, err)
				pending[txn] = true
				return txn
			}
			commit := func(txn txnif.AsyncTxn) {
				require.NoError(t, txn.Commit(ctx))
				delete(pending, txn)
			}
			setup := start()
			db, err := setup.CreateDatabase("rename_history", "", "")
			require.NoError(t, err)
			ids := make(map[string]uint64)
			for i, name := range []string{"t_live", "t_shadow", "occupied"} {
				schema := catalog.NewEmptySchema(name)
				require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
				schema.Extra.BlockMaxRows = 10
				schema.Extra.ObjectMaxBlocks = 2
				require.NoError(t, schema.Finalize(false))
				rel, err := db.CreateRelation(schema)
				require.NoError(t, err)
				ids[name] = rel.ID()
				bat := containers.NewBatch()
				bat.AddVector("id", containers.NewVector(types.T_int64.ToType()))
				bat.Vecs[0].Append(int64((i+1)*100), false)
				err = rel.Append(ctx, bat)
				bat.Close()
				require.NoError(t, err)
			}
			commit(setup)
			rename := func(txn txnif.AsyncTxn, old, next string) error {
				db, err := txn.GetDatabase("rename_history")
				require.NoError(t, err)
				rel, err := db.GetRelationByName(old)
				require.NoError(t, err, old)
				return rel.AlterTable(ctx, api.NewRenameTableReq(0, 0, old, next))
			}
			read := func(txn txnif.AsyncTxn, name string, id uint64, value int64) {
				t.Helper()
				db, err := txn.GetDatabase("rename_history")
				require.NoError(t, err)
				// Resolve from the catalog on every assertion, avoiding a cached
				// relation handle masking loss of an older name binding.
				entry, err := db.GetMeta().(*catalog.DBEntry).GetTableEntryByName(txn.GetTenantID(), name, txn)
				require.NoError(t, err, "catalog name %s", name)
				require.Equal(t, id, entry.ID, name)
				rel, err := db.GetRelationByID(entry.ID)
				require.NoError(t, err)
				var values []int64
				testutil.ForEachColumnView(t, rel, 0, func(view *containers.Batch) error {
					view.Compact()
					for i := 0; i < view.Length(); i++ {
						values = append(values, view.Vecs[0].Get(i).(int64))
					}
					return nil
				})
				require.Equal(t, []int64{value}, values, name)
			}
			missing := func(txn txnif.AsyncTxn, name string) {
				t.Helper()
				db, err := txn.GetDatabase("rename_history")
				require.NoError(t, err)
				_, err = db.GetMeta().(*catalog.DBEntry).GetTableEntryByName(txn.GetTenantID(), name, txn)
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "%s: %v", name, err)
			}
			beforeHistory := start()
			history := start()
			require.NoError(t, rename(history, "t_live", "t_tmp"))
			require.NoError(t, rename(history, "t_shadow", "t_live"))
			require.NoError(t, rename(history, "t_tmp", "t_shadow"))
			commit(history)
			read(beforeHistory, "t_live", ids["t_live"], 100)
			read(beforeHistory, "t_shadow", ids["t_shadow"], 200)
			beforeAbort := start()
			aborted := start()
			require.NoError(t, rename(aborted, "t_live", "t_tmp"))
			require.NoError(t, rename(aborted, "t_shadow", "t_live"), "historical-self admission must execute before abort")
			read(aborted, "t_live", ids["t_live"], 100)
			read(aborted, "t_tmp", ids["t_shadow"], 200)
			missing(aborted, "t_shadow")
			read(aborted, "occupied", ids["occupied"], 300)
			// Prove that the old reader can resolve its names before abort,
			// including while the historical-self rename is provisionally live.
			read(beforeHistory, "t_live", ids["t_live"], 100)
			read(beforeHistory, "t_shadow", ids["t_shadow"], 200)
			read(beforeAbort, "t_live", ids["t_shadow"], 200)
			read(beforeAbort, "t_shadow", ids["t_live"], 100)
			if lateDuplicate {
				err = rename(aborted, "t_tmp", "occupied")
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedDup), "real distinct-owner duplicate: %v", err)
			}
			require.NoError(t, aborted.Rollback(ctx))
			delete(pending, aborted)
			fresh := start()
			for _, txn := range []txnif.AsyncTxn{fresh, beforeAbort} {
				read(txn, "t_live", ids["t_shadow"], 200)
				read(txn, "t_shadow", ids["t_live"], 100)
				read(txn, "occupied", ids["occupied"], 300)
				missing(txn, "t_tmp")
			}
			read(beforeHistory, "t_live", ids["t_live"], 100)
			read(beforeHistory, "t_shadow", ids["t_shadow"], 200)
			missing(beforeHistory, "t_tmp")
			commit(beforeHistory)
			commit(beforeAbort)
			commit(fresh)
			reuse := start()
			require.NoError(t, rename(reuse, "t_live", "t_tmp"))
			require.NoError(t, rename(reuse, "t_shadow", "t_live"))
			require.NoError(t, rename(reuse, "t_tmp", "t_shadow"))
			commit(reuse)
			verify := start()
			read(verify, "t_live", ids["t_live"], 100)
			read(verify, "t_shadow", ids["t_shadow"], 200)
			read(verify, "occupied", ids["occupied"], 300)
			missing(verify, "t_tmp")
			commit(verify)
		})
	}
}

func TestHistoricalRenameCreateDrop(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "abort"}[abort], func(t *testing.T) {
			ctx := context.Background()
			tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
			defer tae.Close()
			setup, err := tae.StartTxn(nil)
			require.NoError(t, err)
			_, err = setup.CreateDatabase("rename_created", "", "")
			require.NoError(t, err)
			require.NoError(t, setup.Commit(ctx))
			txn, err := tae.StartTxn(nil)
			require.NoError(t, err)
			txnActive := true
			defer func() {
				if txnActive {
					require.NoError(t, txn.Rollback(ctx))
				}
			}()
			db, err := txn.GetDatabase("rename_created")
			require.NoError(t, err)
			schema := catalog.NewEmptySchema("created")
			require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
			require.NoError(t, schema.Finalize(false))
			rel, err := db.CreateRelation(schema)
			require.NoError(t, err)
			require.NoError(t, rel.AlterTable(ctx, api.NewRenameTableReq(0, 0, "created", "renamed")))
			_, err = db.DropRelationByName("renamed")
			require.NoError(t, err)
			if abort {
				require.NoError(t, txn.Rollback(ctx))
			} else {
				require.NoError(t, txn.Commit(ctx), "Freeze cleans a create/rename/drop journal")
			}
			txnActive = false
			verify, err := tae.StartTxn(nil)
			require.NoError(t, err)
			verifyActive := true
			defer func() {
				if verifyActive {
					require.NoError(t, verify.Rollback(ctx))
				}
			}()
			fresh, err := verify.GetDatabase("rename_created")
			require.NoError(t, err)
			for _, name := range []string{"created", "renamed"} {
				_, err := fresh.GetRelationByName(name)
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "%s: %v", name, err)
			}
			// Reusing both names proves the removed table owns no provisional claim.
			for _, name := range []string{"created", "renamed"} {
				copy := schema.Clone()
				copy.Name = name
				_, err := fresh.CreateRelation(copy)
				require.NoError(t, err)
			}
			require.NoError(t, verify.Commit(ctx))
			verifyActive = false
		})
	}
}
