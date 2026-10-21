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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// Reproduce COPY ALTER's catalog DROP + rename without a statement boundary
// between the TN rewrite and the lineage refresh: an ordinary boundary could
// repair the stale tombstone and hide the regression.
func TestRCLineageSnapshotPreservesCatalogReplacement(t *testing.T) {
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: config.WithLongScanAndCKPOpts(nil)}, t)
	t.Cleanup(p.Close)
	schema := catalog2.MockSchemaEnhanced(1, 0, 2)
	schema.Name = "original"
	setup := p.StartCNTxn()
	t.Cleanup(func() {
		if setup.Status() == pbtxn.TxnStatus_Active {
			require.NoError(t, setup.Rollback(p.Ctx))
		}
	})
	p.CreateDBAndTable(setup, "catalog_refresh", schema)
	require.NoError(t, setup.Commit(p.Ctx))

	op := p.StartCNTxn(client.WithTxnMode(pbtxn.TxnMode_Pessimistic), client.WithTxnIsolation(pbtxn.TxnIsolation_RC))
	t.Cleanup(func() {
		if op.Status() == pbtxn.TxnStatus_Active {
			require.NoError(t, op.Rollback(p.Ctx))
		}
	})
	db, err := p.D.Engine.Database(p.Ctx, "catalog_refresh", op)
	require.NoError(t, err)
	schema.Name = "replacement"
	replacement := p.CreateTableInDB(op, "catalog_refresh", schema)
	ws := op.GetWorkspace()
	ws.UpdateSnapshotWriteOffset()
	require.NoError(t, db.Delete(p.Ctx, "original"))
	ws.UpdateSnapshotWriteOffset()
	req := api.NewRenameTableReq(replacement.GetDBID(p.Ctx), replacement.GetTableID(p.Ctx), "replacement", "original")
	encoded, err := req.Marshal()
	require.NoError(t, err)
	require.NoError(t, replacement.TableRenameInTxn(p.Ctx, [][]byte{encoded}))
	ws.UpdateSnapshotWriteOffset()

	require.NoError(t, p.T.GetDB().FlushTable(p.Ctx, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, types.TimestampToTS(p.D.Now())))
	_, err = databranchutils.AdvanceLineageSnapshot(p.Ctx, op)
	require.NoError(t, err)

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	res, err := v.(executor.SQLExecutor).Exec(p.Ctx,
		"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='catalog_refresh' and relname='original'",
		executor.Options{}.WithTxn(op).WithDisableIncrStatement())
	require.NoError(t, err)
	t.Cleanup(res.Close)
	ids := make([]uint64, 0, len(res.Batches))
	for _, b := range res.Batches {
		ids = append(ids, vector.MustFixedColNoTypeCheck[uint64](b.Vecs[0])...)
	}
	require.Equal(t, []uint64{replacement.GetTableID(p.Ctx)}, ids)
	// Exercise the real cardinality guard that panicked in the incident.
	require.NoError(t, db.Delete(p.Ctx, "original"))
}
