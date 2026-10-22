// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

// minimal mock cluster for tn handler wiring
type mockCluster struct{}

func (m mockCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	apply(metadata.CNService{ServiceID: "not-exist"})
}
func (m mockCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
}
func (m mockCluster) GetAllTNServices() []metadata.TNService { return nil }
func (m mockCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}
func (m mockCluster) ForceRefresh(sync bool)                               {}
func (m mockCluster) Close()                                               {}
func (m mockCluster) DebugUpdateCNLabel(string, map[string][]string) error { return nil }
func (m mockCluster) DebugUpdateCNWorkState(string, int) error             { return nil }
func (m mockCluster) RemoveCN(string)                                      {}
func (m mockCluster) AddCN(metadata.CNService)                             {}
func (m mockCluster) UpdateCN(metadata.CNService)                          {}

func Test_requestSnapshotRead_Smoke(t *testing.T) {
	ctx := context.Background()

	// register a mock cluster service to avoid panic
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, mockCluster{})

	// build a minimal process for the table
	proc := testutil.NewProc(t)
	// set a benign account name for session info
	proc.Base.SessionInfo.Account = "sys"

	tbl := &txnTable{}
	tbl.proc.Store(proc)

	// a zero ts is acceptable for smoke test
	ts := types.BuildTS(0, 0)
	// ensure compatibility with requestSnapshotRead API which uses timestamp.Timestamp
	_ = timestamp.Timestamp{}

	_, _ = RequestSnapshotRead(ctx, tbl, &ts)
}

func TestCreateTablePublishesLogicalIdentity(t *testing.T) {
	for _, name := range []string{"v", catalog.IndexTableNamePrefix + "test"} {
		t.Run(name, func(t *testing.T) {
			packer := types.NewPacker()
			t.Cleanup(packer.Close)
			eng := &Engine{packerPool: fileservice.NewPool(1,
				func() *types.Packer { return packer },
				func(p *types.Packer) { p.Reset() },
				func(*types.Packer) {},
			)}
			eng.catalog.Store(cache.NewCatalog())
			op, txn := newResetTxnForTest(t, eng)
			t.Cleanup(func() { cleanTxnWritesForTest(txn) })
			txn.tnStores = []DNStore{{}}
			// Resolve the logical-ID index through the transaction's existing
			// catalog relation without starting a SQL executor or a service.
			catDB := &txnDatabase{op: op, databaseId: catalog.MO_CATALOG_ID, databaseName: catalog.MO_CATALOG}
			txn.databaseOps.addCreateDatabase(genDatabaseKey(0, catalog.MO_CATALOG), 0, catDB)
			txn.tableCache.Store(genTableKey(0, catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
				catalog.MO_CATALOG_ID, catalog.MO_CATALOG), &txnTableDelegate{origin: &txnTable{db: catDB}})
			ctx := defines.AttachAccountId(context.Background(), 0)
			if name != "v" {
				// Hidden indexes must not inherit a base table's logical identity.
				ctx = context.WithValue(ctx, defines.LogicalIdKey{}, uint64(77))
			}
			db := &txnDatabase{op: op, databaseId: 10, databaseName: "db"}
			require.NoError(t, db.createWithID(ctx, name, 101, []engine.TableDef{&engine.ViewDef{View: "select 1"}}, false, nil))
			rel, err := db.Relation(ctx, name, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(101), rel.GetTableID(ctx))
			require.Equal(t, uint64(101), rel.GetTableDef(ctx).GetLogicalId())
		})
	}
}
