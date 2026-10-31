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

package v4_0_7

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The data-branch provenance catalogs append to the unpublished 4.0.7
// handler: docs/design/20261028-catalog-upgrade-407.md forbids registering
// the unpublished 4.0.8..4.0.19 identities as upgrade steps, so the two
// catalog creations ride the consolidated final vehicle as its trailing
// tenant entries and raise its recorded offset while it stays unpublished.
func TestHandlerRegistersProvenanceCatalogTail(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		metadata := Handler.Metadata()
		require.Equal(t, "4.0.7", metadata.Version)
		require.Equal(t, "4.0.6", metadata.MinUpgradeVersion)
		require.Equal(t, versions.Yes, metadata.UpgradeCluster)
		require.Equal(t, versions.Yes, metadata.UpgradeTenant)
		require.Equal(t,
			uint32(len(tenantUpgEntries)+len(clusterUpgEntries)),
			metadata.VersionOffset)
		require.Equal(t, int64(defines.MORPCVersion126), metadata.RequiredProtocolVersion)
		require.GreaterOrEqual(t, len(tenantUpgEntries), 2)

		updateLink := tenantUpgEntries[len(tenantUpgEntries)-2]
		retention := tenantUpgEntries[len(tenantUpgEntries)-1]
		require.Equal(t, catalog.MO_CATALOG, updateLink.Schema)
		require.Equal(t, catalog.MO_DATA_BRANCH_UPDATE_LINK, updateLink.TableName)
		require.Equal(t, catalog.MO_CATALOG, retention.Schema)
		require.Equal(t, catalog.MO_DATA_BRANCH_LINK_RETENTION, retention.TableName)
	})
}

// The data-branch catalog creations are new feature entries: the repository
// supports shutdown upgrade only, so they carry no per-feature protocol gate
// and add no cluster-wide protocol check to an affected tenant migration.
func TestProvenanceCatalogEntryProtocolGates(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		updateLink := tenantUpgEntries[len(tenantUpgEntries)-2]
		retention := tenantUpgEntries[len(tenantUpgEntries)-1]
		require.Equal(t, frontend.MoCatalogDataBranchUpdateLinkDDL, updateLink.UpgSql)
		require.Zero(t, updateLink.RequiredProtocolVersion)
		require.Equal(t, versions.CREATE_NEW_TABLE, updateLink.UpgType)
		require.Equal(t, frontend.MoCatalogDataBranchLinkRetentionDDL, retention.UpgSql)
		require.Zero(t, retention.RequiredProtocolVersion)
		require.Equal(t, versions.CREATE_NEW_TABLE, retention.UpgType)
	})
}

// The data-branch catalog creations execute both DDLs in declaration order
// and become read-only no-ops once the tables exist, so a walk resumed after
// a mid-upgrade failure converges without rewriting completed work or
// repeating catalog DDL. Entry-level idempotence matters because the tenant
// walk retries entries: an offset-only mismatch against the recorded catalog
// version hard-errors before any mutation instead of re-firing completed
// work across binaries.
func TestProvenanceCatalogCreateAndIdempotence(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()

		tableExists := map[string]bool{}
		var ddl []string
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables") {
				// CheckTableDefinition keys existence on the WHERE clause's
				// relname = '<name>'; the mem executor must honor it, or a
				// sibling table's row would mask every later entry.
				want := ""
				if i := strings.Index(sql, "relname = '"); i >= 0 {
					rest := sql[i+len("relname = '"):]
					if j := strings.Index(rest, "'"); j >= 0 {
						want = rest[:j]
					}
				}
				r := executor.NewMemResult([]types.Type{
					types.T_varchar.ToType(),
					types.T_varchar.ToType(),
					types.T_uint32.ToType(),
				}, mp)
				names := make([]string, 0, len(tableExists))
				for name, exists := range tableExists {
					if exists && (want == "" || name == want) {
						names = append(names, name)
					}
				}
				if len(names) > 0 {
					dbs := make([]string, 0, len(names))
					tbls := make([]string, 0, len(names))
					accs := make([]uint32, 0, len(names))
					for _, name := range names {
						dbs = append(dbs, "mo_catalog")
						tbls = append(tbls, name)
						accs = append(accs, 0)
					}
					r.NewBatchWithRowCount(len(names))
					require.NoError(t, executor.AppendStringRows(r, 0, dbs))
					require.NoError(t, executor.AppendStringRows(r, 1, tbls))
					require.NoError(t, executor.AppendFixedRows[uint32](r, 2, accs))
				}
				return r.GetResult(), nil
			}
			switch sql {
			case frontend.MoCatalogDataBranchUpdateLinkDDL:
				ddl = append(ddl, sql)
				tableExists[catalog.MO_DATA_BRANCH_UPDATE_LINK] = true
				return executor.Result{}, nil
			case frontend.MoCatalogDataBranchLinkRetentionDDL:
				ddl = append(ddl, sql)
				tableExists[catalog.MO_DATA_BRANCH_LINK_RETENTION] = true
				return executor.Result{}, nil
			}
			return executor.Result{}, nil
		})

		provenance := tenantUpgEntries[len(tenantUpgEntries)-2:]
		for _, entry := range provenance {
			require.NoError(t, entry.Upgrade(txn, 7))
		}
		require.Equal(t,
			[]string{frontend.MoCatalogDataBranchUpdateLinkDDL, frontend.MoCatalogDataBranchLinkRetentionDDL},
			ddl)
		// Second pass must be a no-op once both tables exist.
		for _, entry := range provenance {
			require.NoError(t, entry.Upgrade(txn, 7))
		}
		require.Len(t, ddl, 2, "a completed upgrade must be idempotent")
	})
}

// The consolidated handler keeps the standard lifecycle: Prepare scopes the
// catalog, only v1.2.0 may initialize the upgrade framework, and a failing
// first entry surfaces its error out of the tenant walk.
func TestProvenanceCatalogLifecycle(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := context.Background()
		ok := newVersionTxnExecutor(t, func(string) (executor.Result, error) { return executor.Result{}, nil })
		require.NoError(t, Handler.Prepare(ctx, ok, true))
		require.Error(t, Handler.HandleCreateFrameworkDeps(ok))

		injected := errors.New("injected")
		failed := newVersionTxnExecutor(t, func(string) (executor.Result, error) { return executor.Result{}, injected })
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failed), injected)
	})
}
