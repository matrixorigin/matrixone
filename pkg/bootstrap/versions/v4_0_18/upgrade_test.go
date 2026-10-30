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

package v4_0_18

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

// preChangeCurrentColumnsDDL reproduces the undecorated current template a
// base-era pre-change binary persists: the V109 lineage with the MEDIUMINT
// DATA_TYPE arm (the sysview package's frozen T0 identity). The sysview
// package pins this exact construction byte-identical to its internal frozen
// pre-change template (pkg/util/sysview/view_columns_history_test.go).
func preChangeCurrentColumnsDDL() string {
	const v109DataType = "upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT' "
	const v110DataType = v109DataType + "when upper(mo_show_visible_bin(mc.atttyp,3)) like 'MEDIUMINT%' then 'MEDIUMINT' "
	return strings.ReplaceAll(sysview.InformationSchemaColumnsV109DDL(), v109DataType, v110DataType)
}

// preChangeBinaryNullColumnsDDL is the second frozen pre-change variant (T1):
// the #31461-era selector revision of the pre-change current template. A
// binary of that era persists exactly this text before the v4_0_18 entry
// recreates the view with the decorated definition.
func preChangeBinaryNullColumnsDDL() string {
	return strings.ReplaceAll(preChangeCurrentColumnsDDL(),
		"WHEN 2 then 'binary'", "WHEN 2 then NULL")
}

func TestColumnsUpgradeMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.18", m.Version)
	require.Equal(t, "4.0.7", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeTenant)
	require.Equal(t, versions.No, m.UpgradeCluster)
	// The protocol fence is inherited from the frozen consolidated terminal:
	// the view writers and definitions this forward handle installs are the
	// terminal's own.
	require.Equal(t, defines.MORPCVersion126, m.RequiredProtocolVersion)
	require.Equal(t, uint32(1), m.VersionOffset)
	require.Len(t, tenantUpgEntries, 1)

	// The frozen consolidated terminal pins: 41 = 27 tenant + 14 cluster
	// entries, protocol 126, and the 4.0.6 floor.
	terminal := v4_0_7.Handler.Metadata()
	require.Equal(t, "4.0.7", terminal.Version)
	require.Equal(t, "4.0.6", terminal.MinUpgradeVersion)
	require.Equal(t, uint32(41), terminal.VersionOffset)
	require.Equal(t, defines.MORPCVersion126, terminal.RequiredProtocolVersion)

	// The entry is main's canonical COLUMNS constructor verbatim: its fence
	// stays at main's 109, it carries no PreSql, and its UpgSql is the
	// CREATE OR REPLACE form of the current (decorated) template resolved at
	// runtime.
	entry := tenantUpgEntries[0]
	require.Equal(t, int64(defines.MORPCVersion109), entry.RequiredProtocolVersion)
	require.Empty(t, entry.PreSql, "the canonical entry has no PreSql; the CREATE OR REPLACE is atomic")
	require.Equal(t,
		strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
		entry.UpgSql)
	require.Contains(t, entry.UpgSql, "mc.att_is_unsigned",
		"the entry installs the attributed-DECIMAL decorated definition")

	// CanDirectUpgrade chain boundary on the consolidated chain: the frozen
	// terminal admits its 4.0.6 floor, and this forward handle's own floor
	// is 4.0.7, so the exact READY-4.0.7/41 predecessor is admitted and
	// 4.0.6 is not its direct input.
	require.True(t, Handler.Metadata().CanDirectUpgrade("4.0.7"))
	require.False(t, Handler.Metadata().CanDirectUpgrade("4.0.6"))
	require.True(t, v4_0_7.Handler.Metadata().CanDirectUpgrade("4.0.6"))
	require.False(t, v4_0_7.Handler.Metadata().CanDirectUpgrade("4.0.5"))
}

func TestColumnsUpgradeAdmissionAndIdempotence(t *testing.T) {
	const protocolSQL = "SELECT mo_ctl('cn', 'GetProtocolVersion', '')"
	entry := tenantUpgEntries[0]
	injected := errors.New("injected columns upgrade failure")
	for _, tc := range []struct {
		name       string
		definition string
		folded     string
		protocol   string
		failSQL    string
		wantDDL    []string
		wantErr    bool
	}{
		{name: "already current", definition: sysview.InformationSchemaColumnsDDL},
		{
			name:       "pre-change current template recreates",
			definition: preChangeCurrentColumnsDDL(),
			protocol:   `{"result":"cn0:109,cn1:109"}`,
			wantDDL:    []string{entry.UpgSql},
		},
		{
			name:       "pre-change binary-null variant recreates",
			definition: preChangeBinaryNullColumnsDDL(),
			protocol:   `{"result":"cn0:109,cn1:109"}`,
			wantDDL:    []string{entry.UpgSql},
		},
		{
			name:       "legacy V109 template recreates",
			definition: sysview.InformationSchemaColumnsV109DDL(),
			protocol:   `{"result":"cn0:109,cn1:109"}`,
			wantDDL:    []string{entry.UpgSql},
		},
		{
			name:       "legacy V58 template recreates",
			definition: sysview.InformationSchemaColumnsV58DDL(),
			protocol:   `{"result":"cn0:109,cn1:109"}`,
			wantDDL:    []string{entry.UpgSql},
		},
		{
			name:     "missing under both catalog spellings recreates",
			protocol: `{"result":"cn0:109,cn1:109"}`,
			wantDDL:  []string{entry.UpgSql},
		},
		{
			name:   "canonical under the folded spelling stays read-only",
			folded: sysview.InformationSchemaColumnsDDL,
		},
		{
			name:     "pre-change template under the folded spelling recreates",
			folded:   preChangeCurrentColumnsDDL(),
			protocol: `{"result":"cn0:109,cn1:109"}`,
			wantDDL:  []string{entry.UpgSql},
		},
		{
			name:       "old protocol defers",
			definition: sysview.InformationSchemaColumnsV109DDL(),
			protocol:   `{"result":"cn0:109,cn1:108"}`,
			wantErr:    true,
		},
		{
			name:       "failed create or replace",
			definition: sysview.InformationSchemaColumnsV109DDL(),
			protocol:   `{"result":"cn0:109"}`,
			failSQL:    entry.UpgSql,
			wantErr:    true,
			wantDDL:    []string{entry.UpgSql},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				result := func(value string) executor.Result {
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					if value != "" {
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
					}
					return r.GetResult()
				}
				definition := tc.definition
				folded := tc.folded
				var ddl []string
				// Commit-fence emulation: the MemTxnExecutor cannot model
				// transaction commit/rollback, so the mock stages the
				// replacement into pending on a successful UpgSql and the
				// test applies it only when entry.Upgrade returns nil; a
				// returned error discards the staged write. The real
				// transaction-level proof — replacement executed, owner
				// transaction aborted before the tenant marker commits, view
				// content and tenant progress both rolled back, restart
				// completed — is TestColumnsUpgradeInterruptedRouteResumes in
				// pkg/embed against a live cluster.
				var pending *string
				commit := func(err error) {
					if err == nil && pending != nil {
						definition = *pending
					}
					pending = nil
				}
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "tbl.relname = 'COLUMNS'"):
						return result(definition), nil
					case strings.Contains(sql, "tbl.relname = 'columns'"):
						return result(folded), nil
					case sql == protocolSQL:
						return result(tc.protocol), nil
					default:
						ddl = append(ddl, sql)
						if sql == tc.failSQL {
							return executor.Result{}, injected
						}
						if sql == entry.UpgSql {
							staged := sysview.InformationSchemaColumnsDDL
							pending = &staged
						}
						return executor.Result{}, nil
					}
				}, operator)
				err := entry.Upgrade(txn, 7)
				commit(err)
				if tc.failSQL != "" {
					require.ErrorIs(t, err, injected)
				} else if tc.wantErr {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantDDL, ddl)

				if tc.wantErr && tc.failSQL == "" {
					return
				}
				if tc.failSQL != "" {
					// The failed attempt's staged replacement was discarded
					// at the commit fence above (the emulated rollback), so
					// the retry must observe the staged legacy view and
					// perform the complete repair again.
					tc.failSQL = ""
					ddl = nil
					require.NoError(t, entry.Upgrade(txn, 7))
					commit(nil)
					require.Equal(t, []string{entry.UpgSql}, ddl)
				}
				ddl = nil
				require.NoError(t, entry.Upgrade(txn, 7))
				commit(nil)
				require.Empty(t, ddl, "a completed refresh must be idempotent")
			})
		})
	}
}

func TestColumnsUpgradeLifecycle(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := context.Background()
		operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
		txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			require.True(t, strings.Contains(sql, "tbl.relname = 'COLUMNS'"), sql)
			mp := mpool.MustNewZero()
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
			r.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(r, 0, []string{sysview.InformationSchemaColumnsDDL}))
			return r.GetResult(), nil
		}, operator)
		require.NoError(t, Handler.Prepare(ctx, txn, true))
		require.NoError(t, Handler.HandleTenantUpgrade(ctx, 1, txn))
		require.Error(t, Handler.HandleCreateFrameworkDeps(txn))

		injected := errors.New("injected")
		failed := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, injected }, operator)
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failed), injected)
	})
}
