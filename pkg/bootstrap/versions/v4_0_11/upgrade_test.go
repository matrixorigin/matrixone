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

package v4_0_11

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCDCWatermarkColumnsUpgradeMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.11", m.Version)
	require.Equal(t, "4.0.10", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeCluster)
	require.Equal(t, versions.No, m.UpgradeTenant)
	require.Equal(t, uint32(2), m.VersionOffset, "preserve the published upgrade identity")
	require.Equal(t, defines.MORPCVersion106, m.RequiredProtocolVersion)
	require.Len(t, clusterUpgEntries, 4)
	names := make([]string, 0, len(clusterUpgEntries))
	for _, entry := range clusterUpgEntries {
		require.Equal(t, catalog.MO_CDC_WATERMARK, entry.TableName)
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, int64(defines.MORPCVersion106), entry.RequiredProtocolVersion)
		start := strings.Index(entry.UpgSql, "add column ") + len("add column ")
		names = append(names, entry.UpgSql[start:strings.Index(entry.UpgSql[start:], " ")+start])
	}
	// The prerequisites must be rechecked ahead of the entries anchored on
	// them: stores whose upgrade chain never ran the v4_0_6 handler (first
	// deployed at catalog 4.0.6, e.g. v4.2.5) arrive here without them.
	require.Equal(t,
		[]string{"source_table_id", "owner_generation", "pending_source_table_id", "target_identity"},
		names)
}

// Exercise the upgrade entry point: all CNs must support the new columns
// before any ALTER, and a failed ALTER must stop the upgrade immediately.
// Entries for already-present columns are skipped, so stores that ran the
// v4_0_6 handler upgrade identically.
func TestCDCWatermarkUpgradeAdmission(t *testing.T) {
	injected := errors.New("catalog DDL failure")
	allDDL := []string{
		"alter table mo_catalog.mo_cdc_watermark add column source_table_id bigint unsigned not null default 0 after watermark",
		"alter table mo_catalog.mo_cdc_watermark add column owner_generation bigint unsigned not null default 0 after source_table_id",
		"alter table mo_catalog.mo_cdc_watermark add column pending_source_table_id bigint unsigned null after owner_generation",
		"alter table mo_catalog.mo_cdc_watermark add column target_identity varchar(256) null after pending_source_table_id",
	}
	for _, tc := range []struct {
		name, protocol string
		have           []string // columns already present in mo_columns
		failDDL        bool
		wantDDL        []string
		wantErr        bool
	}{
		{"old", `{"result":"cn0:105"}`, nil, false, nil, true},
		{"mixed", `{"result":"cn0:106,cn1:105"}`, nil, false, nil, true},
		{"ready", `{"result":"cn0:106,cn1:106"}`, nil, false, allDDL, false},
		{"DDL failure", `{"result":"cn0:106"}`, nil, true, allDDL[:1], true},
		{"all present after retrofit", `{"result":"cn0:106"}`,
			[]string{"source_table_id", "owner_generation", "pending_source_table_id", "target_identity"},
			false, nil, false},
		{"prerequisites present only", `{"result":"cn0:106"}`,
			[]string{"source_table_id", "owner_generation"},
			false, allDDL[2:], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				op.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				var ddl []string
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "FROM mo_catalog.mo_columns"):
						attname := sql[strings.Index(sql, "attname = '")+len("attname = '"):]
						attname = attname[:strings.Index(attname, "'")]
						present := false
						for _, have := range tc.have {
							present = present || attname == have
						}
						if !present {
							return executor.Result{}, nil
						}
						r := executor.NewMemResult([]types.Type{
							types.T_varchar.ToType(), types.T_varchar.ToType(),
							types.T_int64.ToType(), types.T_int64.ToType(),
							types.T_int64.ToType(), types.T_int64.ToType(),
							types.T_int32.ToType(),
							types.T_varchar.ToType(), types.T_varchar.ToType(),
							types.T_varchar.ToType(),
						}, mp)
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{"bigint unsigned"}))
						require.NoError(t, executor.AppendStringRows(r, 1, []string{"YES"}))
						require.NoError(t, executor.AppendFixedRows(r, 2, []int64{0}))
						require.NoError(t, executor.AppendFixedRows(r, 3, []int64{20}))
						require.NoError(t, executor.AppendFixedRows(r, 4, []int64{0}))
						require.NoError(t, executor.AppendFixedRows(r, 5, []int64{0}))
						require.NoError(t, executor.AppendFixedRows(r, 6, []int32{1}))
						require.NoError(t, executor.AppendStringRows(r, 7, []string{""}))
						require.NoError(t, executor.AppendStringRows(r, 8, []string{""}))
						require.NoError(t, executor.AppendStringRows(r, 9, []string{""}))
						return r.GetResult(), nil
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{tc.protocol}))
						return r.GetResult(), nil
					default:
						ddl = append(ddl, sql)
						if tc.failDDL {
							return executor.Result{}, injected
						}
						return executor.Result{}, nil
					}
				}, op)
				err := Handler.HandleClusterUpgrade(context.Background(), txn)
				require.Equal(t, tc.wantErr, err != nil)
				if tc.failDDL {
					require.ErrorIs(t, err, injected)
				}
				require.Equal(t, tc.wantDDL, ddl)
			})
		})
	}
}
