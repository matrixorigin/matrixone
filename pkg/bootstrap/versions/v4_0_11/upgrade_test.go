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
	require.Equal(t, defines.MORPCVersion106, m.RequiredProtocolVersion)
	require.Len(t, clusterUpgEntries, 2)
	for _, entry := range clusterUpgEntries {
		require.Equal(t, catalog.MO_CDC_WATERMARK, entry.TableName)
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, int64(defines.MORPCVersion106), entry.RequiredProtocolVersion)
	}
}

// Exercise the upgrade entry point: all CNs must support the new columns
// before either ALTER, and a failed ALTER must stop the upgrade immediately.
func TestCDCWatermarkUpgradeAdmission(t *testing.T) {
	injected := errors.New("catalog DDL failure")
	for _, tc := range []struct {
		name, protocol string
		failDDL        bool
		wantDDL        int
		wantErr        bool
	}{
		{"old", `{"result":"cn0:105"}`, false, 0, true},
		{"mixed", `{"result":"cn0:106,cn1:105"}`, false, 0, true},
		{"ready", `{"result":"cn0:106,cn1:106"}`, false, 2, false},
		{"DDL failure", `{"result":"cn0:106"}`, true, 1, true},
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
						return executor.Result{}, nil // columns do not exist yet
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
				expected := []string{
					"alter table mo_catalog.mo_cdc_watermark add column pending_source_table_id bigint unsigned null after owner_generation",
					"alter table mo_catalog.mo_cdc_watermark add column target_identity varchar(256) null after pending_source_table_id",
				}
				require.Len(t, ddl, tc.wantDDL)
				for i := range ddl {
					require.Equal(t, expected[i], ddl[i])
				}
			})
		})
	}
}
