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
	"fmt"
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
	require.Len(t, clusterUpgEntries, 4)
	require.Equal(t, uint32(len(clusterUpgEntries)), m.VersionOffset)
	for _, entry := range clusterUpgEntries {
		require.Equal(t, catalog.MO_CDC_WATERMARK, entry.TableName)
		require.Equal(t, versions.ADD_COLUMN, entry.UpgType)
		require.Equal(t, int64(defines.MORPCVersion106), entry.RequiredProtocolVersion)
	}
}

// Exercise the upgrade entry point: all CNs must support the new columns
// before any ALTER, and a failed ALTER must stop the upgrade immediately.
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
		{"ready", `{"result":"cn0:106,cn1:106"}`, false, 4, false},
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
					"alter table mo_catalog.mo_cdc_watermark add column source_table_id bigint unsigned not null default 0 after watermark",
					"alter table mo_catalog.mo_cdc_watermark add column owner_generation bigint unsigned not null default 0 after source_table_id",
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

// A reachable upgrade must repair both released six-column catalogs and
// partially upgraded catalogs, without repeating completed ALTERs on retry.
func TestCDCWatermarkUpgradePrerequisitesAndRetry(t *testing.T) {
	names := []string{"source_table_id", "owner_generation", "pending_source_table_id", "target_identity"}
	for _, existing := range []int{0, 1, 2, 3, 4} {
		t.Run(fmt.Sprintf("existing=%d", existing), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				op.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZero()
				defer func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()
				present := map[string]bool{"watermark": true}
				for _, name := range names[:existing] {
					present[name] = true
				}
				var ddl, probes int
				injected := errors.New("injected prerequisite ALTER failure")
				failOwner := existing < 2
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "FROM mo_catalog.mo_columns"):
						for _, name := range names {
							if strings.Contains(sql, "attname = '"+name+"'") && present[name] {
								return existingCDCWatermarkColumn(t, mp), nil
							}
						}
						return executor.Result{}, nil
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						probes++
						r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{`{"result":"cn0:106"}`}))
						return r.GetResult(), nil
					default:
						for i, entry := range clusterUpgEntries {
							if sql != entry.UpgSql {
								continue
							}
							anchor := "watermark"
							if i > 0 {
								anchor = names[i-1]
							}
							require.True(t, present[anchor], "ALTER prerequisite must already exist")
							require.False(t, present[names[i]], "existing columns must not be altered")
							if names[i] == "owner_generation" && failOwner {
								return executor.Result{}, injected
							}
							present[names[i]] = true
							ddl++
							return executor.Result{}, nil
						}
						t.Fatalf("unexpected upgrade SQL")
						return executor.Result{}, nil
					}
				}, op)
				err := Handler.HandleClusterUpgrade(t.Context(), txn)
				if failOwner {
					require.ErrorIs(t, err, injected)
					require.False(t, present["pending_source_table_id"], "stop before dependent ALTER")
					failOwner = false
					require.NoError(t, Handler.HandleClusterUpgrade(t.Context(), txn))
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, len(names)-existing, ddl)
				for _, name := range names {
					require.True(t, present[name])
				}
				before := probes
				require.NoError(t, Handler.HandleClusterUpgrade(t.Context(), txn))
				require.Equal(t, before, probes, "idempotent retry must not probe or ALTER")
				require.Equal(t, len(names)-existing, ddl)
			})
		})
	}
}

func existingCDCWatermarkColumn(t *testing.T, mp *mpool.MPool) executor.Result {
	t.Helper()
	r := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	}, mp)
	r.NewBatchWithRowCount(1)
	for col, value := range map[int]string{0: "bigint unsigned", 1: "NO", 7: "0", 8: "", 9: ""} {
		require.NoError(t, executor.AppendStringRows(r, col, []string{value}))
	}
	for col := 2; col <= 5; col++ {
		require.NoError(t, executor.AppendFixedRows(r, col, []int64{0}))
	}
	require.NoError(t, executor.AppendFixedRows(r, 6, []int32{7}))
	return r.GetResult()
}
