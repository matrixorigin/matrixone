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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type orphanUpgradeTxn struct {
	executor.TxnExecutor
	t *testing.T
}

func (tx orphanUpgradeTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	require.Equal(tx.t, uint32(7), opts.AccountID())
	require.Equal(tx.t, versions.CatalogUpgradePageBytes, opts.MaxResultBytes())
	return tx.TxnExecutor.Exec(sql, opts)
}

func TestTenantUpgradeOrphanPagesRespectTransactionOutcome(t *testing.T) {
	for _, outcome := range []string{"success", "unknown commit", "completed elsewhere", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()) }()
			candidateRows := func(first, count int) executor.Result {
				result := executor.NewMemResult([]types.Type{
					types.T_int32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
					types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
				}, mp)
				result.NewBatchWithRowCount(count)
				roles := make([]int32, count)
				kinds := make([]string, count)
				ids := make([]uint64, count)
				privileges := make([]int32, count)
				levels := make([]string, count)
				keys := make([]string, count)
				for i := range keys {
					roles[i], kinds[i], privileges[i], levels[i] = 1, "account", 1, "*"
					keys[i] = fmt.Sprintf("%04x", first+i)
				}
				require.NoError(t, executor.AppendFixedRows(result, 0, roles))
				require.NoError(t, executor.AppendStringRows(result, 1, kinds))
				require.NoError(t, executor.AppendFixedRows(result, 2, ids))
				require.NoError(t, executor.AppendFixedRows(result, 3, privileges))
				require.NoError(t, executor.AppendStringRows(result, 4, levels))
				require.NoError(t, executor.AppendStringRows(result, 5, keys))
				return result.GetResult()
			}
			pages, highWaterReads := 0, 0
			tx := orphanUpgradeTxn{t: t, TxnExecutor: newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "order by __mo_cpkey_col desc"):
					highWaterReads++
					return candidateRows(1001, 1), nil
				case strings.Contains(sql, "from mo_catalog.mo_role_privs where"):
					pages++
					if strings.Contains(sql, "__mo_cpkey_col >") {
						return candidateRows(1001, 1), nil
					}
					return candidateRows(1, 1000), nil
				default:
					t.Fatalf("unexpected SQL: %s", sql)
					return executor.Result{}, nil
				}
			})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("commit outcome unknown")
			orphanCalls := 0
			active, err := Handler.UpgradeTenant(ctx, 7, 8<<20, func(operation string, limit uint64, adjustable bool, work func(executor.TxnExecutor) error) (bool, error) {
				if operation != "reconciling orphan object privileges" {
					return true, nil
				}
				orphanCalls++
				require.Equal(t, versions.CatalogUpgradePageBytes, limit)
				require.False(t, adjustable)
				if outcome == "completed elsewhere" {
					return false, nil
				}
				require.NoError(t, work(tx))
				switch outcome {
				case "unknown commit":
					return true, failure
				case "cancelled":
					cancel()
				}
				return true, nil
			})
			switch outcome {
			case "success":
				require.NoError(t, err)
				require.True(t, active)
				require.Equal(t, 2, orphanCalls)
				require.Equal(t, 2, pages)
			case "unknown commit":
				require.ErrorIs(t, err, failure)
				require.False(t, active)
				require.Equal(t, 1, pages)
			case "completed elsewhere":
				require.NoError(t, err)
				require.False(t, active)
				require.Zero(t, pages)
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, active)
				require.Equal(t, 1, pages)
			}
			if outcome != "completed elsewhere" {
				require.Equal(t, 1, highWaterReads)
			}
		})
	}
}
