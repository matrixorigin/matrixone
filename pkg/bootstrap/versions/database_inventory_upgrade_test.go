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

package versions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInventoryUpgradePageOwner(t *testing.T) {
	for _, scenario := range []string{"complete", "other worker", "create failure", "create commit unknown", "page commit unknown", "cancel between pages"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var operations, queries []string
			var observed []string
			restore := SetInventoryUpgradeCommittedHookForTest(func(_ context.Context, account int32, operation string) {
				require.EqualValues(t, 7, account)
				observed = append(observed, operation)
			})
			defer restore()
			injected := errors.New("injected upgrade operation failure")
			tx := executor.NewMemTxnExecutor(func(query string) (executor.Result, error) {
				queries = append(queries, query)
				if scenario == "create failure" {
					return executor.Result{}, injected
				}
				return executor.Result{}, nil // An empty identity page ends this component.
			}, nil)
			active, err := UpgradeDatabaseInventory(ctx, 7, 8192, func(operation string, limit uint64, adjustable bool, work func(executor.TxnExecutor) error) (bool, error) {
				operations = append(operations, operation)
				if scenario == "other worker" {
					return false, nil
				}
				if len(operations) == 1 {
					require.EqualValues(t, 8192, limit)
					require.True(t, adjustable)
				} else {
					require.EqualValues(t, CatalogUpgradePageBytes, limit)
					require.False(t, adjustable)
				}
				if err := work(tx); err != nil {
					return false, err
				}
				if scenario == "create commit unknown" || scenario == "page commit unknown" && len(operations) == 2 {
					return false, injected
				}
				if scenario == "cancel between pages" {
					cancel()
				}
				return true, nil
			})
			switch scenario {
			case "complete":
				require.NoError(t, err)
				require.True(t, active)
				require.Equal(t, []string{"create database inventory", "backfill database inventory"}, operations)
				require.Len(t, queries, 2)
				require.True(t, strings.HasPrefix(queries[0], "create table if not exists mo_catalog.mo_database_inventory"))
				require.Contains(t, queries[1], "account_id=7 and dat_id>0")
				require.Equal(t, operations, observed)
			case "other worker":
				require.NoError(t, err)
				require.False(t, active)
				require.Empty(t, queries)
				require.Empty(t, observed)
			default:
				require.False(t, active)
				if scenario == "cancel between pages" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, injected)
				}
				wantOperations := 1
				if scenario == "page commit unknown" {
					wantOperations = 2
				}
				require.Len(t, operations, wantOperations, "no page/cursor advance after an unconfirmed operation")
				if scenario == "page commit unknown" || scenario == "cancel between pages" {
					require.Equal(t, []string{"create database inventory"}, observed)
				} else {
					require.Empty(t, observed, "unconfirmed commits cannot signal the crash barrier")
				}
			}
		})
	}
	t.Run("invalid owner", func(t *testing.T) {
		_, err := UpgradeDatabaseInventory(t.Context(), 7, 8192, nil)
		require.Error(t, err)
		called := false
		for _, input := range []struct {
			account int32
			limit   uint64
		}{{-1, 8192}, {7, 0}} {
			_, err := UpgradeDatabaseInventory(t.Context(), input.account, input.limit,
				func(string, uint64, bool, func(executor.TxnExecutor) error) (bool, error) {
					called = true
					return true, nil
				})
			require.Error(t, err)
		}
		require.False(t, called)
	})
}

func TestInventoryUpgradeSuppliedTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "staged in caller", true: "caller handles failure"}[fail], func(t *testing.T) {
			var queries []string
			injected := errors.New("supplied owner page failure")
			tx := executor.NewMemTxnExecutor(func(query string) (executor.Result, error) {
				queries = append(queries, query)
				if fail && len(queries) == 2 {
					return executor.Result{}, injected
				}
				return executor.Result{}, nil
			}, nil)
			_, err := UpgradeDatabaseInventory(t.Context(), 7, DefaultCatalogUpgradeObjectBytes,
				func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
					return true, work(tx)
				})
			if fail {
				require.ErrorIs(t, err, injected)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, queries, 2, "only create/read in the supplied owner; no BEGIN, COMMIT, ROLLBACK or ready marker")
			require.True(t, strings.HasPrefix(queries[0], "create table if not exists mo_catalog.mo_database_inventory"))
			require.Contains(t, queries[1], "account_id=7 and dat_id>0")
		})
	}
}

func TestInventoryUpgradePageCommitBoundary(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			var operations, reads int
			injected := errors.New("page commit result unknown")
			tx := executor.NewMemTxnExecutor(func(query string) (executor.Result, error) {
				switch {
				case strings.HasPrefix(query, "create table "):
					return executor.Result{}, nil
				case strings.HasPrefix(query, "insert into "):
					return executor.Result{AffectedRows: DatabaseInventoryPageSize}, nil
				default:
					reads++
					if reads == 2 {
						require.Contains(t, query, fmt.Sprintf("dat_id>%d", DatabaseInventoryPageSize))
						return executor.Result{}, nil
					}
					require.Contains(t, query, "dat_id>0")
					ids := make([]uint64, DatabaseInventoryPageSize)
					for i := range ids {
						ids[i] = uint64(i + 1)
					}
					result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
					result.NewBatchWithRowCount(len(ids))
					require.NoError(t, executor.AppendFixedRows(result, 0, ids))
					return result.GetResult(), nil
				}
			}, nil)
			active, err := UpgradeDatabaseInventory(t.Context(), 7, 8192,
				func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
					operations++
					if err := work(tx); err != nil {
						return false, err
					}
					require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB(), "release page result before commit and next owner")
					if unknown && operations == 2 {
						return false, injected
					}
					return true, nil
				})
			if unknown {
				require.ErrorIs(t, err, injected)
				require.False(t, active)
				require.Equal(t, 1, reads, "an uncertain page must not advance the cursor")
				require.Equal(t, 2, operations)
			} else {
				require.NoError(t, err)
				require.True(t, active)
				require.Equal(t, 2, reads)
				require.Equal(t, 3, operations)
			}
		})
	}
}
