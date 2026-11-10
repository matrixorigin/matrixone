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

package disttae

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type inventoryOwnerExecutor struct {
	executor.SQLExecutor
	exec func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e inventoryOwnerExecutor) Exec(ctx context.Context, query string, opts executor.Options) (executor.Result, error) {
	return e.exec(ctx, query, opts)
}

func TestDatabaseInventoryWriteOwner(t *testing.T) {
	for _, scenario := range []string{"inventory success", "inventory missing", "procedure empty", "procedure error", "function error", "system exclusion"} {
		t.Run(scenario, func(t *testing.T) {
			resultMP := newResultOwnershipMPool(t)
			baseline := resultOwnershipBytes(resultMP)
			var observed executor.Options
			var queries []string
			injected := errors.New("injected catalog write failure")
			sqlExecutor := &inventoryOwnerExecutor{exec: func(_ context.Context, query string, opts executor.Options) (executor.Result, error) {
				observed = opts
				queries = append(queries, query)
				if scenario == "procedure error" || scenario == "function error" && query == "delete from mo_catalog.mo_user_defined_function where db='a''b'" {
					return executor.Result{}, injected
				}
				result := newRowIDResult(t, resultMP, types.RandomRowid())
				if scenario == "inventory success" {
					result.AffectedRows = 1
				}
				return result, nil
			}}
			_, _, txn, ctx := newResultOwnershipFixture(t, sqlExecutor, false)
			var err error
			switch scenario {
			case "inventory success", "inventory missing":
				err = writeDatabaseInventory(ctx, txn.op, "inventory write")
			case "system exclusion":
				err = retireDatabaseRoutines(ctx, txn.op, "MO_CATALOG")
			default:
				err = retireDatabaseRoutines(ctx, txn.op, "a'b")
			}
			switch scenario {
			case "inventory missing":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
			case "procedure error", "function error":
				require.ErrorIs(t, err, injected)
			default:
				require.NoError(t, err)
			}
			if scenario == "system exclusion" {
				require.Empty(t, queries)
			} else {
				want := 1
				if scenario == "procedure empty" || scenario == "function error" {
					want = 2
				}
				require.Len(t, queries, want, "borrow the existing owner without additional transaction SQL")
				require.Same(t, txn.op, observed.Txn())
				require.True(t, observed.DisableIncrStatement())
				require.True(t, observed.HasAccountID())
				require.EqualValues(t, 1, observed.AccountID())
				require.Equal(t, scenario == "inventory success" || scenario == "inventory missing", observed.StatementOption().RequireLock())
				if scenario == "procedure empty" {
					require.Equal(t, "delete from mo_catalog.mo_stored_procedure where db='a''b'", queries[0])
					require.Equal(t, "delete from mo_catalog.mo_user_defined_function where db='a''b'", queries[1])
				}
			}
			require.Equal(t, baseline, resultOwnershipBytes(resultMP), "catalog results must be released on all returned-row counts")
		})
	}
}

func TestDatabaseCompatibilityRetirementOwner(t *testing.T) {
	for _, scenario := range []string{"empty", "multiple settings", "write failure", "system exclusion"} {
		t.Run(scenario, func(t *testing.T) {
			resultMP := newResultOwnershipMPool(t)
			baseline := resultOwnershipBytes(resultMP)
			var observed executor.Options
			var queries []string
			injected := errors.New("compatibility retirement failed")
			sqlExecutor := &inventoryOwnerExecutor{exec: func(_ context.Context, query string, opts executor.Options) (executor.Result, error) {
				observed = opts
				queries = append(queries, query)
				if scenario == "write failure" {
					return executor.Result{}, injected
				}
				result := newRowIDResult(t, resultMP, types.RandomRowid())
				if scenario == "multiple settings" {
					result.AffectedRows = 3
				}
				return result, nil
			}}
			_, _, txn, ctx := newResultOwnershipFixture(t, sqlExecutor, false)
			name := "a'b"
			if scenario == "system exclusion" {
				name = "MO_CATALOG"
			}
			err := retireDatabaseCompatibility(ctx, txn.op, name)
			if scenario == "write failure" {
				require.ErrorIs(t, err, injected)
			} else {
				require.NoError(t, err)
			}
			if scenario == "system exclusion" {
				require.Empty(t, queries)
			} else {
				require.Equal(t, []string{"delete from mo_catalog.mo_mysql_compatibility_mode where account_id=1 and dat_name='a''b'"}, queries)
				require.Same(t, txn.op, observed.Txn())
				require.True(t, observed.DisableIncrStatement())
				require.True(t, observed.HasAccountID())
				require.EqualValues(t, 1, observed.AccountID())
			}
			require.Equal(t, baseline, resultOwnershipBytes(resultMP))
		})
	}
}
