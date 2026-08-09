// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRejectBoundLifecycleDDLUsesIndexedBindingLookup(t *testing.T) {
	mp := mpool.MustNewZero()
	query := func(sql string, accountID int32) (executor.Result, error) {
		require.Equal(t, int32(17), accountID)
		require.Contains(t, strings.ToLower(sql), "physical_table_id=43")
		require.Contains(t, sql, "'BLOCKED'")
		result := executor.NewMemResult(
			[]types.Type{types.T_varchar.ToType()},
			mp,
		)
		result.NewBatch()
		require.NoError(t, executor.AppendStringRows(
			result,
			0,
			[]string{"binding"},
		))
		return result.GetResult(), nil
	}
	require.ErrorContains(t, rejectBoundLifecycleDDL(
		context.Background(),
		17,
		43,
		"ALTER TABLE",
		query,
	), "UNSET LIFECYCLE")
}

func TestRejectBoundLifecycleDDLAllowsUnboundTable(t *testing.T) {
	require.NoError(t, rejectBoundLifecycleDDL(
		context.Background(),
		17,
		43,
		"TRUNCATE TABLE",
		func(string, int32) (executor.Result, error) {
			return executor.Result{}, nil
		},
	))
}

func TestRejectBoundLifecycleDDLAllowsTenantBeforeLifecycleCatalogUpgrade(t *testing.T) {
	require.NoError(t, rejectBoundLifecycleDDL(
		context.Background(),
		17,
		43,
		"ALTER TABLE",
		func(string, int32) (executor.Result, error) {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(
				"mo_catalog",
				"mo_lifecycle_bindings",
			)
		},
	))

	wantErr := moerr.NewInternalErrorNoCtx("catalog read failed")
	err := rejectBoundLifecycleDDL(
		context.Background(),
		17,
		43,
		"ALTER TABLE",
		func(string, int32) (executor.Result, error) {
			return executor.Result{}, wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
}

func TestRejectBoundLifecycleDDLAllowsSystemAccountWithoutCatalogLookup(t *testing.T) {
	called := false
	require.NoError(t, rejectBoundLifecycleDDL(
		context.Background(),
		0,
		43,
		"ALTER TABLE",
		func(string, int32) (executor.Result, error) {
			called = true
			return executor.Result{}, nil
		},
	))
	require.False(t, called)
}

func TestLifecycleDatabaseDropBindingDeleteSQLIsTenantAndDatabaseScoped(t *testing.T) {
	sql := lifecycleDatabaseDropBindingDeleteSQL(17, 43)
	require.Contains(t, sql, "account_id=17")
	require.Contains(t, sql, "database_id=43")
	require.NotContains(t, strings.ToLower(sql), "physical_table_id")
}
