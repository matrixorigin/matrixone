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

package plan

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestWrappedCompilerContextKeepsScopedExecutor(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock := NewMockCompilerContext(false, proc)
	for _, ctx := range []CompilerContext{
		divPrecisionCompilerContext{CompilerContext: mock},
		tableDumpBindContext{CompilerContext: mock, ctx: mock.GetContext()},
		tableDumpBindContext{CompilerContext: divPrecisionCompilerContext{CompilerContext: mock}, ctx: mock.GetContext()},
	} {
		provider := ctx.(interface {
			getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
		})
		exec, ok := provider.getInternalSQLExecutor(proc)
		require.True(t, ok)
		require.Same(t, mock.internalSQLExecutor, exec)
		for _, other := range []*process.Process{nil, {}} {
			exec, ok = provider.getInternalSQLExecutor(other)
			require.False(t, ok)
			require.Nil(t, exec)
		}
		result, err := runSqlWithSnapshot(ctx, "select 1", nil)
		require.NoError(t, err)
		result.Close()
	}
	// Embedding only the public compiler contract deliberately hides the
	// optional executor. Wrappers must preserve the normal runtime fallback.
	publicOnly := struct{ CompilerContext }{mock}
	for _, provider := range []interface {
		getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
	}{divPrecisionCompilerContext{CompilerContext: publicOnly}, tableDumpBindContext{CompilerContext: publicOnly}} {
		exec, ok := provider.getInternalSQLExecutor(proc)
		require.False(t, ok)
		require.Nil(t, exec)
	}
}

func TestDatabaseDefaultsMissingDatabaseError(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion110)
	failure := errors.New("catalog unavailable")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not visible", moerr.GetOkExpectedEOB()},
		{"engine error", failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.GetDatabaseIdFunc = func(name string, _ *Snapshot) (uint64, error) {
				require.Equal(t, "missing", name)
				return 0, tc.err
			}
			defaults, err := GetDatabaseDefaults(ctx, "missing", nil)
			require.Nil(t, defaults)
			if tc.err == failure {
				require.ErrorIs(t, err, failure)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB))
				require.ErrorContains(t, err, "Unknown database missing")
			}
		})
	}
}

func TestSubscriptionShowCreateUsesSubscriberDefault(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion110)
	for _, identity := range []uint32{2, 3} {
		t.Run(map[uint32]string{2: "binary subscriber", 3: "general subscriber"}[identity], func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			mock := &optimizer.ctxt
			def, err := buildTestCreateTableStmt(optimizer, "create table publisher_db.t(v varchar(8)) collate utf8mb4_general_ci")
			require.NoError(t, err)
			sub := &SubscriptionMeta{SubName: "subscriber_db", DbName: "publisher_db", AccountId: 7}
			ctx := &subscriptionMetadataTestContext{MockCompilerContext: mock, subscription: sub, querying: sub}
			mock.GetDatabaseIdFunc = func(name string, _ *Snapshot) (uint64, error) {
				require.Equal(t, sub.SubName, name, "publisher database is not owned by the subscriber context")
				return 42, nil
			}
			mock.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				require.Equal(t, DatabaseDefaultsSelectSQL(0, 42), sql)
				return defaultsResult(t, mock.GetProcess().Mp(), []uint32{identity}, []uint32{0}, []uint64{1}), nil
			})
			sql, stmt, err := ConstructCreateTableSQL(ctx, def, nil, false, nil)
			require.NoError(t, err)
			defer stmt.Free()
			if identity == 2 {
				require.Contains(t, sql, "COLLATE=utf8mb4_general_ci")
			} else {
				require.NotContains(t, sql, "COLLATE=")
			}
			require.Equal(t, uint32(types.CharsetUTF8), def.DefaultCharset)
			require.Same(t, sub, ctx.GetQueryingSubscription())
		})
	}
}
