// Copyright 2025 Matrix Origin
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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_runSql(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()

	ctx := context.Background()
	proc.Ctx = context.Background()
	proc.ReplaceTopCtx(ctx)

	compilerContext := NewMockCompilerContext2(ctrl)
	compilerContext.EXPECT().GetProcess().Return(proc).AnyTimes()

	_, err := runSql(compilerContext, "")
	require.Error(t, err, "internal error: no account id in context")
}

func Test_buildPostDmlFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.Nil(t, err)
	}

}

func Test_buildPreDeleteFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.Nil(t, err)
	}

}

// Test WITH clause support for INSERT statement (Issue #22583)
func TestBuildWithInsert(t *testing.T) {
	sqls := []string{
		"WITH cte AS (SELECT * FROM t1 WHERE id = 1) INSERT INTO t1 SELECT id + 10, name, value FROM cte",
		"WITH cte AS (SELECT id, name FROM t1) INSERT INTO t1 (id, name, value) SELECT id + 20, name, 100 FROM cte",
		"WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM cte1 WHERE id > 5) INSERT INTO t1 SELECT id + 30, name, value FROM cte2",
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			// Just verify the SQL can be parsed and the WITH clause is present
			stmts, err := mysql.Parse(context.TODO(), sql, 1)
			require.NoError(t, err)
			require.Equal(t, 1, len(stmts))

			ins, ok := stmts[0].(*tree.Insert)
			require.True(t, ok)
			require.NotNil(t, ins.With, "INSERT.With should not be nil")
			require.Greater(t, len(ins.With.CTEs), 0, "WITH clause should have at least one CTE")
		})
	}
}
