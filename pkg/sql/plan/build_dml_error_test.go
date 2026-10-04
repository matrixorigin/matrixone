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

package plan

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type dmlResolveErrorCompilerContext struct {
	*MockCompilerContext
	err error
}

func (c *dmlResolveErrorCompilerContext) Resolve(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	return nil, nil, c.err
}

type dmlUnsupportedOnceCompilerContext struct {
	*MockCompilerContext
	modernRejected bool
}

func (c *dmlUnsupportedOnceCompilerContext) Resolve(schemaName, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if !c.modernRejected {
		c.modernRejected = true
		return nil, nil, moerr.NewUnsupportedDML(c.GetContext(), "force legacy planner")
	}
	return c.MockCompilerContext.Resolve(schemaName, tableName, snapshot)
}

func TestDMLPlannerPropagatesNonMoResolveErrors(t *testing.T) {
	statements := []struct {
		name string
		sql  string
	}{
		{name: "insert", sql: "insert into t values (1)"},
		{name: "load", sql: "load data inline format='csv', data='1' into table t fields terminated by ','"},
		{name: "delete", sql: "delete from t where a = 1"},
		{name: "update", sql: "update t set a = 1"},
		{name: "replace", sql: "replace into t values (1)"},
	}
	errorsToPropagate := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
		{name: "plain error", err: errors.New("catalog resolution failed")},
		{name: "wrapped canceled", err: fmt.Errorf("resolve table: %w", context.Canceled)},
	}

	for _, statement := range statements {
		t.Run(statement.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), statement.sql, 1)
			require.NoError(t, err)

			for _, resolveError := range errorsToPropagate {
				t.Run(resolveError.name, func(t *testing.T) {
					ctx := &dmlResolveErrorCompilerContext{
						MockCompilerContext: NewMockCompilerContext(true),
						err:                 resolveError.err,
					}
					var plan *Plan
					var buildErr error
					require.NotPanics(t, func() {
						plan, buildErr = BuildPlan(ctx, stmt, false)
					})
					require.Nil(t, plan)
					require.ErrorIs(t, buildErr, resolveError.err)
				})
			}
		})
	}
}

func TestDMLPlannerRoutesUnsupportedDMLToLegacyPlanner(t *testing.T) {
	statements := []struct {
		name string
		sql  string
	}{
		{name: "insert", sql: "insert into nation values (1, 'name', 1, 'comment')"},
		{name: "load", sql: "load data inline format='csv', data='1,name,1,comment' into table nation fields terminated by ','"},
		{name: "delete", sql: "delete from nation where n_nationkey = 1"},
	}

	for _, statement := range statements {
		t.Run(statement.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), statement.sql, 1)
			require.NoError(t, err)
			ctx := &dmlUnsupportedOnceCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}

			plan, err := BuildPlan(ctx, stmt, false)

			require.NoError(t, err)
			require.NotNil(t, plan)
			require.True(t, ctx.modernRejected)
		})
	}
}

func TestUpdatePlannerRejectsUnknownUnsupportedDML(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "update nation set n_name = 'x'", 1)
	require.NoError(t, err)
	ctx := &dmlUnsupportedOnceCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}

	logicPlan, err := BuildPlan(ctx, stmt, false)

	require.Nil(t, logicPlan)
	require.ErrorContains(t, err, "unsupported DML: force legacy planner")
	require.True(t, ctx.modernRejected)
}
