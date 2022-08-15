// Copyright 2021 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type compileTestCase struct {
	sql  string
	pn   *plan.Plan
	e    engine.Engine
	proc *process.Process
}

var (
	tcs []compileTestCase
)

func init() {
	tcs = []compileTestCase{
		newTestCase("select 1", new(testing.T)),
		newTestCase("select * from R", new(testing.T)),
		newTestCase("select * from R where uid > 1", new(testing.T)),
		newTestCase("select * from R order by uid", new(testing.T)),
		newTestCase("select * from R order by uid limit 1", new(testing.T)),
		newTestCase("select * from R limit 1", new(testing.T)),
		newTestCase("select * from R limit 2, 1", new(testing.T)),
		newTestCase("select count(*) from R", new(testing.T)),
		newTestCase("select * from R join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R left join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R right join S on R.uid = S.uid", new(testing.T)),
		newTestCase("select * from R join S on R.uid > S.uid", new(testing.T)),
		newTestCase("insert into R select * from R", new(testing.T)),
	}
}

func testPrint(_ interface{}, _ *batch.Batch) error {
	return nil
}

func TestCompile(t *testing.T) {
	for _, tc := range tcs {
		c := New("test", tc.sql, "", context.TODO(), tc.e, tc.proc, nil)
		err := c.Compile(tc.pn, nil, testPrint)
		require.NoError(t, err)
		c.GetAffectedRows()
		err = c.Run(0)
		require.NoError(t, err)
	}
}

func TestEncode(t *testing.T) {
	for _, tc := range tcs {
		c := New("test", tc.sql, "", context.TODO(), tc.e, tc.proc, nil)
		err := c.Compile(tc.pn, nil, testPrint)
		require.NoError(t, err)
		data, err := types.Encode(c.scope)
		require.NoError(t, err)
		s := new(Scope)
		err = types.Decode(data, s)
		require.NoError(t, err)
		c.scope.equal(t, s)
	}
}

func newTestCase(sql string, t *testing.T) compileTestCase {
	proc := testutil.NewProcess()
	e := memEngine.NewTestEngine()
	opt := plan2.NewBaseOptimizer(e.(*memEngine.MemEngine))
	stmts, err := mysql.Parse(sql)
	require.NoError(t, err)
	qry, err := opt.Optimize(stmts[0])
	require.NoError(t, err)
	return compileTestCase{
		e:    e,
		sql:  sql,
		proc: proc,
		pn: &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: qry,
			},
		},
	}
}

func (s *Scope) equal(t *testing.T, r *Scope) {
	require.Equal(t, s.Magic, r.Magic)
	require.Equal(t, s.IsEnd, r.IsEnd)
	require.Equal(t, s.Plan, r.Plan)
	{
		if s.DataSource != nil {
			if s.DataSource.Bat != nil {
				for i, vec := range s.DataSource.Bat.Vecs {
					require.Equal(t, vec.Col, r.DataSource.Bat.Vecs[i].Col)
				}
			}
			require.Equal(t, s.DataSource.SchemaName, r.DataSource.SchemaName)
			require.Equal(t, s.DataSource.RelationName, r.DataSource.RelationName)
			require.Equal(t, s.DataSource.Attributes, r.DataSource.Attributes)
		}
	}
	require.Equal(t, s.NodeInfo, r.NodeInfo)
	for i := range s.PreScopes {
		s.PreScopes[i].equal(t, r.PreScopes[i])
	}
}
