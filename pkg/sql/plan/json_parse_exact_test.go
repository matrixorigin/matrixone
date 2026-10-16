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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJsonParseExactSQLPlanning(t *testing.T) {
	for _, sql := range []string{
		`select json_parse_exact('{"cost":9007199254740993.001}')`,
		`select cast(json_extract(json_parse_exact('{"cost":9007199254740993.001}'), '$.cost') as decimal(38,3))`,
		`select json_contains_path(json_parse_exact('{"optional":null}'), 'one', '$.optional')`,
		`select json_parse_exact('{"cost":1.001}') into outfile 'agent.parquet' format 'parquet_variant'`,
		`create table agent_exact (id bigint primary key, doc json,
		 trace_id varchar(128) as (json_extract_string(doc, '$.trace_id')) stored,
		 cost decimal(38,3) as (case when json_type(json_extract(doc, '$.cost')) in ('INTEGER','DECIMAL') then cast(json_extract(doc, '$.cost') as decimal(38,3)) else null end) stored,
		 index trace_idx(trace_id), index cost_idx(cost))`,
	} {
		_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err, sql)
	}
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		`prepare exact_agent from 'select json_parse_exact(?)'`)
	require.NoError(t, err)
	expr := findPlanFunctionExpr(p.GetDcl().GetPrepare().GetPlan(), "json_parse_exact")
	require.NotNil(t, expr)
	require.Equal(t, int32(types.T_json), expr.Typ.Id)
	require.NotNil(t, expr.GetF().Args[0].GetP())
}
