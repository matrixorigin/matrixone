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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/stretchr/testify/require"
)

func TestCTEIdentityPublication(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		readers   int
		recursive bool
	}{
		{"singleton", "with r as(select uuid()x) select a.x,b.x from r a join r b on a.x=b.x", 2, false},
		{"three readers", "with r as(select rand()x) select a.x,b.x,c.x from r a,r b,r c", 3, false},
		{"predicate", "with r as(select n_nationkey k from nation where rand()>0.5) select a.k,b.k from r a,r b", 2, false},
		{"ordering", "with r as(select n_nationkey k from nation order by rand() limit 2) select a.k,b.k from r a,r b", 2, false},
		{"recursive", "with recursive r(n) as(select 1 union all select n+1 from r where n<3) select a.n,b.n from r a,r b", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			setPlanTestGlobalVariable(t, mock.CurrentContext().GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			checked := false
			_, err = bindAndOptimizeSelectQueryWithValidator(planpb.Query_SELECT, mock.CurrentContext(), stmt.(*tree.Select), false, false, func(q *Query) error {
				checked = true
				var sources []int32
				for step, id := range q.Steps {
					if q.Nodes[id].ExtraOptions == materialized.CTESinkOption {
						sources = append(sources, int32(step))
						require.False(t, q.Nodes[id].RecursiveSink)
						require.False(t, q.Nodes[id].RecursiveCte)
					}
				}
				require.Len(t, sources, 1, "semantic publication is independent of optional reuse")
				tags := make(map[int32]bool)
				for id := range cteReachablePlanNodes(q) {
					n := q.Nodes[id]
					if n.NodeType == planpb.Node_SINK_SCAN && len(n.SourceStep) == 1 && n.SourceStep[0] == sources[0] {
						require.Len(t, n.BindingTags, 1)
						require.False(t, tags[n.BindingTags[0]], "each consumer needs an independent binding")
						tags[n.BindingTags[0]] = true
					}
				}
				require.Len(t, tags, tc.readers)
				if tc.recursive {
					require.Equal(t, 1, countReachableNodeType(q, planpb.Node_RECURSIVE_CTE))
					require.Equal(t, 1, countReachableNodeType(q, planpb.Node_RECURSIVE_SCAN))
				}
				return nil
			})
			require.NoError(t, err)
			require.True(t, checked)
		})
	}
}

func TestCTEIdentityStableControls(t *testing.T) {
	for _, sql := range []string{
		"with r as(select now()x) select a.x,b.x from r a,r b",
		"with r as(select cast(n_name as integer)x from nation) select a.x,b.x from r a,r b",
		"with r as(select result from generate_series(abs(1),3)g) select a.result,b.result from r a,r b",
		"with r as(select value from unnest('[1,2]')u) select a.value,b.value from r a,r b",
		"with r as(select datediff('2026-01-03','2026-01-01')x) select a.x,b.x from r a,r b",
		"with r as(select convert('abc' using utf8mb4)x) select a.x,b.x from r a,r b",
		"with r as(select extractvalue('<a>2</a>','/a')x) select a.x,b.x from r a,r b",
		"with r as(select unix_timestamp()x) select a.x,b.x from r a,r b",
		"with unused as(select rand(),missing_column from nation) select 1",
	} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		setPlanTestGlobalVariable(t, mock.CurrentContext().GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
		logic, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
		require.Zero(t, countReachableNodeType(logic.GetQuery(), planpb.Node_SINK_SCAN), sql)
	}
}

func TestCTEIdentityInvocationOwnership(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantError string }{
		{"outer capture", `with r as(select uuid()x) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"local declaration", `select (with r as(select uuid()x) select max(x) from r group by t2.a) from bvt_test2.t2`, ""},
		{"round capture", `select (with recursive base as(select uuid()x),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<t2.a) select count(*) from r) from bvt_test2.t2`, "shared CTE capture across recursive rounds"},
		{"stable recursive sibling programs", `select (with recursive r(n) as(select (select t2.a) union all select n+1 from r where n<3) select (select min(n) from r)+(select max(n) from r)) from bvt_test2.t2`, ""},
		{"stable CHAR recursive siblings", `select (with recursive r(n,s) as(select (select t2.a),cast('a' as char(8)) union all select n+1,concat(s,'') from r where n<3) select (select min(n) from r)+(select max(n) from r)) from bvt_test2.t2`, ""},
		{"stable VARCHAR recursive siblings", `select (with recursive r(n,s) as(select (select t2.a),cast('a' as varchar(8)) union all select n+1,concat(s,'') from r where n<3) select (select min(n) from r)+(select max(n) from r)) from bvt_test2.t2`, ""},
		{"stable series siblings", `select (with r as(select result k from generate_series(1,3)g) select (select min(k) from r where k>=t2.a)+(select max(k) from r)) from bvt_test2.t2`, ""},
		{"stable JSON_TABLE siblings", `select (with r as(select x from json_table('[1,2,3]','$[*]' columns(x int path '$'))j) select (select min(x) from r where x>=t2.a)+(select max(x) from r)) from bvt_test2.t2`, ""},
		{"external JSON_TABLE capture", `with r as(select x from json_table(cast('file:///missing' as datalink),'$[*]' columns(x int path '$'))j) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"volatile JSON_TABLE argument", `with r as(select x from json_table(concat('["',uuid(),'"]'),'$[*]' columns(x varchar(36) path '$'))j) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"volatile history index", `with r as(select last_query_id(cast(rand()*2 as bigint))x) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"volatile series argument", `with r as(select result k from generate_series(cast(rand()*10 as bigint),10)g) select (select max(k) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"volatile unnest argument", `with r as(select cast(value as varchar(36))x from unnest(concat('["',uuid(),'"]'))u) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"volatile charset argument", `with r as(select convert(uuid() using utf8mb4)x) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"effectful stable result", `with r as(select sleep(0)x) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"stable recursive wrapper programs", `select (with recursive r(n) as(select (select t2.a) union all select n+1 from r where n<3),s(k) as(select n+1 from r) select (select min(k) from s)+(select max(k) from s)) from bvt_test2.t2`, ""},
		{"late recursive instability", `select (with recursive r(n,x) as(select 1,cast(null as varchar(36)) union all select n+1,uuid() from r where n<t2.a) select (select max(x) from r)) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"recursive volatile predecessor", `select (with recursive base(k,x) as(select 1,uuid() union all select k+1,x from base where k<2),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<t2.a) select count(*) from r) from bvt_test2.t2`, "shared CTE capture across recursive rounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			logic, err := runOneStmt(mock, t, tc.sql)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
				require.NoError(t, logic.GetQuery().ValidateCorrelation())
			}
		})
	}
}

func TestCTEEvaluationClassification(t *testing.T) {
	for _, tc := range []struct {
		name, sql             string
		publication, unstable bool
	}{
		{"series", "select result from generate_series(abs(1),3)g", false, false},
		{"series input", "select g.result from nation n join generate_series(n.n_nationkey,n.n_nationkey+1)g on true", false, false},
		{"unnest", "select value from unnest('[1,2]')u", false, false},
		{"unnest input", `select u.value from nation n join unnest(concat('["',n.n_name,'"]'))u on true`, false, false},
		{"JSON_TABLE text", `select x from json_table('[1,2]','$[*]' columns(x int path '$'))j`, false, false},
		{"JSON_TABLE typed JSON", `select x from json_table(cast('[1,2]' as json),'$[*]' columns(x int path '$'))j`, false, false},
		{"JSON_TABLE input", `select j.x from nation n join json_table(concat('[',n.n_nationkey,']'),'$[*]' columns(x int path '$'))j on true`, false, false},
		{"JSON_TABLE NULL", `select x from json_table(null,'$[*]' columns(x int path '$'))j`, false, false},
		{"JSON_TABLE external input", `select x from json_table(cast('file:///missing' as datalink),'$[*]' columns(x int path '$'))j`, true, true},
		{"JSON_TABLE file expression", `select x from json_table(load_file('file:///missing'),'$[*]' columns(x int path '$'))j`, true, true},
		{"JSON_TABLE volatile projection", `select x,uuid() from json_table('[1,2]','$[*]' columns(x int path '$'))j`, true, true},
		{"CHAR recursion", "with recursive r(n,s)as(select 1,cast('a' as char(8)) union all select n+1,concat(s,'') from r where n<3) select * from r", true, false},
		{"VARCHAR recursion", "with recursive r(n,s)as(select 1,cast('a' as varchar(8)) union all select n+1,concat(s,'') from r where n<3) select * from r", true, false},
		{"unstable cast input", "with recursive r(n,s)as(select 1,cast('a' as char(64)) union all select n+1,concat(s,uuid()) from r where n<3) select * from r", true, true},
		{"unstable series input", "select result from generate_series(cast(rand()*10 as bigint),10)g", true, true},
		{"unstable unnest input", `select value from unnest(concat('["',uuid(),'"]'))u`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, true)
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			root, err := builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
			require.NoError(t, err)
			evaluation := builder.classifyCTEEvaluation(root, make(map[int32]bool))
			require.Equal(t, tc.publication, evaluation.needsPublication)
			require.Equal(t, tc.unstable, evaluation.valueUnstable)
		})
	}
	for _, name := range []string{"processlist", "generate_random_int64", "unknown_scan"} {
		node := &planpb.Node{NodeType: planpb.Node_FUNCTION_SCAN, TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: name}}, TblFuncExprList: []*planpb.Expr{{}}}
		require.False(t, tableFunctionHasStableIntrinsicValues(node), name)
	}
	for _, tc := range []struct {
		name   string
		typ    types.T
		stable bool
	}{
		{"json_table", types.T_json, true}, {"json_table", types.T_char, true},
		{"json_table", types.T_varchar, true}, {"json_table", types.T_text, true},
		{"json_table", types.T_datalink, false}, {"json_table", types.T_any, false},
		{"unnest", types.T_json, true}, {"unnest", types.T_varchar, true},
		{"unnest", types.T_datalink, false}, {"unnest", types.T_any, false},
	} {
		node := &planpb.Node{NodeType: planpb.Node_FUNCTION_SCAN, TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: tc.name}}, TblFuncExprList: []*planpb.Expr{{Typ: planpb.Type{Id: int32(tc.typ)}}}}
		require.Equal(t, tc.stable, tableFunctionHasStableIntrinsicValues(node), "%s %s", tc.name, tc.typ)
		node.TblFuncExprList[0] = nil
		require.False(t, tableFunctionHasStableIntrinsicValues(node))
	}
	for _, args := range [][]*planpb.Expr{nil, {{}, {}}} {
		node := &planpb.Node{NodeType: planpb.Node_FUNCTION_SCAN, TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: "json_table"}}, TblFuncExprList: args}
		require.False(t, tableFunctionHasStableIntrinsicValues(node))
	}
	require.False(t, tableFunctionHasStableIntrinsicValues(nil))
	for _, argc := range []int{0, 4} {
		node := &planpb.Node{NodeType: planpb.Node_FUNCTION_SCAN, TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: "generate_series"}}, TblFuncExprList: make([]*planpb.Expr, argc)}
		require.False(t, tableFunctionHasStableIntrinsicValues(node))
	}
}

func TestCTEIdentityCachedReaderLocking(t *testing.T) {
	for _, tc := range []struct{ sql, wantError string }{
		{"with recursive r(n)as(select 1 union all select n+1 from r where n<3) select a.n from(select n from r)a join r b on a.n=b.n for update", "FOR UPDATE on a recursive CTE"},
		{"with r as(select n_nationkey k,uuid()x from nation) select a.k from(select k from r)a join r b on a.k=b.k for update", "FOR UPDATE on a materialized CTE"},
	} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		_, err := runOneStmt(mock, t, tc.sql)
		require.ErrorContains(t, err, tc.wantError, tc.sql)
	}
}

func TestCTEIdentityRecursiveSourceSteps(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		members   int
		nullable  bool
	}{
		{"late member", `with recursive b as(select rand() x), r(n) as(select 1 union all select n+1 from r,b where n<3 and b.x>=0) select * from r`, 1, false},
		{"first of two members", `with recursive b as(select rand() x), r(n,p) as(select 1,1 union all select n+1,p from r,b where n=1 and b.x>=0 union all select n+2,null from r where n=1) select * from r`, 2, true},
		{"second of two members", `with recursive b as(select rand() x), r(n,p) as(select 1,1 union all select n+1,p from r where n=1 union all select n+2,null from r,b where n=1 and b.x>=0) select * from r`, 2, true},
		{"transitive producer", `with recursive b as(select rand() x), d as(select x from b), r(n) as(select 1 union all select n+1 from r,d where n<3 and d.x>=0) select * from r`, 1, false},
		{"anchor control", `with recursive b as(select rand() x), r(n) as(select 1 from b union all select n+1 from r,b where n<3 and b.x>=0) select * from r`, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			checked := false
			_, err = bindAndOptimizeSelectQueryWithValidator(planpb.Query_SELECT, mock.CurrentContext(), stmt.(*tree.Select), false, false, func(q *Query) error {
				var recursive *planpb.Node
				for _, node := range q.Nodes {
					if node.NodeType == planpb.Node_RECURSIVE_CTE {
						require.Nil(t, recursive)
						recursive = node
					}
				}
				require.NotNil(t, recursive)
				require.Len(t, recursive.SourceStep, tc.members+1)
				owned := make(map[int32]bool)
				for i, step := range recursive.SourceStep {
					sinkID := q.Steps[step]
					sink := q.Nodes[sinkID]
					require.Equal(t, planpb.Node_SINK, sink.NodeType)
					require.Equal(t, i > 0, sink.RecursiveCte)
					require.False(t, sink.RecursiveSink)
					require.NotEqual(t, materialized.CTESinkOption, sink.ExtraOptions)
					require.False(t, owned[sinkID], "each source must own a distinct sink")
					owned[sinkID] = true
					registrations := 0
					for _, id := range q.Steps {
						if id == sinkID {
							registrations++
						}
					}
					require.Equal(t, 1, registrations)
				}
				if tc.nullable {
					require.False(t, recursive.ProjectList[1].Typ.NotNullable, "NULL from the second member must reach the common schema")
				}
				feedback, outward := 0, 0
				for _, node := range q.Nodes {
					if node.NodeType != planpb.Node_RECURSIVE_SCAN && node.NodeType != planpb.Node_SINK_SCAN {
						continue
					}
					for _, step := range node.SourceStep {
						sink := q.Nodes[q.Steps[step]]
						if sink.RecursiveSink {
							require.Equal(t, []int32{recursive.NodeId}, sink.Children)
							if node.NodeType == planpb.Node_RECURSIVE_SCAN {
								feedback++
							} else {
								outward++
							}
						}
					}
				}
				require.Equal(t, tc.members, feedback)
				require.Equal(t, 1, outward)
				checked = true
				return nil
			})
			require.NoError(t, err)
			require.True(t, checked)
		})
	}
}
