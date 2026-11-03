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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestMixedExistenceKeyPlanning(t *testing.T) {
	const sql = `select n.n_name from tpch.nation n where
		exists (select 1 from tpch.region r
		  where r.r_name = cast(n.n_nationkey as char)
		    and r.r_comment = case when n.n_name = 'x' then 'a' else 'b' end) or
		exists (select 1 from tpch.region r2
		  where r2.r_name = cast(n.n_nationkey as char))`
	for _, storedText := range []bool{false, true} {
		t.Run(map[bool]string{false: "varchar", true: "stored text"}[storedText], func(t *testing.T) {
			opt := NewMockOptimizer(true, newPlanTestProcess(t))
			if storedText {
				for _, c := range opt.ctxt.tables["nation"].Cols {
					if c.Name == "n_nationkey" {
						c.Typ.Id = int32(types.T_int64)
					} else if c.Name == "n_name" {
						c.Typ.Id, c.Typ.Width = int32(types.T_text), 0
					}
				}
				for _, c := range opt.ctxt.tables["region"].Cols {
					if c.Name == "r_name" || c.Name == "r_comment" {
						c.Typ.Id, c.Typ.Width = int32(types.T_text), 0
					}
				}
			}
			p, err := runOneStmt(opt, t, sql)
			require.NoError(t, err)
			require.False(t, reachablePlanHasJoinType(p.GetQuery(), planpb.Node_MARK))
			require.True(t, reachablePlanHasJoinType(p.GetQuery(), planpb.Node_SEMI))
			require.True(t, reachablePlanHasNodeType(p.GetQuery(), planpb.Node_UNION_ALL))
			var keyCounts []int
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_MARK {
					keyCounts = append(keyCounts, len(node.OnList))
					for _, cond := range node.OnList {
						f := cond.GetF()
						require.NotNil(t, f)
						id, _ := function.DecodeOverloadID(f.Func.Obj)
						require.Equal(t, int32(function.EQUAL), id, "a safe equality should remain available to hash join")
					}
				}
			}
			require.ElementsMatch(t, []int{1, 2}, keyCounts)
		})
	}
}

func TestExistentialArithmeticKeyPlanning(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
	}{
		{"outer subtraction", "r.r_regionkey = n.n_regionkey - 1"},
		{"inner addition", "r.r_regionkey + 1 = n.n_regionkey"},
		{"commuted equality", "n.n_regionkey - 1 = r.r_regionkey"},
		{"compound keys", "r.r_regionkey * 10 + r.r_regionkey = n.n_regionkey * 10 + n.n_nationkey"},
		{"negative multiplier", "r.r_regionkey = n.n_regionkey * -2"},
		{"maximum safe multiplier", "r.r_regionkey = n.n_regionkey * 4294967296"},
		{"unary negation", "r.r_regionkey = -cast(n.n_regionkey as bigint)"},
		{"residual", "r.r_regionkey = n.n_regionkey - 1 and r.r_regionkey < n.n_nationkey"},
	} {
		for _, consumer := range []struct {
			name, prefix string
			join         planpb.Node_JoinType
		}{
			{"filter", "select n.n_name from tpch.nation n where exists", planpb.Node_SEMI},
			{"negative filter", "select n.n_name from tpch.nation n where not exists", planpb.Node_ANTI},
			{"projection", "select exists", planpb.Node_MARK},
		} {
			t.Run(tc.name+"/"+consumer.name, func(t *testing.T) {
				sql := consumer.prefix + " (select 1 from tpch.region r where " + tc.predicate + ")"
				if consumer.join == planpb.Node_MARK {
					sql += " from tpch.nation n"
				}
				p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
				require.NoError(t, err)
				query := p.GetQuery()
				require.True(t, reachablePlanHasJoinType(query, consumer.join), "expected %s", consumer.join)
				if consumer.join != planpb.Node_MARK {
					require.False(t, reachablePlanHasJoinType(query, planpb.Node_MARK))
				}
				var hashKey bool
				for _, node := range reachableFlattenSubqueryNodes(query) {
					if node.NodeType != planpb.Node_JOIN || node.JoinType != consumer.join {
						continue
					}
					for _, cond := range node.OnList {
						fn := cond.GetF()
						hashKey = hashKey || fn != nil && IsEqualFunc(fn.Func.Obj)
					}
				}
				require.True(t, hashKey, "safe arithmetic equality must remain visible to hash lowering")
			})
		}
	}
}

func TestExistentialArithmeticKeepsUnsafeBarriers(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		wide      bool
	}{
		{"bigint subtraction", "n.n_regionkey - 1", true},
		{"bigint negation", "-n.n_regionkey", true},
		{"multiplication overflow", "n.n_regionkey * 4294967297", false},
		{"intermediate overflow", "(n.n_regionkey + 9223372036854775807) - 9223372036854775807", false},
		{"division", "1 / n.n_regionkey", false},
		{"volatile", "n.n_regionkey + rand()", false},
		{"fallible cast", "cast(n.n_comment as bigint) - 1", false},
	} {
		for _, prefix := range []string{"select exists", "select n.n_name from tpch.nation n where exists", "select n.n_name from tpch.nation n where not exists"} {
			t.Run(tc.name+"/"+prefix, func(t *testing.T) {
				opt := NewMockOptimizer(true, newPlanTestProcess(t))
				if tc.wide {
					for _, col := range opt.ctxt.tables["nation"].Cols {
						if col.Name == "n_regionkey" {
							col.Typ.Id = int32(types.T_int64)
						}
					}
				}
				sql := prefix + " (select 1 from tpch.region r where r.r_regionkey = " + tc.key + ")"
				if prefix == "select exists" {
					sql += " from tpch.nation n"
				}
				p, err := runOneStmt(opt, t, sql)
				require.NoError(t, err)
				require.True(t, reachablePlanHasJoinType(p.GetQuery(), planpb.Node_MARK))
				for _, node := range reachableFlattenSubqueryNodes(p.GetQuery()) {
					if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_MARK {
						continue
					}
					require.False(t, node.MarkExistenceOnly)
					require.Len(t, node.OnList, 1)
					fn := node.OnList[0].GetF()
					require.NotNil(t, fn)
					id, _ := function.DecodeOverloadID(fn.Func.Obj)
					require.Equal(t, int32(function.ISTRUE), id)
				}
			})
		}
	}
}

func TestExistentialArithmeticNonEqualityKeepsCondition(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		functionID      int32
	}{
		{"range", "r.r_regionkey > n.n_regionkey - 1", function.GREAT_THAN},
		{"mixed disjunction", "r.r_regionkey = n.n_regionkey - 1 or r.r_regionkey > n.n_regionkey + 1", function.OR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
				"select exists (select 1 from tpch.region r where "+tc.predicate+") from tpch.nation n")
			require.NoError(t, err)
			require.True(t, reachablePlanHasNode(p.GetQuery(), func(node *planpb.Node) bool {
				if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_MARK {
					return false
				}
				require.Len(t, node.OnList, 1)
				fn := node.OnList[0].GetF()
				require.NotNil(t, fn)
				id, _ := function.DecodeOverloadID(fn.Func.Obj)
				require.Equal(t, tc.functionID, id, "totality must not turn a non-equality into a hash key")
				return true
			}))
		})
	}
}

func TestExistentialStringCastTotalityBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source planpb.Type
		target planpb.Type
		want   bool
	}{
		{"signed minimum fits", planpb.Type{Id: int32(types.T_int64)}, planpb.Type{Id: int32(types.T_varchar), Width: 20}, true},
		{"signed minimum too wide", planpb.Type{Id: int32(types.T_int64)}, planpb.Type{Id: int32(types.T_varchar), Width: 19}, false},
		{"unsigned maximum fits", planpb.Type{Id: int32(types.T_uint64)}, planpb.Type{Id: int32(types.T_char), Width: 20}, true},
		{"unsigned maximum too wide", planpb.Type{Id: int32(types.T_uint64)}, planpb.Type{Id: int32(types.T_char), Width: 19}, false},
		{"string widened across metadata", planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3}, planpb.Type{Id: int32(types.T_varchar), Width: 8}, true},
		{"string narrowed", planpb.Type{Id: int32(types.T_varchar), Width: 8}, planpb.Type{Id: int32(types.T_char), Width: 7}, false},
		{"unknown source width", planpb.Type{Id: int32(types.T_varchar)}, planpb.Type{Id: int32(types.T_varchar), Width: 8}, false},
		{"unknown target width", planpb.Type{Id: int32(types.T_int32)}, planpb.Type{Id: int32(types.T_varchar)}, false},
		{"text capacity not known", planpb.Type{Id: int32(types.T_text)}, planpb.Type{Id: int32(types.T_varchar), Width: 65535}, false},
		{"float formatting not certified", planpb.Type{Id: int32(types.T_float64)}, planpb.Type{Id: int32(types.T_varchar), Width: 100}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, existentialStringCastIsTotal(tc.source, tc.target))
		})
	}
}

func TestExistentialKeyTotalityKeepsUnsafeBarriers(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
	}{
		{"narrowed string", `select exists (select 1 from tpch.region r where r.r_name = cast(n.n_comment as char(1))) from tpch.nation n`},
		{"fallible projected integer origin", `select exists (select 1 from tpch.region r where r.r_regionkey = d.k - 1) from (select cast(n.n_comment as int) as k from tpch.nation n) d`},
		{"fallible case arm", `select exists (select 1 from tpch.region r where r.r_name = case when n.n_name = 'x' then 'ok' else cast(cast(n.n_comment as unsigned) as char) end) from tpch.nation n`},
		{"volatile case arm", `select exists (select 1 from tpch.region r where r.r_name = case when n.n_name = 'x' then 'ok' else cast(rand() as char) end) from tpch.nation n`},
		{"computed unbounded string", `select exists (select 1 from tpch.region r where r.r_name = cast(repeat(n.n_name, 100000) as char)) from tpch.nation n`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			var marks int
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_MARK {
					continue
				}
				marks++
				require.Len(t, node.OnList, 1)
				f := node.OnList[0].GetF()
				require.NotNil(t, f)
				id, _ := function.DecodeOverloadID(f.Func.Obj)
				require.Equal(t, int32(function.ISTRUE), id, "fallible or volatile keys must keep their evaluation barrier")
			}
			require.Equal(t, 1, marks)
		})
	}
}

func TestMixedExistenceThreeBranches(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, `
	 select n.n_name from tpch.nation n where
	 exists(select 1 from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_name=n.n_name) or
	 exists(select 1 from tpch.nation x where x.n_regionkey=n.n_regionkey and x.n_nationkey=n.n_nationkey) or
	 exists(select 1 from tpch.region y where y.r_regionkey=n.n_regionkey)`)
	require.NoError(t, err)
	require.False(t, reachablePlanHasJoinType(p.GetQuery(), planpb.Node_MARK))
	require.True(t, reachablePlanHasJoinType(p.GetQuery(), planpb.Node_SEMI))
	require.True(t, reachablePlanHasNodeType(p.GetQuery(), planpb.Node_UNION_ALL))
}
