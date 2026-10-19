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

package compile

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type dependentFixtureSource struct {
	batch *batch.Batch
	next  int
	rows  int
	close func()
}

func TestDependentValueSourceAllocation(t *testing.T) {
	p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer p.Free()
	budget, err := p.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<26, budget)
	require.NoError(t, err)
	defer func() {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.T_int32.ToType().PlanType()
	n := &plan.Node{NodeType: plan.Node_VALUE_SCAN,
		TableDef:   &plan.TableDef{Cols: []*plan.ColDef{{Name: "k", Typ: typ}}},
		RowsetData: &plan.RowsetData{Cols: []*plan.ColData{{}}},
	}
	for _, v := range []int32{1, 2} {
		n.RowsetData.Cols[0].Data = append(n.RowsetData.Cols[0].Data, &plan.RowsetExpr{
			Expr: &plan.Expr{Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: v}}}},
		})
	}
	c := &Compile{proc: p}
	factory, err := c.compileDependentSource(n)
	require.NoError(t, err)
	source, err := factory(p, allocation, account)
	require.NoError(t, err)
	defer source.Close()
	b, err := source.Read(p.Ctx)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](b.Vecs[0]))
	require.Same(t, allocation, b.Vecs[0].AllocationAccountSelection())
	require.NoError(t, source.Close())
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, budget.Used())
}

func (s *dependentFixtureSource) Read(context.Context) (*batch.Batch, error) {
	if s.next == s.rows {
		return nil, nil
	}
	s.next++
	return s.batch, nil
}
func (s *dependentFixtureSource) Close() error { s.close(); return nil }

func TestDependentUnionEqualityKeys(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	typ := types.T_varchar.ToType().PlanType()
	for _, kind := range []plan.Node_NodeType{plan.Node_UNION, plan.Node_UNION_ALL} {
		for _, physical := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/physical=%v", kind, physical), func(t *testing.T) {
				n := &plan.Node{NodeType: kind, ProjectList: []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}}
				if physical {
					n.PhysicalEqualityKeyList = []*plan.Expr{plan2.GetColExpr(typ, 0, 0)}
				}
				c := &Compile{proc: proc}
				factory, _, err := c.compileDependentKernel(n, []*plan.Node{{}, {}})
				require.NoError(t, err)
				op, err := factory(proc, nil)
				require.NoError(t, err)
				if kind == plan.Node_UNION_ALL {
					concat := op.(*unionall.UnionAll)
					defer concat.Release()
					return
				}
				distinct := op.(*group.Group)
				defer distinct.Release()
				defer distinct.GetChildren(0).(*unionall.UnionAll).Release()
				if physical {
					require.Len(t, distinct.GroupBy, 2)
					require.Equal(t, []int32{1}, distinct.GroupByHashKey)
					require.True(t, proto.Equal(n.PhysicalEqualityKeyList[0], distinct.GroupBy[1]))
					require.Len(t, distinct.ProjectList, 1)
					require.Zero(t, distinct.ProjectList[0].GetCol().ColPos)
				} else {
					require.Len(t, distinct.GroupBy, 1)
					require.Empty(t, distinct.GroupByHashKey)
					require.Empty(t, distinct.ProjectList)
				}
			})
		}
	}
}

func TestDependentNativeStages(t *testing.T) {
	cases := []struct {
		name, sql string
		want      []string
	}{
		{"independent_equi_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a JOIN NATION b ON a.N_NATIONKEY=b.N_NATIONKEY`, []string{"1", "2", "3"}},
		{"independent_left_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a LEFT JOIN NATION b ON a.N_NATIONKEY=b.N_NATIONKEY+1`, []string{"1", "2", "3"}},
		{"independent_full_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a FULL JOIN NATION b ON a.N_NATIONKEY+1=b.N_NATIONKEY`, []string{"1", "2", "3", "NULL"}},
		{"independent_semi_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a WHERE a.N_NATIONKEY IN (SELECT b.N_NATIONKEY FROM NATION b WHERE b.N_NATIONKEY<3)`, []string{"1", "2"}},
		{"independent_anti_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a WHERE a.N_NATIONKEY NOT IN (SELECT b.N_NATIONKEY FROM NATION b WHERE b.N_NATIONKEY<3)`, []string{"3"}},
		{"independent_equi_residual", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a JOIN NATION b ON a.N_NATIONKEY=b.N_NATIONKEY AND a.N_REGIONKEY>b.N_NATIONKEY`, []string{"1", "2", "3"}},
		{"independent_empty_join", `SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a JOIN NATION b ON a.N_NATIONKEY=b.N_NATIONKEY AND a.N_REGIONKEY<b.N_NATIONKEY`, []string{}},
		{"found_rows_limit", `SELECT SQL_CALC_FOUND_ROWS (SELECT (SELECT n1.N_NATIONKEY)) FROM NATION n1 LIMIT 1`, []string{"1"}},
		{"found_rows_offset", `SELECT SQL_CALC_FOUND_ROWS (SELECT (SELECT n1.N_NATIONKEY)) FROM NATION n1 LIMIT 1 OFFSET 1`, []string{"2"}},
		{"found_rows_zero", `SELECT SQL_CALC_FOUND_ROWS (SELECT (SELECT n1.N_NATIONKEY FROM NATION n2)) FROM NATION n1 LIMIT 0`, []string{}},
		{"found_rows_demand", `SELECT SQL_CALC_FOUND_ROWS CASE WHEN n1.N_NATIONKEY=1 THEN (SELECT (SELECT n1.N_NATIONKEY)) ELSE (SELECT (SELECT n1.N_NATIONKEY FROM NATION n2)) END FROM NATION n1 LIMIT 1`, []string{"1"}},
		{"time_window", `SELECT (SELECT SUM(s) FROM (SELECT SUM((SELECT n1.N_NATIONKEY)) s FROM (SELECT CAST('2026-01-01' AS DATETIME) ts FROM NATION n2) x INTERVAL(ts,10,MINUTE)) w) FROM NATION n1`, []string{"3", "6", "9"}},
		{"time_window_fill_value", `SELECT (SELECT SUM(s) FROM (SELECT SUM((SELECT n1.N_NATIONKEY)) s FROM (SELECT CASE WHEN n2.N_NATIONKEY=1 THEN CAST('2026-01-01 00:00:00' AS DATETIME) ELSE CAST('2026-01-01 00:20:00' AS DATETIME) END ts FROM NATION n2 WHERE n2.N_NATIONKEY<>2) x INTERVAL(ts,10,MINUTE) GAPFILL(PARTITION) FILL(VALUE, (SELECT (SELECT n1.N_NATIONKEY+10)))) w) FROM NATION n1`, []string{"13", "16", "19"}},
		{"time_window_fill_next", `SELECT (SELECT SUM(s) FROM (SELECT SUM((SELECT n1.N_NATIONKEY)) s FROM (SELECT CASE WHEN n2.N_NATIONKEY=1 THEN CAST('2026-01-01 00:00:00' AS DATETIME) ELSE CAST('2026-01-01 00:20:00' AS DATETIME) END ts FROM NATION n2 WHERE n2.N_NATIONKEY<>2) x INTERVAL(ts,10,MINUTE) GAPFILL(PARTITION) FILL(NEXT)) w) FROM NATION n1`, []string{"3", "6", "9"}},
		{"cte_grouped_window", `SELECT (WITH x(k) AS (SELECT n1.N_NATIONKEY) SELECT ROW_NUMBER() OVER (ORDER BY COUNT(*)) FROM x GROUP BY k) FROM NATION n1`, []string{"1", "1", "1"}},
		{"exists_sorted_offset", `SELECT EXISTS(SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2 ORDER BY n2.N_NATIONKEY LIMIT 1 OFFSET 1) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_sorted_empty", `SELECT EXISTS(SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2 ORDER BY n2.N_NATIONKEY LIMIT 1 OFFSET 3) FROM NATION n1`, []string{"false", "false", "false"}},
		{"exists_filter_demands_value", `SELECT EXISTS(SELECT 1 FROM NATION n2 WHERE (SELECT n1.N_NATIONKEY FROM NATION n3)>0 ORDER BY n2.N_NATIONKEY) FROM NATION n1`, nil},
		{"grouped_outer_key", `SELECT (SELECT COUNT(*) FROM NATION n2 GROUP BY n1.N_NATIONKEY) FROM NATION n1`, []string{"3", "3", "3"}},
		{"grouped_having", `SELECT (SELECT COUNT(*) FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY) GROUP BY n2.N_REGIONKEY HAVING COUNT(*)=1) FROM NATION n1`, []string{"1", "1", "1"}},
		{"exists_grouped_unused_aggregate", `SELECT EXISTS(SELECT SUM((SELECT n1.N_NATIONKEY FROM NATION n3)) FROM NATION n2 GROUP BY n2.N_REGIONKEY) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_unused_window", `SELECT EXISTS(SELECT SUM((SELECT n1.N_NATIONKEY FROM NATION n3)) OVER () FROM NATION n2) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_union_all_projection", `SELECT EXISTS(SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2 WHERE n2.N_NATIONKEY<0 UNION ALL SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_group_demands_key", `SELECT EXISTS(SELECT 1 FROM NATION n2 GROUP BY (SELECT n1.N_NATIONKEY FROM NATION n3)) FROM NATION n1`, nil},
		{"exists_global_aggregate_ignores_argument", `SELECT EXISTS(SELECT SUM((SELECT n1.N_NATIONKEY FROM NATION n3)) FROM NATION n2) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_global_aggregate_empty", `SELECT EXISTS(SELECT SUM((SELECT n1.N_NATIONKEY FROM NATION n3)) FROM NATION n2 WHERE n2.N_NATIONKEY<0) FROM NATION n1`, []string{"true", "true", "true"}},
		{"exists_having_demands_argument", `SELECT EXISTS(SELECT SUM((SELECT n1.N_NATIONKEY FROM NATION n3)) s FROM NATION n2 HAVING s>0) FROM NATION n1`, nil},
		{"exists_sorted_projection", `SELECT EXISTS(SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2 ORDER BY n2.N_NATIONKEY) FROM NATION n1`, []string{"true", "true", "true"}},
		{"cte_two_programs", `WITH x AS (SELECT N_NATIONKEY FROM NATION) SELECT (SELECT (SELECT COUNT(*) FROM x WHERE x.N_NATIONKEY=n1.N_NATIONKEY))+(SELECT (SELECT SUM(x.N_NATIONKEY) FROM x WHERE x.N_NATIONKEY=n1.N_NATIONKEY)) FROM NATION n1`, []string{"2", "3", "4"}},
		{"sorted_limit_skips_projection", `SELECT (SELECT (SELECT n1.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY<=n1.N_NATIONKEY)) FROM NATION n1 ORDER BY n1.N_NATIONKEY LIMIT 1`, []string{"1"}},
		{"sorted_offset_skips_projection", `SELECT CASE WHEN n1.N_NATIONKEY=2 THEN 7 ELSE (SELECT (SELECT n1.N_NATIONKEY FROM NATION n2)) END FROM NATION n1 ORDER BY n1.N_NATIONKEY LIMIT 1 OFFSET 1`, []string{"7"}},
		{"sorted_limit_demands_key", `SELECT (SELECT (SELECT n1.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY<=n1.N_NATIONKEY)) v FROM NATION n1 ORDER BY v LIMIT 1`, nil},
		{"cte_empty_aggregate_exists", `SELECT EXISTS(WITH x(k) AS (SELECT n1.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY<0) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"true", "true", "true"}},
		{"cte_union_missing_identity", `SELECT EXISTS(WITH x(k) AS (SELECT n1.N_NATIONKEY) SELECT k FROM x UNION ALL SELECT 7) FROM NATION n1`, []string{"true", "true", "true"}},
		{"cte_non_equality_having", `SELECT (WITH x(k) AS (SELECT n1.N_REGIONKEY) SELECT COUNT(k) FROM x WHERE k<=n1.N_NATIONKEY HAVING COUNT(k)=0) FROM NATION n1`, []string{"0", "0", "0"}},
		{"cte_recursive_guard", `SELECT CASE WHEN n1.N_NATIONKEY=2 THEN (WITH RECURSIVE x(k) AS (SELECT n1.N_NATIONKEY UNION ALL SELECT k FROM x WHERE k=1) SELECT COUNT(*) FROM x) ELSE 0 END FROM NATION n1`, []string{"0", "1", "0"}},
		{"cte_captured_shared", `SELECT (WITH x AS (SELECT n2.N_NATIONKEY+n1.N_NATIONKEY k FROM NATION n2) SELECT (SELECT MIN(k) FROM x)+(SELECT MAX(k) FROM x)) FROM NATION n1`, []string{"6", "8", "10"}},
		{"cte_capture_shadowed_use", `SELECT (WITH x AS (SELECT n2.N_NATIONKEY+n1.N_NATIONKEY k FROM NATION n2) SELECT (SELECT MIN(k) FROM x,NATION n1 WHERE n1.N_NATIONKEY=3)+(SELECT MAX(k) FROM x)) FROM NATION n1`, []string{"6", "8", "10"}},
		{"cte_capture_two_ancestors", `SELECT (SELECT (WITH x AS (SELECT n1.N_NATIONKEY+n2.N_NATIONKEY k) SELECT (SELECT k FROM x)) FROM NATION n2 WHERE n2.N_NATIONKEY=2) FROM NATION n1`, []string{"3", "4", "5"}},
		{"cte_capture_definition_chain", `SELECT (WITH x AS (SELECT n2.N_NATIONKEY+n1.N_NATIONKEY k FROM NATION n2),y AS (SELECT k+1 j FROM x) SELECT (SELECT MIN(j) FROM y)) FROM NATION n1`, []string{"3", "4", "5"}},
		{"recursive_cte_two_programs", `SELECT (WITH RECURSIVE x(k) AS (SELECT (SELECT n1.N_NATIONKEY) UNION ALL SELECT k+1 FROM x WHERE k<3) SELECT (SELECT MIN(k) FROM x)+(SELECT MAX(k) FROM x)) FROM NATION n1`, []string{"4", "5", "6"}},
		{"cross_apply", `SELECT (SELECT COUNT(g.result) FROM NATION n2 CROSS APPLY generate_series(n2.N_NATIONKEY,3) g WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) FROM NATION n1`, []string{"3", "2", "1"}},
		{"outer_apply", `SELECT (SELECT 10*COUNT(*)+COUNT(g.result) FROM NATION n2 OUTER APPLY generate_series(n2.N_NATIONKEY,0,1) g WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) FROM NATION n1`, []string{"10", "10", "10"}},
		{"sample_argument", `SELECT (SELECT SUM(x.k) FROM (SELECT SAMPLE((SELECT n1.N_NATIONKEY),100 PERCENT) k FROM NATION n2) x) FROM NATION n1`, []string{"3", "6", "9"}},
		{"table_function", `SELECT (SELECT COUNT(*) FROM generate_series(1,3) g WHERE g.result>=(SELECT n1.N_NATIONKEY)) FROM NATION n1`, []string{"3", "2", "1"}},
		{"table_function_capture", `SELECT (SELECT (SELECT COUNT(*) FROM generate_series(n1.N_NATIONKEY,3) g)) FROM NATION n1`, []string{"3", "2", "1"}},
		{"quantified_first_row", `SELECT (SELECT n1.N_NATIONKEY = ANY (SELECT CASE WHEN n2.N_NATIONKEY=1 THEN n1.N_NATIONKEY ELSE (SELECT n1.N_NATIONKEY FROM NATION n3) END FROM NATION n2)) FROM NATION n1`, []string{"true", "true", "true"}},
		{"all_first_row", `SELECT (SELECT n1.N_NATIONKEY = ALL (SELECT CASE WHEN n2.N_NATIONKEY=1 THEN n1.N_NATIONKEY+1 ELSE (SELECT n1.N_NATIONKEY FROM NATION n3) END FROM NATION n2)) FROM NATION n1`, []string{"false", "false", "false"}},
		{"exists_ignores_projection", `SELECT EXISTS(SELECT (SELECT n1.N_NATIONKEY FROM NATION n3) FROM NATION n2) FROM NATION n1`, []string{"true", "true", "true"}},
		{"limit_first_row", `SELECT (SELECT CASE WHEN n2.N_NATIONKEY=1 THEN (SELECT n1.N_NATIONKEY) ELSE (SELECT n1.N_NATIONKEY FROM NATION n3) END FROM NATION n2 LIMIT 1) FROM NATION n1`, []string{"1", "2", "3"}},
		{"offset_skips_projection", `SELECT (SELECT CASE WHEN n2.N_NATIONKEY=2 THEN (SELECT n1.N_NATIONKEY) ELSE (SELECT n1.N_NATIONKEY FROM NATION n3) END FROM NATION n2 LIMIT 1 OFFSET 1) FROM NATION n1`, []string{"1", "2", "3"}},
		{"asof_inner", `SELECT (SELECT COUNT(b.k) FROM (SELECT n2.N_NATIONKEY k,CAST('2026-01-02' AS DATETIME) ts FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) a ASOF JOIN (SELECT n3.N_NATIONKEY k,CAST('2026-01-01' AS DATETIME) ts FROM NATION n3) b ON a.k=b.k AND a.ts>=b.ts) FROM NATION n1`, []string{"1", "1", "1"}},
		{"asof_left_unmatched", `SELECT (SELECT COUNT(b.k) FROM (SELECT n2.N_NATIONKEY k,CAST('2026-01-01' AS DATETIME) ts FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) a ASOF LEFT JOIN (SELECT n3.N_NATIONKEY k,CAST('2026-01-02' AS DATETIME) ts FROM NATION n3) b ON a.k=b.k AND a.ts>=b.ts) FROM NATION n1`, []string{"0", "0", "0"}},
		{"empty_count", `SELECT (SELECT COUNT(COALESCE((SELECT COUNT(*) FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY+10),0)) FROM NATION n2) FROM NATION n1`, []string{"3", "3", "3"}},
		{"scalar_count", `SELECT (SELECT (SELECT COUNT(*) FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"1", "1", "1"}},
		{"aggregate_offset", `SELECT (SELECT MAX(n2.N_REGIONKEY) FROM NATION n2 WHERE n2.N_REGIONKEY=(SELECT MAX(n3.N_REGIONKEY) FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY LIMIT 1 OFFSET 1)) FROM NATION n1`, []string{"NULL", "NULL", "NULL"}},
		{"ordered_offset", `SELECT (SELECT n2.N_REGIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY >= (SELECT n1.N_NATIONKEY) ORDER BY n2.N_REGIONKEY DESC LIMIT 1 OFFSET 1) FROM NATION n1`, []string{"20", "20", "NULL"}},
		{"window", `SELECT (SELECT (SELECT ROW_NUMBER() OVER (ORDER BY n3.N_NATIONKEY) FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"1", "1", "1"}},
		{"window_argument", `SELECT (SELECT SUM((SELECT n1.N_NATIONKEY)) OVER (ORDER BY n2.N_NATIONKEY ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"1", "2", "3"}},
		{"union_pad_space", `SELECT (SELECT COUNT(*) FROM (SELECT CAST('a' AS CHAR(2)) k WHERE (SELECT n1.N_NATIONKEY)>0 UNION SELECT CAST('a ' AS VARCHAR(2))) u) FROM NATION n1`, []string{"1", "1", "1"}},
		{"union_all_pad_space", `SELECT (SELECT COUNT(*) FROM (SELECT CAST('a' AS CHAR(2)) k WHERE (SELECT n1.N_NATIONKEY)>0 UNION ALL SELECT CAST('a ' AS VARCHAR(2))) u) FROM NATION n1`, []string{"2", "2", "2"}},
		{"window_partition_capture", `SELECT (SELECT SUM(n2.N_NATIONKEY) OVER (PARTITION BY (SELECT n1.N_NATIONKEY)) FROM NATION n2 LIMIT 1) FROM NATION n1`, []string{"6", "6", "6"}},
		{"union_all", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY) UNION ALL SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"2", "2", "2"}},
		{"union_distinct", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY) UNION SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"1", "1", "1"}},
		{"except_distinct", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY>=(SELECT n1.N_NATIONKEY) EXCEPT SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"2", "1", "0"}},
		{"except_all", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY>=(SELECT n1.N_NATIONKEY) EXCEPT ALL SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"2", "1", "0"}},
		{"intersect_distinct", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY>=(SELECT n1.N_NATIONKEY) INTERSECT SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"1", "1", "1"}},
		{"intersect_all", `SELECT (SELECT COUNT(*) FROM (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY>=(SELECT n1.N_NATIONKEY) INTERSECT ALL SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) u) FROM NATION n1`, []string{"1", "1", "1"}},
		{"right_join_pair", `SELECT (SELECT COUNT(a.N_NATIONKEY) FROM NATION a RIGHT JOIN NATION b ON a.N_NATIONKEY=(SELECT n1.N_NATIONKEY+b.N_NATIONKEY)) FROM NATION n1`, []string{"2", "1", "0"}},
		{"cte_single", `SELECT (WITH x AS (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"1", "1", "1"}},
		{"cte_reuse", `SELECT (WITH x AS (SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY=(SELECT n1.N_NATIONKEY)) SELECT COUNT(*) FROM x a JOIN x b ON a.N_NATIONKEY=b.N_NATIONKEY) FROM NATION n1`, []string{"1", "1", "1"}},
		{"cte_ancestor", `WITH x AS (SELECT N_NATIONKEY FROM NATION) SELECT (SELECT (SELECT COUNT(*) FROM x WHERE x.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"1", "1", "1"}},
		{"recursive_cte", `SELECT (WITH RECURSIVE x(k) AS (SELECT (SELECT n1.N_NATIONKEY) UNION ALL SELECT k+1 FROM x WHERE k<3) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"3", "2", "1"}},
		{"recursive_distinct", `SELECT (WITH RECURSIVE x(k) AS (SELECT (SELECT n1.N_NATIONKEY) UNION SELECT k FROM x) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"1", "1", "1"}},
		{"recursive_members", `SELECT (WITH RECURSIVE x(k) AS (SELECT (SELECT n1.N_NATIONKEY) UNION ALL SELECT k+1 FROM x WHERE k<3 UNION ALL SELECT k+2 FROM x WHERE k<2) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"4", "2", "1"}},
		{"recursive_empty", `SELECT (WITH RECURSIVE x(k) AS (SELECT (SELECT n1.N_NATIONKEY) WHERE 1=0 UNION ALL SELECT k+1 FROM x WHERE k<3) SELECT COUNT(*) FROM x) FROM NATION n1`, []string{"0", "0", "0"}},
		{"unselected_scalar", `SELECT (SELECT CASE WHEN n1.N_NATIONKEY>0 THEN 7 ELSE (SELECT n1.N_NATIONKEY FROM NATION n3) END FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"7", "7", "7"}},
		{"join_null", `SELECT (SELECT COUNT(*) FROM NATION a LEFT JOIN NATION b ON (SELECT CASE WHEN n1.N_NATIONKEY=a.N_NATIONKEY THEN NULL ELSE b.N_NATIONKEY=a.N_NATIONKEY END)) FROM NATION n1`, []string{"3", "3", "3"}},
		{"derived_skip_empty", `SELECT EXISTS(SELECT 1 WHERE EXISTS(SELECT 1 FROM (SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY) d)) FROM NATION n1`, []string{"true", "true", "true"}},
		{"derived_skip_nonempty", `SELECT EXISTS(SELECT 1 FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY AND EXISTS(SELECT 1 FROM (SELECT n3.N_NATIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY=n1.N_NATIONKEY) d)) FROM NATION n1`, []string{"true", "true", "true"}},
		{"left_join_pair", `SELECT (SELECT COUNT(b.N_NATIONKEY) FROM NATION a LEFT JOIN NATION b ON b.N_NATIONKEY=(SELECT n1.N_NATIONKEY+a.N_NATIONKEY)) FROM NATION n1`, []string{"2", "1", "0"}},
		{"inner_join_pair", `SELECT (SELECT COUNT(*) FROM NATION a JOIN NATION b ON b.N_NATIONKEY=(SELECT n1.N_NATIONKEY+a.N_NATIONKEY)) FROM NATION n1`, []string{"2", "1", "0"}},
		{"full_join_tuple_lhs", `SELECT COUNT(*) FROM NATION a FULL JOIN NATION b ON (a.N_NATIONKEY,(SELECT (SELECT b.N_NATIONKEY)))=(SELECT a.N_NATIONKEY,b.N_NATIONKEY)`, []string{"9"}},
		{"left_join_tuple_lhs", `SELECT COUNT(*) FROM NATION a LEFT JOIN NATION b ON (a.N_NATIONKEY,(SELECT (SELECT b.N_NATIONKEY)))=(SELECT a.N_NATIONKEY,b.N_NATIONKEY)`, []string{"9"}},
		{"full_join_pair", `SELECT (SELECT COUNT(*) FROM NATION a FULL JOIN NATION b ON b.N_NATIONKEY=(SELECT n1.N_NATIONKEY+a.N_NATIONKEY)) FROM NATION n1`, []string{"4", "5", "6"}},
		{"row_not_in", `SELECT (n1.N_NATIONKEY,0) NOT IN (SELECT (SELECT n1.N_NATIONKEY+1),NULL FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"true", "true", "true"}},
		{"row_scalar", `SELECT (n1.N_NATIONKEY,0) = (SELECT (SELECT n1.N_NATIONKEY),NULL FROM NATION n2 WHERE n2.N_NATIONKEY=n1.N_NATIONKEY) FROM NATION n1`, []string{"NULL", "NULL", "NULL"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			mock := plan2.NewMockOptimizer(true, proc)
			stmts, err := mysql.Parse(proc.Ctx, tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
			require.NoError(t, err)
			require.NotNil(t, query.Correlation)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 8192)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<28, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 64, 65, 66, 67)
			require.NoError(t, err)
			opened, closed := 0, 0
			sourceCompiler := func(node *plan.Node) (dependent.SourceFactory, error) {
				if node.NodeType == plan.Node_VALUE_SCAN {
					return func(p *process.Process, sourceAllocation *vector.AllocationAccountSelection, account *mpool.AllocationAccount) (dependent.Source, error) {
						opened++
						return &dependentFixtureSource{batch: batch.EmptyForConstFoldBatch, rows: 1, close: func() { closed++ }}, nil
					}, nil
				}
				return func(p *process.Process, sourceAllocation *vector.AllocationAccountSelection, account *mpool.AllocationAccount) (dependent.Source, error) {
					opened++
					b := batch.NewWithSize(len(node.TableDef.Cols))
					for i, col := range node.TableDef.Cols {
						b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), sourceAllocation)
						if err != nil {
							b.Clean(p.Mp())
							return nil, err
						}
						for key := int32(1); key <= 3; key++ {
							v := key
							if col.Name == "n_regionkey" {
								v = key * 10
							}
							if err := vector.AppendFixed(b.Vecs[i], v, false, p.Mp()); err != nil {
								b.Clean(p.Mp())
								return nil, err
							}
						}
					}
					b.SetRowCount(3)
					return &dependentFixtureSource{batch: b, rows: 1, close: func() { closed++; b.Clean(p.Mp()) }}, nil
				}, nil
			}
			c := &Compile{proc: proc, stmt: stmts[0]}
			template, err := dependent.Compile(proc.Ctx, query, sourceCompiler, c.compileDependentKernel)
			require.NoError(t, err)
			foundOwner := c.selectFoundRowsOwnerNode(query)
			if foundOwner != nil {
				require.NoError(t, template.CountFoundRowsAt(foundOwner.NodeId))
			}
			for range 2 {
				func() {
					proc.BeginFoundRowsStatement(foundOwner != nil)
					execution, err := template.NewExecution(proc, allocation, account)
					require.NoError(t, err)
					defer func() { require.NoError(t, execution.Close()) }()
					var got []string
					if tc.want != nil {
						got = make([]string, 0)
					}
					cardinalityError := false
					for {
						b, err := execution.Next(proc.Ctx)
						if err != nil {
							_ = execution.Close()
							if tc.want == nil {
								require.ErrorContains(t, err, "Subquery returns more than 1 row")
								cardinalityError = true
								break
							}
						}
						require.NoError(t, err)
						if b == nil {
							break
						}
						for row := 0; row < b.RowCount(); row++ {
							v := b.Vecs[0]
							if v.IsNull(uint64(row)) {
								got = append(got, "NULL")
								continue
							}
							switch v.GetType().Oid {
							case types.T_bool:
								got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[bool](v, row)))
							case types.T_int32:
								got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[int32](v, row)))
							case types.T_int64:
								got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](v, row)))
							case types.T_decimal64:
								got = append(got, vector.GetFixedAtNoTypeCheck[types.Decimal64](v, row).Format(v.GetType().Scale))
							case types.T_decimal128:
								got = append(got, vector.GetFixedAtNoTypeCheck[types.Decimal128](v, row).Format(v.GetType().Scale))
							case types.T_decimal256:
								got = append(got, vector.GetFixedAtNoTypeCheck[types.Decimal256](v, row).Format(v.GetType().Scale))
							case types.T_uint64:
								got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint64](v, row)))
							default:
								t.Fatalf("unexpected output %s", v.GetType())
							}
						}
					}
					require.Equal(t, tc.want == nil, cardinalityError)
					require.Equal(t, tc.want, got)
					if foundOwner != nil {
						require.True(t, proc.FoundRowsRecorded())
						require.Equal(t, uint64(3), proc.GetFoundRows())
					}
					require.NoError(t, execution.Close())
					require.Equal(t, opened, closed)
					require.Zero(t, account.Snapshot().Used)
					require.Zero(t, budget.Used())
				}()
			}
		})
	}
}

// The planner fixture substitutes only the catalog source. CSV decoding,
// expression demand, aggregate execution and terminal cleanup are real.
func TestDependentExternalSourceReuse(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	mock := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx, `SELECT (SELECT (SELECT MAX(n2.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY<=n1.N_NATIONKEY)) FROM NATION n1`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, q.Correlation)
	scans := 0
	for _, n := range q.Nodes {
		if n.NodeType != plan.Node_TABLE_SCAN {
			continue
		}
		require.Len(t, n.TableDef.Cols, 1)
		n.NodeType = plan.Node_EXTERNAL_SCAN
		n.ExternScan = &plan.ExternScan{Type: int32(plan.ExternType_EXTERNAL_TB), LoadType: tree.INLINE,
			Data: "1\n2\n3\n", Format: tree.CSV, Terminated: ",", EnclosedBy: []byte{'"'}, EscapedBy: []byte{'\\'},
			TbColToDataCol: map[string]int32{n.TableDef.Cols[0].Name: 0}}
		scans++
	}
	require.Equal(t, 2, scans)
	c := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, ncpu: 1}
	defer c.clearDependentSourceMetadata()
	template, err := dependent.Compile(proc.Ctx, q, c.compileDependentSource, c.compileDependentKernel)
	require.NoError(t, err)
	require.False(t, proc.Base.LoadTag)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 8192)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<28, budget)
	require.NoError(t, err)
	defer func() {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 64, 65, 66, 67)
	require.NoError(t, err)
	for range 2 {
		func() {
			execution, err := template.NewExecution(proc, selection, account)
			require.NoError(t, err)
			defer func() { require.NoError(t, execution.Close()) }()
			var got []int32
			for {
				b, readErr := execution.Next(proc.Ctx)
				if readErr != nil {
					_ = execution.Close()
				}
				require.NoError(t, readErr)
				if b == nil {
					break
				}
				for i := 0; i < b.RowCount(); i++ {
					got = append(got, vector.GetFixedAtNoTypeCheck[int32](b.Vecs[0], i))
				}
			}
			require.Equal(t, []int32{1, 2, 3}, got)
			require.NoError(t, execution.Close())
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, budget.Used())
		}()
	}
}

func TestDependentFuzzyFilterDemand(t *testing.T) {
	for _, side := range []plan.Node_FuzzyBuildSide{plan.Node_FUZZY_BUILD_SIDE_TABLE, plan.Node_FUZZY_BUILD_SIDE_SINK} {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("side_%d_duplicate_%t", side, duplicate), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				mock := plan2.NewMockOptimizer(true, proc)
				stmts, err := mysql.Parse(proc.Ctx, `SELECT (SELECT (SELECT n.N_NATIONKEY)) FROM NATION n`, 1)
				require.NoError(t, err)
				defer stmts[0].Free()
				q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
				require.NoError(t, err)
				require.NotNil(t, q.Correlation)
				var scan *plan.Node
				for _, n := range q.Nodes {
					if n.NodeType == plan.Node_TABLE_SCAN {
						scan = n
						break
					}
				}
				require.NotNil(t, scan)
				left := plan2.DeepCopyNode(scan)
				left.NodeId = int32(len(q.Nodes))
				q.Nodes = append(q.Nodes, left)
				right := q.Nodes[q.Steps[q.MainOutputStep()]]
				def := plan2.DeepCopyTableDef(scan.TableDef, true)
				def.Pkey = &plan.PrimaryKeyDef{PkeyColName: def.Cols[0].Name, Names: []string{def.Cols[0].Name}}
				fuzzy := &plan.Node{NodeType: plan.Node_FUZZY_FILTER, NodeId: int32(len(q.Nodes)), Children: []int32{left.NodeId, right.NodeId},
					ObjRef: &plan.ObjectRef{SchemaName: "tpch"}, TableDef: def, FuzzyBuildSide: side}
				q.Nodes = append(q.Nodes, fuzzy)
				q.Steps[q.MainOutputStep()] = fuzzy.NodeId
				c := &Compile{proc: proc}
				defer func() {
					for _, f := range c.fuzzys {
						f.release()
					}
				}()
				sourceCompiler := func(node *plan.Node) (dependent.SourceFactory, error) {
					if node.NodeType == plan.Node_VALUE_SCAN {
						return c.compileDependentSource(node)
					}
					return func(p *process.Process, a *vector.AllocationAccountSelection, _ *mpool.AllocationAccount) (dependent.Source, error) {
						b := batch.NewWithSize(1)
						var err error
						b.Vecs[0], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), a)
						if err != nil {
							b.Clean(p.Mp())
							return nil, err
						}
						keys := []int32{1, 2, 3}
						if node.NodeId == left.NodeId {
							keys = []int32{9}
							if duplicate {
								keys[0] = 2
							}
						}
						for _, v := range keys {
							if err := vector.AppendFixed(b.Vecs[0], v, false, p.Mp()); err != nil {
								b.Clean(p.Mp())
								return nil, err
							}
						}
						b.SetRowCount(len(keys))
						return &dependentFixtureSource{batch: b, rows: 1, close: func() { b.Clean(p.Mp()) }}, nil
					}, nil
				}
				template, err := dependent.Compile(proc.Ctx, q, sourceCompiler, c.compileDependentKernel)
				require.NoError(t, err)
				budget, err := proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				registry, err := mpool.NewAllocationAccountRegistry(1, 8192)
				require.NoError(t, err)
				account, err := registry.OpenWithController(1<<28, budget)
				require.NoError(t, err)
				defer func() {
					account.Seal()
					_, err := registry.Finalize(account)
					require.NoError(t, err)
				}()
				allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 64, 65, 66, 67)
				require.NoError(t, err)
				for range 2 {
					func() {
						for _, f := range c.fuzzys {
							f.reset()
						}
						execution, err := template.NewExecution(proc, allocation, account)
						require.NoError(t, err)
						defer func() { require.NoError(t, execution.Close()) }()
						b, err := execution.Next(proc.Ctx)
						if duplicate {
							require.ErrorContains(t, err, "Duplicate entry")
						} else {
							require.NoError(t, err)
							require.Nil(t, b)
						}
						require.NoError(t, execution.Close())
						require.Zero(t, account.Snapshot().Used)
						require.Zero(t, budget.Used())
					}()
				}
			})
		}
	}
}

func TestDependentSourceAdmissionBeforeOpen(t *testing.T) {
	p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer p.Free()
	budget := process.MustNewExecutionResourceBudget(64, 64)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	require.NoError(t, p.SetExecutionResourceBudgetForTesting(generation))
	opened := 0
	failure := errors.New("source open failure")
	factory := dependentSourceFactory(128, func(*process.Process, *vector.AllocationAccountSelection) (vm.Operator, error) {
		opened++
		return nil, failure
	})
	source, err := factory(p, nil, nil)
	require.Error(t, err)
	require.Nil(t, source)
	require.Zero(t, opened)
	require.Zero(t, generation.Used())
	// The same factory can enter a later admitted generation. Its failed open
	// releases the source header even though no native operator was published.
	larger := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	next, err := larger.OpenGeneration(2)
	require.NoError(t, err)
	defer larger.Close()
	defer next.Close()
	later := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer later.Free()
	require.NoError(t, later.SetExecutionResourceBudgetForTesting(next))
	for range 2 {
		source, err = factory(later, nil, nil)
		require.ErrorIs(t, err, failure)
		require.Nil(t, source)
		require.Zero(t, next.Used())
	}
	require.Equal(t, 2, opened)
}
