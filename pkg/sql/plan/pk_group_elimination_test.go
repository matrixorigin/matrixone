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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func TestPrimaryKeyGroupEliminationUnlocksScanLimit(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno, count(*) from constraint_test.emp group by empno limit 10",
	)
	require.NoError(t, err)

	query := logical.GetQuery()
	require.NotNil(t, query)
	require.False(t, reachableNodeType(query, planpb.Node_AGG))

	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.NotNil(t, scan.Limit)
	require.Equal(t, uint64(10), scan.Limit.GetLit().GetU64Val())
}

func TestPrimaryKeyGroupEliminationSupportsSingleRowAggregates(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		`select empno,
		        count(comm), sum(sal), avg(sal), min(ename), max(ename),
		        any_value(job)
		   from constraint_test.emp
		  group by empno
		  limit 7`,
	)
	require.NoError(t, err)

	query := logical.GetQuery()
	require.False(t, reachableNodeType(query, planpb.Node_AGG))
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.NotNil(t, scan.Limit)
	require.Equal(t, uint64(7), scan.Limit.GetLit().GetU64Val())
}

func TestPrimaryKeyGroupEliminationRequiresExactSingleRowAggregateLaw(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantAgg bool
	}{
		{
			name:    "wide decimal avg falls back",
			sql:     "select empno, avg(cast(sal as decimal(38,10))) from constraint_test.emp group by empno limit 10",
			wantAgg: true,
		},
		{
			name:    "mixed aggregate falls back atomically",
			sql:     "select empno, count(*), avg(cast(sal as decimal(38,10))) from constraint_test.emp group by empno limit 10",
			wantAgg: true,
		},
		{
			name:    "safe decimal avg remains eligible",
			sql:     "select empno, avg(cast(sal as decimal(20,2))) from constraint_test.emp group by empno limit 10",
			wantAgg: false,
		},
		{
			name:    "float sum falls back",
			sql:     "select empno, sum(cast(sal as double)) from constraint_test.emp group by empno limit 10",
			wantAgg: true,
		},
		{
			name:    "float avg falls back",
			sql:     "select empno, avg(cast(sal as double)) from constraint_test.emp group by empno limit 10",
			wantAgg: true,
		},
		{
			name:    "float min and max remain eligible",
			sql:     "select empno, min(cast(sal as double)), max(cast(sal as double)) from constraint_test.emp group by empno limit 10",
			wantAgg: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.Equal(t, test.wantAgg, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresTruncationSafeExpressions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantAgg bool
	}{
		{
			name:    "fallible aggregate argument falls back",
			sql:     "select empno, min(cast(ename as signed)) from constraint_test.emp group by empno limit 1 offset 1",
			wantAgg: true,
		},
		{
			name:    "volatile aggregate argument falls back",
			sql:     "select empno, min(nextval('pk_group_limit_seq')) from constraint_test.emp group by empno limit 1",
			wantAgg: true,
		},
		{
			name:    "fallible extra grouping expression falls back",
			sql:     "select empno, count(*) from constraint_test.emp group by empno, cast(ename as signed) limit 1 offset 1",
			wantAgg: true,
		},
		{
			name:    "total widening cast remains eligible",
			sql:     "select empno, min(cast(sal as decimal(20, 2))) from constraint_test.emp group by empno limit 1 offset 1",
			wantAgg: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.Equal(t, test.wantAgg, reachableNodeType(query, planpb.Node_AGG))

			scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
			require.NotNil(t, scan)
			if test.wantAgg {
				require.Nil(t, scan.Limit)
				require.Nil(t, scan.Offset)
			} else {
				require.NotNil(t, scan.Limit)
				require.NotNil(t, scan.Offset)
			}
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresTruncationSafeScanPredicates(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantAgg bool
	}{
		{
			name:    "volatile predicate falls back",
			sql:     "select empno, count(*) from constraint_test.emp where nextval('pk_group_limit_seq') > 0 group by empno limit 1",
			wantAgg: true,
		},
		{
			name:    "fallible predicate falls back",
			sql:     "select empno, count(*) from constraint_test.emp where cast(ename as signed) > 0 group by empno limit 1",
			wantAgg: true,
		},
		{
			name: "total comparison remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where sal > 100 group by empno limit 1",
		},
		{
			name: "total conjunction remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where sal > 100 and empno >= 0 group by empno limit 1",
		},
		{
			name: "total disjunction remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where sal > 100 or empno >= 0 group by empno limit 1",
		},
		{
			name: "total between remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where sal between 100 and 200 group by empno limit 1",
		},
		{
			name: "total in remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where empno in (1, 2) group by empno limit 1",
		},
		{
			name: "total null test remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where comm is null group by empno limit 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.Equal(t, test.wantAgg, reachableNodeType(query, planpb.Node_AGG))

			scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
			require.NotNil(t, scan)
			require.NotEmpty(t, scan.FilterList, "the public WHERE path must reach scan evaluation")
			if test.wantAgg {
				require.Nil(t, scan.Limit)
				require.Nil(t, scan.Offset)
			} else {
				require.NotNil(t, scan.Limit)
			}
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresTotalResolvedComparisons(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		columnTypes map[string]planpb.Type
		wantAgg     bool
	}{
		{
			name: "decimal256 scale alignment can overflow",
			sql:  "select empno, count(*) from constraint_test.emp where sal < comm group by empno limit 1",
			columnTypes: map[string]planpb.Type{
				"sal":  {Id: int32(types.T_decimal256), Width: 65, Scale: 0},
				"comm": {Id: int32(types.T_decimal256), Width: 65, Scale: 12},
			},
			wantAgg: true,
		},
		{
			name: "decimal256 scale alignment proven total",
			sql:  "select empno, count(*) from constraint_test.emp where sal < comm group by empno limit 1",
			columnTypes: map[string]planpb.Type{
				"sal":  {Id: int32(types.T_decimal256), Width: 64, Scale: 0},
				"comm": {Id: int32(types.T_decimal256), Width: 65, Scale: 12},
			},
		},
		{
			name: "json boolean coercion can reject containers",
			sql:  "select empno, count(*) from constraint_test.emp where ename = true group by empno limit 1",
			columnTypes: map[string]planpb.Type{
				"ename": {Id: int32(types.T_json)},
			},
			wantAgg: true,
		},
		{
			name: "json comparison in one domain remains eligible",
			sql:  "select empno, count(*) from constraint_test.emp where ename = job group by empno limit 1",
			columnTypes: map[string]planpb.Type{
				"ename": {Id: int32(types.T_json)},
				"job":   {Id: int32(types.T_json)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			require.NotNil(t, table)
			// Keep the plan on the direct table-scan path whose predicate owner and
			// bounded-demand behavior this test is proving.
			table.Indexes = nil
			for name, typ := range test.columnTypes {
				var found bool
				for _, col := range table.Cols {
					if col.Name == name {
						col.Typ = typ
						found = true
						break
					}
				}
				require.True(t, found, "missing mock column %s", name)
			}

			logical, err := runOneStmt(optimizer, t, test.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.Equal(t, test.wantAgg, reachableNodeType(query, planpb.Node_AGG))

			scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
			require.NotNil(t, scan)
			require.NotEmpty(t, scan.FilterList)
			if test.wantAgg {
				require.Nil(t, scan.Limit)
			} else {
				require.NotNil(t, scan.Limit)
			}
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresTruncationSafeHavingPredicates(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantAgg bool
	}{
		{
			name:    "volatile having falls back",
			sql:     "select empno, count(*) from constraint_test.emp group by empno having nextval('pk_group_limit_seq') > 0 limit 1",
			wantAgg: true,
		},
		{
			name:    "fallible having falls back",
			sql:     "select empno, count(*) from constraint_test.emp group by empno having cast(max(ename) as signed) > 0 limit 1",
			wantAgg: true,
		},
		{
			name: "total having remains eligible",
			sql:  "select empno, sum(sal) from constraint_test.emp group by empno having sum(sal) > 100 limit 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.Equal(t, test.wantAgg, reachableNodeType(query, planpb.Node_AGG))
			if test.wantAgg {
				scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
				require.NotNil(t, scan)
				require.Nil(t, scan.Limit)
				require.Nil(t, scan.Offset)
			}
		})
	}
}

func TestSingleRowSumOrAvgCastIsExact(t *testing.T) {
	decimal := func(oid types.T, width, scale int32) planpb.Type {
		return planpb.Type{Id: int32(oid), Width: width, Scale: scale}
	}
	tests := []struct {
		name   string
		fn     string
		source planpb.Type
		target planpb.Type
		want   bool
	}{
		{"integer sum", "sum", planpb.Type{Id: int32(types.T_int64)}, decimal(types.T_decimal128, 38, 0), true},
		{"float sum signed zero", "sum", planpb.Type{Id: int32(types.T_float32)}, planpb.Type{Id: int32(types.T_float64)}, false},
		{"float avg signed zero", "avg", planpb.Type{Id: int32(types.T_float64)}, planpb.Type{Id: int32(types.T_float64)}, false},
		{"decimal64 widened", "avg", decimal(types.T_decimal64, 18, 0), decimal(types.T_decimal128, 38, 6), true},
		{"decimal128 loses integer digits", "avg", decimal(types.T_decimal128, 38, 10), decimal(types.T_decimal128, 38, 12), false},
		{"decimal128 exact boundary", "avg", decimal(types.T_decimal128, 37, 11), decimal(types.T_decimal128, 38, 12), true},
		{"decimal256 loses integer digits", "avg", decimal(types.T_decimal256, 65, 0), decimal(types.T_decimal256, 65, 6), false},
		{"decimal256 exact boundary", "avg", decimal(types.T_decimal256, 65, 12), decimal(types.T_decimal256, 65, 12), true},
		{"decimal sum preserves scale", "sum", decimal(types.T_decimal128, 38, 10), decimal(types.T_decimal128, 38, 10), true},
		{"decimal avg narrows scale", "avg", decimal(types.T_decimal128, 20, 8), decimal(types.T_decimal128, 38, 7), false},
		{"missing source precision", "avg", decimal(types.T_decimal128, 0, 0), decimal(types.T_decimal128, 38, 12), false},
		{"source scale exceeds precision", "avg", decimal(types.T_decimal128, 20, 21), decimal(types.T_decimal128, 38, 21), false},
		{"target scale exceeds precision", "avg", decimal(types.T_decimal128, 20, 2), decimal(types.T_decimal128, 38, 39), false},
		{"non-decimal target", "avg", decimal(types.T_decimal128, 20, 2), planpb.Type{Id: int32(types.T_float64)}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, singleRowSumOrAvgCastIsExact(test.fn, test.source, test.target))
		})
	}
}

func TestSingleRowCastIsTotal(t *testing.T) {
	typ := func(oid types.T) planpb.Type {
		return planpb.Type{Id: int32(oid)}
	}
	decimal := func(oid types.T, width, scale int32) planpb.Type {
		return planpb.Type{Id: int32(oid), Width: width, Scale: scale}
	}

	tests := []struct {
		name   string
		source planpb.Type
		target planpb.Type
		want   bool
	}{
		{"same type", typ(types.T_varchar), typ(types.T_varchar), true},
		{"decimal widening", decimal(types.T_decimal64, 7, 2), decimal(types.T_decimal128, 20, 2), true},
		{"decimal integral narrowing", decimal(types.T_decimal128, 20, 2), decimal(types.T_decimal64, 7, 2), false},
		{"decimal scale narrowing", decimal(types.T_decimal64, 7, 2), decimal(types.T_decimal64, 7, 1), false},
		{"decimal to float64", decimal(types.T_decimal256, 76, 10), typ(types.T_float64), true},
		{"malformed decimal source", decimal(types.T_decimal64, 19, 2), typ(types.T_float64), false},
		{"malformed decimal target", decimal(types.T_decimal64, 7, 2), decimal(types.T_decimal64, 19, 2), false},
		{"same malformed decimal", decimal(types.T_decimal64, 19, 2), decimal(types.T_decimal64, 19, 2), false},
		{"text to integer", typ(types.T_varchar), typ(types.T_int64), false},
		{"float widening", typ(types.T_float32), typ(types.T_float64), true},
		{"float narrowing", typ(types.T_float64), typ(types.T_float32), false},
		{"signed widening", typ(types.T_int32), typ(types.T_int64), true},
		{"unsigned into wider signed", typ(types.T_uint32), typ(types.T_int64), true},
		{"unsigned into same width signed", typ(types.T_uint64), typ(types.T_int64), false},
		{"signed to unsigned", typ(types.T_int64), typ(types.T_uint64), false},
		{"signed narrowing", typ(types.T_int64), typ(types.T_int32), false},
		{"integer to float", typ(types.T_uint64), typ(types.T_float32), true},
		{"integer fits decimal", typ(types.T_uint64), decimal(types.T_decimal128, 22, 2), true},
		{"integer exceeds decimal", typ(types.T_uint64), decimal(types.T_decimal128, 21, 2), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, singleRowCastIsTotal(test.source, test.target))
		})
	}
}

func TestPredicateOperatorEvaluationTotality(t *testing.T) {
	typ := func(oid types.T) planpb.Type {
		return planpb.Type{Id: int32(oid)}
	}
	decimal := func(width, scale int32) planpb.Type {
		return planpb.Type{Id: int32(types.T_decimal256), Width: width, Scale: scale}
	}

	tests := []struct {
		name       string
		functionID int32
		left       planpb.Type
		right      planpb.Type
		want       bool
	}{
		{"integer equality", function.EQUAL, typ(types.T_int64), typ(types.T_int64), true},
		{"mixed datetime timestamp", function.LESS_THAN, typ(types.T_datetime), typ(types.T_timestamp), true},
		{"json equality", function.EQUAL, typ(types.T_json), typ(types.T_json), true},
		{"json boolean equality", function.EQUAL, typ(types.T_json), typ(types.T_bool), false},
		{"decimal256 equal scale", function.GREAT_THAN, decimal(76, 12), decimal(76, 12), true},
		{"decimal256 safe scale boundary", function.LESS_THAN, decimal(64, 0), decimal(65, 12), true},
		{"decimal256 scale overflow", function.LESS_THAN, decimal(65, 0), decimal(65, 12), false},
		{"decimal256 reverse scale overflow", function.LESS_THAN, decimal(65, 12), decimal(65, 0), false},
		{"malformed decimal metadata", function.EQUAL, decimal(77, 0), decimal(77, 0), false},
		{"enum equality has executor", function.EQUAL, typ(types.T_enum), typ(types.T_enum), true},
		{"enum inequality lacks executor", function.NOT_EQUAL, typ(types.T_enum), typ(types.T_enum), false},
		{"bit inequality executor mismatch", function.NOT_EQUAL, typ(types.T_bit), typ(types.T_bit), false},
		{"geometry ordering unsupported", function.GREAT_THAN, typ(types.T_geometry), typ(types.T_geometry), false},
		{"unknown comparison function", function.ABS, typ(types.T_int64), typ(types.T_int64), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want,
				comparisonEvaluationIsTotal(test.functionID, test.left, test.right))
		})
	}

	value := func(valueType planpb.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  valueType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}},
		}
	}
	require.True(t, isTruncationSafeBetween([]*planpb.Expr{
		value(typ(types.T_int64)), value(typ(types.T_int64)), value(typ(types.T_int64)),
	}))
	require.False(t, isTruncationSafeBetween([]*planpb.Expr{
		value(typ(types.T_json)), value(typ(types.T_json)), value(typ(types.T_json)),
	}))
	require.False(t, isTruncationSafeBetween([]*planpb.Expr{
		value(typ(types.T_year)), value(typ(types.T_year)), value(typ(types.T_year)),
	}))
	inArgs := func(valueType planpb.Type) []*planpb.Expr {
		return []*planpb.Expr{
			value(valueType),
			{
				Typ: planpb.Type{Id: int32(types.T_tuple)},
				Expr: &planpb.Expr_List{List: &planpb.ExprList{
					List: []*planpb.Expr{value(valueType)},
				}},
			},
		}
	}
	require.True(t, isTruncationSafeIn(inArgs(typ(types.T_int64))))
	require.True(t, isTruncationSafeIn(inArgs(decimal(65, 12))))
	require.False(t, isTruncationSafeIn(inArgs(decimal(77, 12))))
	require.False(t, isTruncationSafeIn(inArgs(typ(types.T_json))))
	require.False(t, isTruncationSafeIn(inArgs(typ(types.T_array_bf16))))
}

func TestPrimaryKeyGroupEliminationPreservesBoundedDemand(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantLimit  uint64
		wantOffset uint64
	}{
		{
			name:       "offset reaches scan",
			sql:        "select empno, count(*) from constraint_test.emp group by empno limit 10 offset 25",
			wantLimit:  10,
			wantOffset: 25,
		},
		{
			name:      "scan filter precedes bounded demand",
			sql:       "select empno, count(*) from constraint_test.emp where sal > 100 group by empno limit 6",
			wantLimit: 6,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.False(t, reachableNodeType(query, planpb.Node_AGG))
			scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
			require.NotNil(t, scan)
			require.NotNil(t, scan.Limit)
			require.Equal(t, test.wantLimit, scan.Limit.GetLit().GetU64Val())
			if test.wantOffset == 0 {
				require.Nil(t, scan.Offset)
			} else {
				require.NotNil(t, scan.Offset)
				require.Equal(t, test.wantOffset, scan.Offset.GetLit().GetU64Val())
			}
		})
	}
}

func TestPrimaryKeyGroupEliminationWithOrderRemovesHashButNotScan(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno, count(*) c from constraint_test.emp group by empno order by c desc limit 10",
	)
	require.NoError(t, err)

	query := logical.GetQuery()
	require.False(t, reachableNodeType(query, planpb.Node_AGG))
	require.True(t, reachableNodeType(query, planpb.Node_SORT))
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Nil(t, scan.Limit, "Sort remains a semantic barrier to scan LIMIT")
}

func TestPrimaryKeyGroupEliminationDoesNotCrossJoin(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		`select e.empno, count(*)
		   from constraint_test.emp e
		   join constraint_test.dept d on e.deptno = d.deptno
		  group by e.empno
		  limit 10`,
	)
	require.NoError(t, err)
	require.True(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
}

func TestPrimaryKeyGroupEliminationRequiresCompleteCompositeKey(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	partsupp := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("tpch", "partsupp")]
	require.NotNil(t, partsupp)
	require.NotNil(t, partsupp.Pkey)
	// The shared legacy mock has malformed composite-name padding. Repair only
	// this optimizer instance so the rule is exercised against the same
	// metadata shape emitted by a real catalog.
	partsupp.Pkey.Names = []string{"ps_partkey", "ps_suppkey"}

	logical, err := runOneStmt(
		optimizer,
		t,
		`select ps_partkey, ps_suppkey, count(*)
		   from tpch.partsupp
		  group by ps_partkey, ps_suppkey
		  limit 10`,
	)
	require.NoError(t, err)
	require.False(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))

	logical, err = runOneStmt(
		optimizer,
		t,
		`select ps_partkey, count(*)
		   from tpch.partsupp
		  group by ps_partkey
		  limit 10`,
	)
	require.NoError(t, err)
	require.True(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
}

func TestPrimaryKeyGroupEliminationPreservesInactiveGroupingSetsWithoutAggregates(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "rollup",
			sql:  "select empno from constraint_test.emp group by rollup(empno) limit 10",
		},
		{
			name: "cube",
			sql:  "select empno from constraint_test.emp group by cube(empno) limit 10",
		},
		{
			name: "grouping sets",
			sql:  "select empno from constraint_test.emp group by grouping sets ((empno), ()) limit 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			agg := firstReachableNode(logical.GetQuery(), planpb.Node_AGG)
			require.NotNil(t, agg)
			require.True(t, hasInactiveGroupingColumn(agg.GroupingFlag))
		})
	}

	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno from constraint_test.emp group by empno limit 10",
	)
	require.NoError(t, err)
	require.False(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
}

func TestPrimaryKeyGroupEliminationFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "missing primary key",
			sql:  "select deptno, count(*) from constraint_test.emp group by deptno limit 10",
		},
		{
			name: "distinct aggregate",
			sql:  "select empno, count(distinct deptno) from constraint_test.emp group by empno limit 10",
		},
		{
			name: "unsupported aggregate",
			sql:  "select empno, group_concat(ename) from constraint_test.emp group by empno limit 10",
		},
		{
			name: "unbounded aggregate keeps established plan",
			sql:  "select empno, count(*) from constraint_test.emp group by empno",
		},
		{
			name: "grouping family stays atomic",
			sql:  "select empno, count(*) from constraint_test.emp group by cube(empno) limit 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.True(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
		})
	}
}

func TestPrimaryKeyGroupEliminationPreservesHavingSemantics(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno, sum(sal) from constraint_test.emp group by empno having sum(sal) > 100 limit 10",
	)
	require.NoError(t, err)
	require.False(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG))
}

func reachableNodeType(query *planpb.Query, typ planpb.Node_NodeType) bool {
	return firstReachableNode(query, typ) != nil
}

func firstReachableNode(query *planpb.Query, typ planpb.Node_NodeType) *planpb.Node {
	if query == nil {
		return nil
	}
	seen := make(map[int32]struct{})
	var visit func(int32) *planpb.Node
	visit = func(nodeID int32) *planpb.Node {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return nil
		}
		if _, ok := seen[nodeID]; ok {
			return nil
		}
		seen[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node.NodeType == typ {
			return node
		}
		for _, child := range node.Children {
			if found := visit(child); found != nil {
				return found
			}
		}
		return nil
	}
	for _, root := range query.Steps {
		if found := visit(root); found != nil {
			return found
		}
	}
	return nil
}
