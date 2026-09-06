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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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

func TestPrimaryKeyGroupEliminationRemapsAggregateReferencesInExpressionLists(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "select list",
			sql:  "select empno, count(*) in (count(*), sum(empno)) from constraint_test.emp group by empno limit 1",
		},
		{
			name: "order by",
			sql:  "select empno from constraint_test.emp group by empno order by count(*) in (count(*), sum(empno)) limit 1",
		},
		{
			name: "having",
			sql:  "select empno from constraint_test.emp group by empno having count(*) in (count(*), sum(empno)) limit 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)

			query := logical.GetQuery()
			require.NotNil(t, query)
			require.False(t, reachableNodeType(query, planpb.Node_AGG))
			requireNoDanglingColumnTags(t, query)
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresExactSingleRowAggregateLaw(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantAgg bool
	}{
		{
			name:    "wide decimal avg remains eligible after promotion",
			sql:     "select empno, avg(cast(sal as decimal(38,10))) from constraint_test.emp group by empno limit 10",
			wantAgg: false,
		},
		{
			name:    "mixed aggregate remains eligible after promotion",
			sql:     "select empno, count(*), avg(cast(sal as decimal(38,10))) from constraint_test.emp group by empno limit 10",
			wantAgg: false,
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
		{"decimal128 promotes without losing integer digits", "avg", decimal(types.T_decimal128, 38, 10), decimal(types.T_decimal256, 42, 14), true},
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
	stringType := func(oid types.T, width int32, charset uint32) planpb.Type {
		return planpb.Type{Id: int32(oid), Width: width, Charset: charset}
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
		{"varchar to wider char", stringType(types.T_varchar, 17, uint32(types.CharsetUTF8)), stringType(types.T_char, 25, uint32(types.CharsetUTF8)), true},
		{"char widening", stringType(types.T_char, 10, uint32(types.CharsetUTF8)), stringType(types.T_char, 25, uint32(types.CharsetUTF8)), true},
		{"varchar to narrower char", stringType(types.T_varchar, 25, uint32(types.CharsetUTF8)), stringType(types.T_char, 10, uint32(types.CharsetUTF8)), false},
		{"unbounded varchar to char", stringType(types.T_varchar, 0, uint32(types.CharsetUTF8)), stringType(types.T_char, 25, uint32(types.CharsetUTF8)), false},
		{"varchar to char charset change", stringType(types.T_varchar, 17, uint32(types.CharsetUTF8MB4Bin)), stringType(types.T_char, 25, uint32(types.CharsetUTF8)), false},
		{"text to char", stringType(types.T_text, 17, uint32(types.CharsetUTF8)), stringType(types.T_char, 25, uint32(types.CharsetUTF8)), false},
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

func TestSingleRowAggregateExprRequiresWellTypedReplacement(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	builder := &QueryBuilder{compCtx: optimizer.CurrentContext()}
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	int32Type := planpb.Type{Id: int32(types.T_int32)}
	varcharType := planpb.Type{Id: int32(types.T_varchar), Width: 8}
	functionExpr := func(name string, resultType planpb.Type, args ...*planpb.Expr) *planpb.Expr {
		functionID, ok := singleRowAggregateFunctionID(name)
		require.True(t, ok)
		return &planpb.Expr{
			Typ: resultType,
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					ObjName: name,
					Obj:     function.EncodeOverloadID(functionID, 0),
				},
				Args: args,
			}},
		}
	}

	starCountArg := &planpb.Expr{
		Typ:  intType,
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}},
	}
	_, ok := builder.singleRowAggregateExpr(functionExpr("starcount", intType, starCountArg))
	require.True(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr("starcount", intType))
	require.False(t, ok)
	nullableStarCountArg := DeepCopyExpr(starCountArg)
	nullableStarCountArg.Typ.NotNullable = false
	_, ok = builder.singleRowAggregateExpr(functionExpr("starcount", intType, nullableStarCountArg))
	require.False(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"starcount", planpb.Type{Id: int32(types.T_decimal128), Width: 38}, starCountArg))
	require.False(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"count", planpb.Type{Id: int32(types.T_decimal128), Width: 38},
		GetColExpr(varcharType, 1, 0)))
	require.False(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"count", planpb.Type{Id: int32(types.T_int64)},
		GetColExpr(varcharType, 1, 0)))
	require.False(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"min", intType, GetColExpr(varcharType, 1, 0)))
	require.False(t, ok)
	mismatchedID := functionExpr("starcount", intType, starCountArg)
	mismatchedID.GetF().Func.Obj = function.EncodeOverloadID(int32(function.COUNT), 0)
	_, ok = builder.singleRowAggregateExpr(mismatchedID)
	require.False(t, ok)
	invalidOverload := functionExpr("min", int32Type, GetColExpr(int32Type, 1, 0))
	invalidOverload.GetF().Func.Obj = function.EncodeOverloadID(int32(function.MIN), 1)
	_, ok = builder.singleRowAggregateExpr(invalidOverload)
	require.False(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"min", int32Type, GetColExpr(int32Type, 1, 0)))
	require.True(t, ok)
	_, ok = builder.singleRowAggregateExpr(functionExpr(
		"min", intType, GetColExpr(int32Type, 1, 0)))
	require.False(t, ok,
		"a total widening cast cannot legitimize a result type rejected by the registered aggregate")
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
	require.True(t, isTruncationSafeBetween([]*planpb.Expr{
		value(decimal(64, 0)), value(decimal(65, 12)), value(decimal(65, 12)),
	}))
	require.False(t, isTruncationSafeBetween([]*planpb.Expr{
		value(decimal(65, 0)), value(decimal(65, 12)), value(decimal(65, 12)),
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
	mismatchedInArgs := inArgs(typ(types.T_int64))
	mismatchedInArgs[1].GetList().List[0].Typ = typ(types.T_uint64)
	require.False(t, isTruncationSafeIn(mismatchedInArgs))
	require.False(t, isTruncationSafeIn([]*planpb.Expr{
		value(typ(types.T_int64)),
		{
			Typ: typ(types.T_int64),
			Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
				Len:  1,
				Data: []byte("invalid literal vector"),
			}},
		},
	}))
	require.True(t, isTruncationSafeIn(inArgs(decimal(65, 12))))
	require.False(t, isTruncationSafeIn(inArgs(decimal(77, 12))))
	require.False(t, isTruncationSafeIn(inArgs(typ(types.T_json))))
	require.False(t, isTruncationSafeIn(inArgs(typ(types.T_array_bf16))))
	require.True(t, isTruncationSafePredicateValue(&planpb.Expr{
		Typ:  typ(types.T_int64),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}))
	require.False(t, isTruncationSafePredicateValue(&planpb.Expr{
		Typ:  typ(types.T_int64),
		Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "threshold"}},
	}))
	require.False(t, isTruncationSafePredicateValue(inArgs(typ(types.T_int64))[1]),
		"a tuple constant is safe only through the IN/NOT IN contract")
	require.False(t, isTruncationSafePredicateExpr(value(typ(types.T_int64))),
		"a non-boolean Filter expression must fail closed")
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
		{
			name:      "zero limit remains explicit bounded demand",
			sql:       "select empno, count(*) from constraint_test.emp group by empno limit 0",
			wantLimit: 0,
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

func TestPrimaryKeyGroupEliminationSupportsPreparedBoundedDemand(t *testing.T) {
	prepared, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"prepare pk_group_page from 'select empno, count(*) from constraint_test.emp group by empno limit ? offset ?'",
	)
	require.NoError(t, err)
	query := prepared.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)
	require.False(t, reachableNodeType(query, planpb.Node_AGG))

	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.NotNil(t, scan.Limit)
	require.Equal(t, "cast", scan.Limit.GetF().Func.ObjName)
	require.Equal(t, int32(0), scan.Limit.GetF().Args[0].GetP().Pos)
	require.NotNil(t, scan.Offset)
	require.Equal(t, "cast", scan.Offset.GetF().Func.ObjName)
	require.Equal(t, int32(1), scan.Offset.GetF().Args[0].GetP().Pos)

	prepared, err = runOneStmt(
		NewMockOptimizer(false),
		t,
		"prepare pk_group_filter from 'select empno, count(*) from constraint_test.emp where empno in (?, ?) group by empno limit ?'",
	)
	require.NoError(t, err)
	query = prepared.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)
	require.True(t, reachableNodeType(query, planpb.Node_AGG),
		"runtime casts around untyped predicate parameters remain fallible")
	scan = firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Len(t, scan.FilterList, 1)
	require.Nil(t, scan.Limit,
		"bounded demand must not suppress a predicate-parameter conversion error")
}

func TestPrimaryKeyGroupEliminationKeepsAggregateFreeLegacyPathUnbounded(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno from constraint_test.emp group by empno",
	)
	require.NoError(t, err)
	require.False(t, reachableNodeType(logical.GetQuery(), planpb.Node_AGG),
		"the bounded-demand gate applies only to aggregate-bearing rewrites")
}

func TestPrimaryKeyGroupEliminationRemovesProvenConstantOrder(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantLimit  uint64
		wantOffset uint64
	}{
		{
			name:      "aggregate alias",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by c desc limit 10",
			wantLimit: 10,
		},
		{
			name:      "foldable expression",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by c + 1 limit 7",
			wantLimit: 7,
		},
		{
			name:      "multiple constant keys",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by c desc, c + 1 asc, (1 + 1) limit 6",
			wantLimit: 6,
		},
		{
			name:      "deterministic cast",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by cast(c as signed) limit 3",
			wantLimit: 3,
		},
		{
			name:      "interval consumed by temporal function",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by date_add('2026-01-01', interval c day) limit 3",
			wantLimit: 3,
		},
		{
			name:      "null companion key",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by c, null limit 2",
			wantLimit: 2,
		},
		{
			name:       "offset",
			sql:        "select empno, count(*) c from constraint_test.emp group by empno order by c limit 5 offset 3",
			wantLimit:  5,
			wantOffset: 3,
		},
		{
			name:      "always true having",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno having count(*) = 1 order by c limit 4",
			wantLimit: 4,
		},
		{
			name:      "zero limit",
			sql:       "select empno, count(*) c from constraint_test.emp group by empno order by c limit 0",
			wantLimit: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)

			query := logical.GetQuery()
			require.False(t, reachableNodeType(query, planpb.Node_AGG))
			require.False(t, reachableNodeType(query, planpb.Node_SORT))
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

func TestPrimaryKeyGroupEliminationKeepsNonConstantOrObservableOrder(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "mixed row key",
			sql:  "select empno, count(*) c from constraint_test.emp group by empno order by c, empno limit 10",
		},
		{
			name: "nullable count",
			sql:  "select empno, count(comm) c from constraint_test.emp group by empno order by c limit 10",
		},
		{
			name: "row value aggregate",
			sql:  "select empno, min(empno) c from constraint_test.emp group by empno order by c limit 10",
		},
		{
			name: "varying sum",
			sql:  "select empno, sum(sal) c from constraint_test.emp group by empno order by c limit 10",
		},
		{
			name: "volatile expression",
			sql:  "select empno, count(*) c from constraint_test.emp group by empno order by c + rand() limit 10",
		},
		{
			name: "division by zero",
			sql:  "select empno, count(*) c from constraint_test.emp group by empno order by c / 0 limit 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)

			query := logical.GetQuery()
			require.True(t, reachableNodeType(query, planpb.Node_SORT))
			scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
			require.NotNil(t, scan)
			require.Nil(t, scan.Limit, "a retained Sort must keep its full input stream")
			require.Nil(t, scan.Offset)
		})
	}
}

func TestPrimaryKeyGroupEliminationKeepsPreparedOrderParameter(t *testing.T) {
	prepared, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"prepare pk_group_order from 'select empno, count(*) c from constraint_test.emp group by empno order by c + ? limit 10'",
	)
	require.NoError(t, err)

	query := prepared.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)
	require.True(t, reachableNodeType(query, planpb.Node_SORT))
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Nil(t, scan.Limit)
}

func TestPrimaryKeyGroupEliminationSupportsPreparedConstantOrderPagination(t *testing.T) {
	prepared, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"prepare pk_group_order_page from 'select empno, count(*) c from constraint_test.emp group by empno order by c limit ? offset ?'",
	)
	require.NoError(t, err)

	query := prepared.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)
	require.False(t, reachableNodeType(query, planpb.Node_AGG))
	require.False(t, reachableNodeType(query, planpb.Node_SORT))
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Equal(t, "cast", scan.Limit.GetF().Func.ObjName)
	require.Equal(t, int32(0), scan.Limit.GetF().Args[0].GetP().Pos)
	require.Equal(t, "cast", scan.Offset.GetF().Func.ObjName)
	require.Equal(t, int32(1), scan.Offset.GetF().Args[0].GetP().Pos)
}

func TestPrimaryKeyGroupEliminationRejectsStandaloneIntervalOrder(t *testing.T) {
	tests := []string{
		"select empno, count(*) c from constraint_test.emp group by empno order by interval c day limit 10",
		"select empno, count(*) c from constraint_test.emp group by empno order by interval (c + 1) day limit 10",
		"select empno, count(*) c from constraint_test.emp group by empno order by c, interval 1 day limit 10",
		"select empno from constraint_test.emp order by interval 1 day",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = NewBaseOptimizer(ctx).Optimize(stmt, false)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
			require.ErrorContains(t, err, "standalone INTERVAL expression in ORDER BY")
		})
	}
}

func TestConstantSingletonGroupSortRemovalPreservesRootAndBarriers(t *testing.T) {
	newBuilder := func() (*QueryBuilder, map[int32]struct{}) {
		int64Type := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
		valueScan := &planpb.Node{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN}
		singletonProject := &planpb.Node{
			NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0},
			BindingTags: []int32{11},
			ProjectList: []*planpb.Expr{makePlan2Int64ConstExprWithType(1)},
		}
		selectProject := &planpb.Node{
			NodeId: 2, NodeType: planpb.Node_PROJECT, Children: []int32{1},
			BindingTags: []int32{12},
			ProjectList: []*planpb.Expr{GetColExpr(int64Type, 11, 0)},
		}
		sort := &planpb.Node{
			NodeId: 3, NodeType: planpb.Node_SORT, Children: []int32{2},
			OrderBy: []*planpb.OrderBySpec{{Expr: GetColExpr(int64Type, 12, 0)}},
			Limit:   makePlan2Uint64ConstExprWithType(10),
		}
		return &QueryBuilder{qry: &planpb.Query{
			Nodes: []*planpb.Node{valueScan, singletonProject, selectProject, sort},
		}}, map[int32]struct{}{1: {}}
	}

	t.Run("sort root is replaced", func(t *testing.T) {
		builder, rewritten := newBuilder()
		rootID := builder.removeConstantSortAfterSingletonGroup(3, rewritten)
		require.Equal(t, int32(2), rootID)
		require.NotNil(t, builder.qry.Nodes[2].Limit)
		require.Equal(t, uint64(10), builder.qry.Nodes[2].Limit.GetLit().GetU64Val())
	})

	t.Run("rank option retains sort", func(t *testing.T) {
		builder, rewritten := newBuilder()
		builder.qry.Nodes[3].RankOption = &planpb.RankOption{Mode: "force"}
		require.Equal(t, int32(3), builder.removeConstantSortAfterSingletonGroup(3, rewritten))
	})

	t.Run("unrelated constant order retains sort", func(t *testing.T) {
		builder, rewritten := newBuilder()
		builder.qry.Nodes[3].OrderBy[0].Expr = makePlan2Int64ConstExprWithType(1)
		require.Equal(t, int32(3), builder.removeConstantSortAfterSingletonGroup(3, rewritten),
			"the singleton-group pass must not become a global constant-order rewrite")
		require.Nil(t, builder.qry.Nodes[2].Limit)
	})

	t.Run("unsupported constant type retains sort without panic", func(t *testing.T) {
		builder, rewritten := newBuilder()
		builder.compCtx = NewMockCompilerContext(false)
		builder.qry.Nodes[3].OrderBy[0].Expr = &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_interval)},
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
				GetColExpr(planpb.Type{Id: int32(types.T_int64), NotNullable: true}, 12, 0),
				makePlan2StringConstExprWithType("day"),
			}}},
		}
		require.Equal(t, int32(3), builder.removeConstantSortAfterSingletonGroup(3, rewritten))
		require.Nil(t, builder.qry.Nodes[2].Limit)
	})

	t.Run("unsafe pagination composition retains sort", func(t *testing.T) {
		builder, rewritten := newBuilder()
		builder.qry.Nodes[2].Limit = makePlan2Uint64ConstExprWithType(1)
		builder.qry.Nodes[3].Offset = makePlan2Uint64ConstExprWithType(1)
		require.Equal(t, int32(3), builder.removeConstantSortAfterSingletonGroup(3, rewritten))
		require.Equal(t, uint64(1), builder.qry.Nodes[2].Limit.GetLit().GetU64Val())
	})
}

func TestPrimaryKeyGroupEliminationPreservesSQLCalcFoundRowsStream(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select sql_calc_found_rows empno, count(*) c from constraint_test.emp group by empno order by c limit 10",
	)
	require.NoError(t, err)

	query := logical.GetQuery()
	require.False(t, reachableNodeType(query, planpb.Node_AGG),
		"a complete primary key still proves one row per SQL group")
	require.True(t, reachableNodeType(query, planpb.Node_SORT),
		"FOUND_ROWS() retains the complete ordered input owner")
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Nil(t, scan.Limit,
		"FOUND_ROWS() must observe the complete pre-LIMIT group stream")
	require.Nil(t, scan.Offset)
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

func TestPrimaryKeyGroupEliminationDoesNotCrossSetOperation(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		`select * from (
			select empno, count(*) c from constraint_test.emp group by empno
			union all
			select empno, count(*) c from constraint_test.emp group by empno
		) u limit 1`,
	)
	require.NoError(t, err)
	require.Equal(t, 2, reachableNodeTypeCount(logical.GetQuery(), planpb.Node_AGG),
		"outer bounded demand must not enter either UNION branch")
}

func TestPrimaryKeyGroupEliminationDoesNotCrossSemanticFilters(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*planpb.Node)
	}{
		{name: "terminal filter", configure: func(node *planpb.Node) { node.IsEnd = true }},
		{name: "barrier filter", configure: func(node *planpb.Node) { node.FilterIsBarrier = true }},
		{name: "rollup filter", configure: func(node *planpb.Node) { node.RollupFilter = true }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := groupHashKeyTestTable("id", "id")
			table.Cols[0].Typ.NotNullable = true
			scan := &planpb.Node{
				NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
				BindingTags: []int32{1}, TableDef: table,
			}
			agg := &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0},
				BindingTags: []int32{2, 3},
				GroupBy:     []*planpb.Expr{GetColExpr(table.Cols[0].Typ, 1, 0)},
				AggList: []*planpb.Expr{{
					Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &planpb.Expr_F{F: &planpb.Function{
						Func: &planpb.ObjectRef{ObjName: "starcount"},
					}},
				}},
			}
			filter := &planpb.Node{
				NodeId: 2, NodeType: planpb.Node_FILTER, Children: []int32{1},
			}
			test.configure(filter)
			root := &planpb.Node{
				NodeId: 3, NodeType: planpb.Node_PROJECT, Children: []int32{2},
				Limit: makePlan2Uint64ConstExprWithType(1),
			}
			builder := &QueryBuilder{qry: &planpb.Query{
				Nodes: []*planpb.Node{scan, agg, filter, root},
			}}

			builder.rewriteEffectlessAggToProject(3)

			require.Equal(t, planpb.Node_AGG, agg.NodeType)
		})
	}
}

func TestPrimaryKeyGroupEliminationRequiresCompleteCompositeKey(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	partsupp := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("tpch", "partsupp")]
	require.NotNil(t, partsupp)
	require.NotNil(t, partsupp.Pkey)
	// The shared mock has malformed component padding and uses the composite
	// cluster-by prefix for a primary key. Repair only this optimizer instance
	// to the encoded composite-PK shape retained by upgraded catalogs.
	partsupp.Pkey.Names = []string{"ps_partkey", "ps_suppkey"}
	partsupp.Pkey.PkeyColName = catalog.PrefixPriColName + "010ps_partkey010ps_suppkey"
	partsupp.Pkey.CompPkeyCol = MakeHiddenColDefByName(partsupp.Pkey.PkeyColName)

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

func TestPrimaryKeyGroupEliminationRequiresGroupingCompatiblePrimaryKey(t *testing.T) {
	primaryKeyTypes := []struct {
		name string
		typ  planpb.Type
	}{
		{name: "float64 signed zero", typ: planpb.Type{Id: int32(types.T_float64)}},
		{name: "scaled float32", typ: planpb.Type{Id: int32(types.T_float32), Width: 8, Scale: 2}},
		{name: "char trailing spaces", typ: planpb.Type{Id: int32(types.T_char), Width: 8}},
		{name: "collated varchar", typ: planpb.Type{
			Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8),
		}},
	}
	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "aggregate-bearing rewrite",
			sql:  "select empno, count(*) from constraint_test.emp group by empno limit 1",
		},
		{
			name: "aggregate-free rewrite",
			sql:  "select empno from constraint_test.emp group by empno limit 1",
		},
	}

	for _, primaryKeyType := range primaryKeyTypes {
		for _, query := range queries {
			t.Run(primaryKeyType.name+"/"+query.name, func(t *testing.T) {
				optimizer := NewMockOptimizer(false)
				table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
				require.NotNil(t, table)
				require.NotNil(t, table.Pkey)

				var primaryKeyColumn *planpb.ColDef
				for _, col := range table.Cols {
					if col.Name == "empno" {
						primaryKeyColumn = col
						break
					}
				}
				require.NotNil(t, primaryKeyColumn)
				primaryKeyColumn.Typ = primaryKeyType.typ
				table.Pkey.CompPkeyCol = primaryKeyColumn

				logical, err := runOneStmt(optimizer, t, query.sql)
				require.NoError(t, err)
				queryPlan := logical.GetQuery()
				require.True(t, reachableNodeType(queryPlan, planpb.Node_AGG),
					"storage-distinct primary keys may still belong to one SQL group")

				scan := firstReachableNode(queryPlan, planpb.Node_TABLE_SCAN)
				require.NotNil(t, scan)
				require.Nil(t, scan.Limit,
					"bounded demand must not cross the retained aggregate")
			})
		}
	}
}

func TestPrimaryKeyGroupEliminationRequiresGroupTypeToMatchPrimaryKeyColumn(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	tests := []struct {
		name          string
		groupType     planpb.Type
		bindingTags   []int32
		groupingFlags []bool
		wantRewrite   bool
	}{
		{
			name: "matching expression and schema types", groupType: intType,
			bindingTags: []int32{2}, wantRewrite: true,
		},
		{
			name:        "detached group expression type",
			groupType:   planpb.Type{Id: int32(types.T_uint64), NotNullable: true},
			bindingTags: []int32{2},
		},
		{
			name: "malformed grouping flag vector", groupType: intType,
			bindingTags: []int32{2}, groupingFlags: []bool{true, true},
		},
		{
			name: "group output tag aliases scan input", groupType: intType,
			bindingTags: []int32{1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := groupHashKeyTestTable("id", "id")
			table.Cols[0].Typ = intType
			scan := &planpb.Node{
				NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
				BindingTags: []int32{1}, TableDef: table,
			}
			agg := &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0},
				BindingTags:  test.bindingTags,
				GroupBy:      []*planpb.Expr{GetColExpr(test.groupType, 1, 0)},
				GroupingFlag: test.groupingFlags,
			}
			root := &planpb.Node{
				NodeId: 2, NodeType: planpb.Node_PROJECT, Children: []int32{1},
				Limit: makePlan2Uint64ConstExprWithType(1),
			}
			builder := &QueryBuilder{qry: &planpb.Query{
				Nodes: []*planpb.Node{scan, agg, root},
			}}

			builder.rewriteEffectlessAggToProject(2)

			require.Equal(t, test.wantRewrite, agg.NodeType == planpb.Node_PROJECT)
		})
	}
}

func TestOnlyFullGroupByRequiresGroupingCompatiblePrimaryKey(t *testing.T) {
	tests := []struct {
		name    string
		typ     planpb.Type
		wantErr bool
	}{
		{
			name: "varchar control",
			typ:  planpb.Type{Id: int32(types.T_varchar), Width: 8},
		},
		{
			name:    "float64 signed zero",
			typ:     planpb.Type{Id: int32(types.T_float64)},
			wantErr: true,
		},
		{
			name:    "char trailing spaces",
			typ:     planpb.Type{Id: int32(types.T_char), Width: 8},
			wantErr: true,
		},
		{
			name: "collated varchar",
			typ: planpb.Type{
				Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8),
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			optimizer.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			require.NotNil(t, table)
			require.NotNil(t, table.Pkey)

			var primaryKeyColumn *planpb.ColDef
			for _, col := range table.Cols {
				if col.Name == "empno" {
					primaryKeyColumn = col
					break
				}
			}
			require.NotNil(t, primaryKeyColumn)
			primaryKeyColumn.Typ = test.typ
			table.Pkey.CompPkeyCol = primaryKeyColumn

			_, err := runOneStmt(
				optimizer,
				t,
				"select empno, ename, sum(sal) from constraint_test.emp group by empno",
			)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
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
			optimizer := NewMockOptimizer(false)
			useLegacyGroupingSetPlan(t, optimizer)
			logical, err := runOneStmt(optimizer, t, test.sql)
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

func TestPrimaryKeyGroupEliminationPreservesCombinedWhereAndHavingSemantics(t *testing.T) {
	logical, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		`select empno, sum(sal)
		   from constraint_test.emp
		  where deptno > 0
		  group by empno
		 having sum(sal) > 100
		  limit 1 offset 1`,
	)
	require.NoError(t, err)
	query := logical.GetQuery()
	require.False(t, reachableNodeType(query, planpb.Node_AGG))
	scan := firstReachableNode(query, planpb.Node_TABLE_SCAN)
	require.NotNil(t, scan)
	require.Len(t, scan.FilterList, 2,
		"both WHERE and remapped HAVING predicates must precede bounded demand")
	require.Equal(t, uint64(1), scan.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(1), scan.Offset.GetLit().GetU64Val())
}

func reachableNodeType(query *planpb.Query, typ planpb.Node_NodeType) bool {
	return firstReachableNode(query, typ) != nil
}

func reachableNodeTypeCount(query *planpb.Query, typ planpb.Node_NodeType) int {
	if query == nil {
		return 0
	}
	seen := make(map[int32]struct{})
	count := 0
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := seen[nodeID]; ok {
			return
		}
		seen[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node.NodeType == typ {
			count++
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, root := range query.Steps {
		visit(root)
	}
	return count
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

func requireNoDanglingColumnTags(t *testing.T, query *planpb.Query) {
	t.Helper()
	reachable := make(map[int32]struct{})
	producedTags := make(map[int32]struct{})
	var collect func(int32)
	collect = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := reachable[nodeID]; ok {
			return
		}
		reachable[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		for _, tag := range node.BindingTags {
			producedTags[tag] = struct{}{}
		}
		for _, child := range node.Children {
			collect(child)
		}
	}
	for _, root := range query.Steps {
		collect(root)
	}

	for nodeID := range reachable {
		node := query.Nodes[nodeID]
		err := planpb.VisitExpressionsInOwner(node, func(root *planpb.Expr) error {
			return planpb.VisitExprTree(root, func(expr *planpb.Expr) error {
				col := expr.GetCol()
				if col == nil || col.RelPos <= 0 {
					return nil
				}
				_, ok := producedTags[col.RelPos]
				require.Truef(t, ok,
					"node %d references column tag %d, but no reachable node produces it",
					nodeID, col.RelPos)
				return nil
			})
		})
		require.NoError(t, err)
	}
}
