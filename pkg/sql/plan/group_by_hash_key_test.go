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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func groupHashKeyTestCol(tag, pos int32) *pbplan.Expr {
	return &pbplan.Expr{Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: tag, ColPos: pos}}}
}

func groupHashKeyTestTable(pkName string, pkNames ...string) *pbplan.TableDef {
	intType := pbplan.Type{Id: int32(types.T_int64)}
	return &pbplan.TableDef{
		Cols: []*pbplan.ColDef{
			{Name: "id", Typ: intType},
			{Name: "tenant", Typ: intType},
			{Name: "payload", Typ: intType},
		},
		Name2ColIndex: map[string]int32{
			"id": 0, "tenant": 1, "payload": 2,
		},
		Pkey: &pbplan.PrimaryKeyDef{PkeyColName: pkName, Names: pkNames},
	}
}

func TestDetermineGroupByHashKeys(t *testing.T) {
	complexPayload := &pbplan.Expr{Expr: &pbplan.Expr_F{F: &pbplan.Function{
		Args: []*pbplan.Expr{groupHashKeyTestCol(1, 2)},
	}}}
	float64Key := groupHashKeyTestTable("id", "id")
	float64Key.Cols[0].Typ = pbplan.Type{Id: int32(types.T_float64)}
	charKey := groupHashKeyTestTable("id", "id")
	charKey.Cols[0].Typ = pbplan.Type{Id: int32(types.T_char), Width: 8}
	varcharKey := groupHashKeyTestTable("id", "id")
	varcharKey.Cols[0].Typ = pbplan.Type{Id: int32(types.T_varchar), Width: 8}
	collatedVarcharKey := groupHashKeyTestTable("id", "id")
	collatedVarcharKey.Cols[0].Typ = pbplan.Type{
		Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8),
	}
	scaledFloat32CompositeKey := groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")
	scaledFloat32CompositeKey.Cols[1].Typ = pbplan.Type{
		Id: int32(types.T_float32), Width: 8, Scale: 2,
	}

	tests := []struct {
		name          string
		groupBy       []*pbplan.Expr
		groupingFlags []bool
		tables        map[int32]*pbplan.TableDef
		want          []int32
	}{
		{
			name:    "single primary key determines payload",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0},
		},
		{
			name:    "group order is preserved",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, 0)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{1},
		},
		{
			name: "complete composite primary key",
			groupBy: []*pbplan.Expr{
				groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, 1), groupHashKeyTestCol(1, 0),
			},
			tables: map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")},
			want:   []int32{1, 2},
		},
		{
			name:    "incomplete composite primary key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")},
		},
		{
			name:    "float64 primary key is not a grouping dependency proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: float64Key},
		},
		{
			name:    "char primary key is not a grouping dependency proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: charKey},
		},
		{
			name:    "varchar primary key remains a grouping dependency proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: varcharKey},
			want:    []int32{0},
		},
		{
			name:    "collated varchar primary key is not a grouping dependency proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: collatedVarcharKey},
		},
		{
			name: "scaled float32 composite component invalidates grouping dependency proof",
			groupBy: []*pbplan.Expr{
				groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 1), groupHashKeyTestCol(1, 2),
			},
			tables: map[int32]*pbplan.TableDef{1: scaledFloat32CompositeKey},
		},
		{
			name:          "grouping set is not reduced",
			groupBy:       []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			groupingFlags: []bool{true, false},
			tables:        map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
		},
		{
			name: "only columns from the determined table are removed",
			groupBy: []*pbplan.Expr{
				groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), groupHashKeyTestCol(2, 2),
			},
			tables: map[int32]*pbplan.TableDef{
				1: groupHashKeyTestTable("id", "id"),
				2: {Cols: []*pbplan.ColDef{{Name: "id"}, {Name: "tenant"}, {Name: "payload"}}},
			},
			want: []int32{0, 2},
		},
		{
			name:    "derived expression remains a physical key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), complexPayload},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0, 2},
		},
		{
			name:    "synthetic column remains a physical key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, -1)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0, 2},
		},
		{
			name:    "fake primary key is not a proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.FakePrimaryKeyColName, catalog.FakePrimaryKeyColName)},
		},
		{
			name:    "hidden composite key without components is not a proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agg := &pbplan.Node{
				NodeId: 1, NodeType: pbplan.Node_AGG, Children: []int32{0},
				GroupBy: test.groupBy, GroupingFlag: test.groupingFlags,
			}
			builder := &QueryBuilder{
				qry:       &pbplan.Query{Nodes: []*pbplan.Node{{NodeId: 0, NodeType: pbplan.Node_TABLE_SCAN}, agg}},
				tag2Table: test.tables,
			}
			builder.determineGroupByHashKeys(1)
			require.Equal(t, test.want, agg.GroupByHashKey)
		})
	}
}

func TestBuildPlanUsesPrimaryKeyAsPhysicalGroupKey(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno, ename, sum(sal) from constraint_test.emp group by empno, ename",
	)
	require.NoError(t, err)

	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 2 {
			found = true
			require.Equal(t, []int32{0}, node.GroupByHashKey)
			require.Len(t, node.GroupBy, 2, "logical group output must remain unchanged")
		}
	}
	require.True(t, found)
}

func TestBuildPlanKeepsAllPhysicalGroupKeysForIncompatiblePrimaryKey(t *testing.T) {
	tests := []struct {
		name string
		typ  pbplan.Type
	}{
		{name: "float64 signed zero", typ: pbplan.Type{Id: int32(types.T_float64)}},
		{name: "char trailing spaces", typ: pbplan.Type{Id: int32(types.T_char), Width: 8}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			require.NotNil(t, table)
			require.NotNil(t, table.Pkey)

			var primaryKeyColumn *pbplan.ColDef
			for _, col := range table.Cols {
				if col.Name == "empno" {
					primaryKeyColumn = col
					break
				}
			}
			require.NotNil(t, primaryKeyColumn)
			primaryKeyColumn.Typ = test.typ
			table.Pkey.CompPkeyCol = primaryKeyColumn

			logicPlan, err := runOneStmt(
				optimizer,
				t,
				"select empno, ename, sum(sal) from constraint_test.emp group by empno, ename",
			)
			require.NoError(t, err)

			found := false
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 2 {
					found = true
					require.Empty(t, node.GroupByHashKey,
						"storage-distinct primary keys can share one SQL grouping value")
				}
			}
			require.True(t, found)
		})
	}
}

func TestAggPullupRequiresGroupingCompatiblePrimaryKey(t *testing.T) {
	tests := []struct {
		name         string
		typ          pbplan.Type
		leftType     pbplan.Type
		rightPKName  string
		rightJoinPos int32
		wantPullup   bool
	}{
		{
			name:        "varchar control",
			typ:         pbplan.Type{Id: int32(types.T_varchar), Width: 8, NotNullable: true},
			rightPKName: "id",
			wantPullup:  true,
		},
		{
			name:         "right primary key ordinal differs from aggregate output ordinal",
			typ:          pbplan.Type{Id: int32(types.T_varchar), Width: 8, NotNullable: true},
			rightPKName:  "tenant",
			rightJoinPos: 1,
			wantPullup:   true,
		},
		{
			name:         "right non-primary column shares aggregate output ordinal",
			typ:          pbplan.Type{Id: int32(types.T_varchar), Width: 8, NotNullable: true},
			rightPKName:  "id",
			rightJoinPos: 1,
		},
		{
			name:        "float64 signed zero",
			typ:         pbplan.Type{Id: int32(types.T_float64), NotNullable: true},
			rightPKName: "id",
		},
		{
			name:        "char trailing spaces",
			typ:         pbplan.Type{Id: int32(types.T_char), Width: 8, NotNullable: true},
			rightPKName: "id",
		},
		{
			name: "collated varchar equality",
			typ: pbplan.Type{
				Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8), NotNullable: true,
			},
			rightPKName: "id",
		},
		{
			name:        "datetime timestamp cross-type equality",
			typ:         pbplan.Type{Id: int32(types.T_timestamp), NotNullable: true},
			leftType:    pbplan.Type{Id: int32(types.T_datetime), NotNullable: true},
			rightPKName: "id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leftType := test.typ
			if test.leftType.Id != 0 {
				leftType = test.leftType
			}
			leftTable := groupHashKeyTestTable("id", "id")
			leftTable.Cols[0].Typ = leftType
			leftScan := &pbplan.Node{
				NodeId: 0, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{10},
				TableDef: leftTable, Stats: &pbplan.Stats{Outcnt: 1},
			}
			agg := &pbplan.Node{
				NodeId: 1, NodeType: pbplan.Node_AGG, Children: []int32{0},
				BindingTags: []int32{20, 21},
				GroupBy:     []*pbplan.Expr{GetColExpr(leftType, 10, 0)},
				Stats:       &pbplan.Stats{Outcnt: 1},
			}
			rightTable := groupHashKeyTestTable(test.rightPKName, test.rightPKName)
			rightTable.Cols[rightTable.Name2ColIndex[test.rightPKName]].Typ = test.typ
			rightTable.Cols[test.rightJoinPos].Typ = test.typ
			rightScan := &pbplan.Node{
				NodeId: 2, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{30},
				TableDef: rightTable, Stats: &pbplan.Stats{Outcnt: 1},
			}
			joinCondition := &pbplan.Expr{
				Typ: pbplan.Type{Id: int32(types.T_bool), NotNullable: true},
				Expr: &pbplan.Expr_F{F: &pbplan.Function{
					Func: getFunctionObjRef(
						function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
					Args: []*pbplan.Expr{
						GetColExpr(leftType, 20, 0),
						GetColExpr(test.typ, 30, test.rightJoinPos),
					},
				}},
			}
			join := &pbplan.Node{
				NodeId: 3, NodeType: pbplan.Node_JOIN, JoinType: pbplan.Node_INNER,
				Children: []int32{1, 2}, OnList: []*pbplan.Expr{joinCondition},
				Stats: &pbplan.Stats{Outcnt: 1},
			}
			builder := &QueryBuilder{qry: &pbplan.Query{
				Nodes: []*pbplan.Node{leftScan, agg, rightScan, join},
			}}

			pulledUp := applyAggPullup(3, join, agg, leftScan, rightScan, builder)

			require.Equal(t, test.wantPullup, pulledUp)
		})
	}
}

func TestAggPullupRequiresGroupOutputBijection(t *testing.T) {
	intType := pbplan.Type{Id: int32(types.T_int64), NotNullable: true}
	tests := []struct {
		name             string
		leftOutputByJoin []int32
		wantPullup       bool
	}{
		{name: "permuted complete outputs", leftOutputByJoin: []int32{1, 0}, wantPullup: true},
		{name: "duplicate output", leftOutputByJoin: []int32{0, 0}},
		{name: "out of range output", leftOutputByJoin: []int32{0, 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leftScan := &pbplan.Node{
				NodeId: 0, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{10},
				TableDef: groupHashKeyTestTable("id", "id"), Stats: &pbplan.Stats{Outcnt: 1},
			}
			agg := &pbplan.Node{
				NodeId: 1, NodeType: pbplan.Node_AGG, Children: []int32{0},
				BindingTags: []int32{20, 21},
				GroupBy: []*pbplan.Expr{
					GetColExpr(intType, 10, 1),
					GetColExpr(intType, 10, 2),
				},
				Stats: &pbplan.Stats{Outcnt: 1},
			}
			rightTable := groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")
			rightTable.Cols[0].Typ = intType
			rightTable.Cols[1].Typ = intType
			rightScan := &pbplan.Node{
				NodeId: 2, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{30},
				TableDef: rightTable, Stats: &pbplan.Stats{Outcnt: 1},
			}
			joinConditions := make([]*pbplan.Expr, 2)
			for i := range joinConditions {
				joinConditions[i] = &pbplan.Expr{
					Typ: pbplan.Type{Id: int32(types.T_bool), NotNullable: true},
					Expr: &pbplan.Expr_F{F: &pbplan.Function{
						Func: getFunctionObjRef(
							function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
						Args: []*pbplan.Expr{
							GetColExpr(intType, 20, test.leftOutputByJoin[i]),
							GetColExpr(intType, 30, int32(i)),
						},
					}},
				}
			}
			join := &pbplan.Node{
				NodeId: 3, NodeType: pbplan.Node_JOIN, JoinType: pbplan.Node_INNER,
				Children: []int32{1, 2}, OnList: joinConditions,
				Stats: &pbplan.Stats{Outcnt: 1},
			}
			builder := &QueryBuilder{qry: &pbplan.Query{
				Nodes: []*pbplan.Node{leftScan, agg, rightScan, join},
			}}

			pulledUp := applyAggPullup(3, join, agg, leftScan, rightScan, builder)

			require.Equal(t, test.wantPullup, pulledUp)
			if test.wantPullup {
				require.Equal(t, int32(2), joinConditions[0].GetF().Args[0].GetCol().ColPos)
				require.Equal(t, int32(1), joinConditions[1].GetF().Args[0].GetCol().ColPos)
			}
		})
	}
}

func TestAggPullupRequiresTypeMetadataToMatchReferencedColumns(t *testing.T) {
	intType := pbplan.Type{Id: int32(types.T_int64), NotNullable: true}
	uintType := pbplan.Type{Id: int32(types.T_uint64), NotNullable: true}
	tests := []struct {
		name       string
		configure  func(*pbplan.Node, *pbplan.Node, *pbplan.Node, *pbplan.Expr)
		wantPullup bool
	}{
		{name: "consistent metadata control", wantPullup: true},
		{
			name: "join output type disagrees with aggregate output",
			configure: func(_ *pbplan.Node, _ *pbplan.Node, right *pbplan.Node, cond *pbplan.Expr) {
				cond.GetF().Args[0].Typ = uintType
				cond.GetF().Args[1].Typ = uintType
				right.TableDef.Cols[0].Typ = uintType
			},
		},
		{
			name: "join key type disagrees with right table column",
			configure: func(left *pbplan.Node, agg *pbplan.Node, _ *pbplan.Node, cond *pbplan.Expr) {
				left.TableDef.Cols[0].Typ = uintType
				agg.GroupBy[0].Typ = uintType
				cond.GetF().Args[0].Typ = uintType
				cond.GetF().Args[1].Typ = uintType
			},
		},
		{
			name: "group expression type disagrees with left table column",
			configure: func(_ *pbplan.Node, agg *pbplan.Node, right *pbplan.Node, cond *pbplan.Expr) {
				agg.GroupBy[0].Typ = uintType
				cond.GetF().Args[0].Typ = uintType
				cond.GetF().Args[1].Typ = uintType
				right.TableDef.Cols[0].Typ = uintType
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leftTable := groupHashKeyTestTable("id", "id")
			leftTable.Cols[0].Typ = intType
			leftScan := &pbplan.Node{
				NodeId: 0, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{10},
				TableDef: leftTable, Stats: &pbplan.Stats{Outcnt: 1},
			}
			agg := &pbplan.Node{
				NodeId: 1, NodeType: pbplan.Node_AGG, Children: []int32{0},
				BindingTags: []int32{20, 21},
				GroupBy:     []*pbplan.Expr{GetColExpr(intType, 10, 0)},
				Stats:       &pbplan.Stats{Outcnt: 1},
			}
			rightTable := groupHashKeyTestTable("id", "id")
			rightTable.Cols[0].Typ = intType
			rightScan := &pbplan.Node{
				NodeId: 2, NodeType: pbplan.Node_TABLE_SCAN, BindingTags: []int32{30},
				TableDef: rightTable, Stats: &pbplan.Stats{Outcnt: 1},
			}
			joinCondition := &pbplan.Expr{
				Typ: pbplan.Type{Id: int32(types.T_bool), NotNullable: true},
				Expr: &pbplan.Expr_F{F: &pbplan.Function{
					Func: getFunctionObjRef(
						function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
					Args: []*pbplan.Expr{
						GetColExpr(intType, 20, 0),
						GetColExpr(intType, 30, 0),
					},
				}},
			}
			join := &pbplan.Node{
				NodeId: 3, NodeType: pbplan.Node_JOIN, JoinType: pbplan.Node_INNER,
				Children: []int32{1, 2}, OnList: []*pbplan.Expr{joinCondition},
				Stats: &pbplan.Stats{Outcnt: 1},
			}
			if test.configure != nil {
				test.configure(leftScan, agg, rightScan, joinCondition)
			}
			builder := &QueryBuilder{qry: &pbplan.Query{
				Nodes: []*pbplan.Node{leftScan, agg, rightScan, join},
			}}

			pulledUp := applyAggPullup(3, join, agg, leftScan, rightScan, builder)

			require.Equal(t, test.wantPullup, pulledUp)
		})
	}
}

func TestBuildPlanAnnotatesDistinctRewriteAggregate(t *testing.T) {
	for _, aggregate := range []string{"count", "sum"} {
		t.Run(aggregate, func(t *testing.T) {
			logicPlan, err := runOneStmt(
				NewMockOptimizer(false),
				t,
				fmt.Sprintf(
					"select empno, ename, %s(distinct deptno) from constraint_test.emp group by empno, ename",
					aggregate,
				),
			)
			require.NoError(t, err)

			var outer, inner *pbplan.Node
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != pbplan.Node_AGG {
					continue
				}
				switch len(node.GroupBy) {
				case 2:
					outer = node
				case 3:
					inner = node
				}
			}

			require.NotNil(t, outer, "distinct rewrite must retain the original aggregate")
			require.NotNil(t, inner, "distinct rewrite must create a deduplication aggregate")
			require.Equal(t, []int32{0}, outer.GroupByHashKey,
				"rewriting the outer expressions must preserve its proven physical key")
			require.Equal(t, []int32{0}, inner.GroupByHashKey,
				"the primary key determines both the payload and distinct argument")
		})
	}
}

func TestBuildPlanKeepsMixedCountDistinctParallelMergeable(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select e.deptno, count(d.deptno), count(distinct d.loc) "+
			"from constraint_test.emp e left join constraint_test.dept d on e.deptno = d.deptno "+
			"group by e.deptno",
	)
	require.NoError(t, err)

	var mixedAgg *pbplan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.AggList) == 2 {
			mixedAgg = node
			break
		}
	}
	require.NotNil(t, mixedAgg)
	require.False(t, RequiresSingleStageDistinctAgg(mixedAgg))
	var distinctCount bool
	for _, expr := range mixedAgg.AggList {
		agg := expr.GetF()
		if agg != nil && agg.Func != nil && uint64(agg.Func.Obj)&function.Distinct != 0 {
			distinctCount = true
		}
	}
	require.True(t, distinctCount, "mixed aggregates must retain COUNT(DISTINCT) for MergeGroup")
}

func TestBuildPlanAnnotatesJoinDistinctRewriteAggregate(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select e.empno, e.ename, count(distinct d.loc) "+
			"from constraint_test.emp e left join constraint_test.dept d on e.deptno = d.deptno "+
			"group by e.empno, e.ename",
	)
	require.NoError(t, err)

	var inner *pbplan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 3 {
			inner = node
			break
		}
	}
	require.NotNil(t, inner)
	require.Equal(t, []int32{0, 2}, inner.GroupByHashKey)
}

func TestBuildPlanAnnotatesJoinPromotedCharDistinctRewriteAggregate(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select e.empno, e.ename, count(distinct "+
			"coalesce(cast(d.dname as char(8)), cast(d.loc as varchar(8)))) "+
			"from constraint_test.emp e left join constraint_test.dept d on e.deptno = d.deptno "+
			"group by e.empno, e.ename",
	)
	require.NoError(t, err)

	var inner *pbplan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 4 {
			inner = node
			break
		}
	}
	require.NotNil(t, inner)
	require.Equal(t, []int32{0, 3}, inner.GroupByHashKey)
	require.True(t, isCastOverload(inner.GroupBy[3], 3))
}
