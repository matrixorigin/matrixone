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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestProveInsertInputKeysUnique(t *testing.T) {
	tests := []struct {
		name       string
		targetPK   []string
		insertCols []string
		sourcePK   []string
		shape      func(*planpb.Node) *planpb.Node
		want       bool
	}{
		{
			name:       "identity project",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			want:       true,
		},
		{
			name:       "filter preserves uniqueness",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			shape: func(_ *planpb.Node) *planpb.Node {
				return &planpb.Node{
					NodeType: planpb.Node_FILTER,
					Children: []int32{1},
				}
			},
			want: true,
		},
		{
			name:       "sort preserves uniqueness",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			shape: func(_ *planpb.Node) *planpb.Node {
				return &planpb.Node{
					NodeType: planpb.Node_SORT,
					Children: []int32{1},
				}
			},
			want: true,
		},
		{
			name:       "stacked projects preserve uniqueness",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			shape: func(_ *planpb.Node) *planpb.Node {
				return &planpb.Node{
					NodeType:    planpb.Node_PROJECT,
					Children:    []int32{1},
					BindingTags: []int32{30},
					ProjectList: []*planpb.Expr{
						insertProofColExpr(20, 0),
						insertProofColExpr(20, 1),
					},
				}
			},
			want: true,
		},
		{
			name:       "composite target contains source key",
			targetPK:   []string{"id", "tenant"},
			insertCols: []string{"id", "tenant", "payload"},
			sourcePK:   []string{"id"},
			want:       true,
		},
		{
			name:       "source key is not target key",
			targetPK:   []string{"tenant"},
			insertCols: []string{"id", "tenant", "payload"},
			sourcePK:   []string{"id"},
			want:       false,
		},
		{
			name:       "target key omitted",
			targetPK:   []string{"id", "tenant"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			want:       false,
		},
		{
			name:       "target key specified twice",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "id", "payload"},
			sourcePK:   []string{"id"},
			want:       false,
		},
		{
			name:       "target key specified twice with different case",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "ID", "payload"},
			sourcePK:   []string{"id"},
			want:       false,
		},
		{
			name:       "source composite key does not prove simple target key",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "tenant", "payload"},
			sourcePK:   []string{"id", "tenant"},
			want:       false,
		},
		{
			name:       "fake source primary key",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"__mo_fake_pk_col"},
			want:       false,
		},
		{
			name:       "join rejected",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			shape: func(_ *planpb.Node) *planpb.Node {
				return &planpb.Node{NodeType: planpb.Node_JOIN, Children: []int32{1, 0}}
			},
			want: false,
		},
		{
			name:       "computed projection rejected",
			targetPK:   []string{"id"},
			insertCols: []string{"id", "payload"},
			sourcePK:   []string{"id"},
			shape: func(project *planpb.Node) *planpb.Node {
				project.ProjectList[0] = &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{}}}
				return project
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := makeInsertProofTable("source", tc.sourcePK)
			target := makeInsertProofTable("target", tc.targetPK)
			scan := &planpb.Node{
				NodeType:        planpb.Node_TABLE_SCAN,
				TableDef:        source,
				BindingTags:     []int32{10},
				ProjectList:     nil,
				FilterList:      nil,
				BlockFilterList: nil,
			}
			projectList := []*planpb.Expr{
				insertProofColExpr(10, 0),
				insertProofColExpr(10, 1),
			}
			if len(tc.insertCols) == 3 {
				projectList = append(projectList, insertProofColExpr(10, 2))
			}
			project := &planpb.Node{
				NodeType:    planpb.Node_PROJECT,
				Children:    []int32{0},
				BindingTags: []int32{20},
				ProjectList: projectList,
			}
			nodes := []*planpb.Node{scan, project}
			root := project
			if tc.shape != nil {
				root = tc.shape(project)
				nodes = append(nodes, root)
				if root.NodeType == planpb.Node_JOIN {
					root.Children[1] = 0
				}
			}
			builder := &QueryBuilder{qry: &planpb.Query{Nodes: nodes}}
			rootID := int32(len(nodes) - 1)
			if tc.shape == nil {
				rootID = 1
			}
			require.Equal(t, tc.want, builder.proveInsertInputKeysUnique(rootID, tc.insertCols, target))
		})
	}
}

func TestInsertRejectsDuplicateTargetColumns(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		duplicateCol string
	}{
		{
			name:         "insert select exact case",
			query:        "insert into constraint_test.emp (empno, empno, job) select empno, deptno, job from constraint_test.employees",
			duplicateCol: "empno",
		},
		{
			name:         "insert select different case",
			query:        "insert into constraint_test.emp (empno, EMPNO, job) select empno, deptno, job from constraint_test.employees",
			duplicateCol: "empno",
		},
		{
			name:         "insert values non-key column",
			query:        "insert into constraint_test.emp (empno, ename, ENAME) values (1, 'alice', 'bob')",
			duplicateCol: "ename",
		},
		{
			name:         "replace values",
			query:        "replace into constraint_test.emp (empno, empno, job) values (1, 2, 'engineer')",
			duplicateCol: "empno",
		},
		{
			name:         "generated columns before rewrite",
			query:        "insert into constraint_test.t_on_update_gen (g, G) values (default, default)",
			duplicateCol: "g",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tc.query)
			require.Error(t, err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ER_FIELD_SPECIFIED_TWICE), moErr.MySQLCode())
			require.Equal(t, "42000", moErr.SqlState())
			require.Equal(t, "Column '"+tc.duplicateCol+"' specified twice", moErr.Error())
		})
	}
}

func TestInsertSelectMarksTargetPKDedupInputUnique(t *testing.T) {
	queries := []string{
		"insert into constraint_test.emp (empno, ename, job) " +
			"select empno, ename, job from constraint_test.emp where empno > 0",
		"insert into constraint_test.emp (empno, ename, job) " +
			"select empno, ename, job from constraint_test.emp where empno > 0 order by empno",
		"insert into constraint_test.emp (empno, ename, job) " +
			"(select empno, ename, job from constraint_test.emp where empno > 0)",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			logicPlan, err := runOneStmt(mock, t, query)
			require.NoError(t, err)
			require.NotNil(t, logicPlan.GetQuery())

			var marked int
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_DEDUP &&
					node.DedupInputKeysUnique {
					marked++
				}
			}
			require.Equal(t, 1, marked, "only the target-PK DEDUP should use the proof")
		})
	}
}

func TestInsertSelectMarksCrossTableTargetPKDedupInputUnique(t *testing.T) {
	mock := NewMockOptimizer(true)
	// The mock catalog omits Type.Table by default. Populate its two real table
	// entries so Resolve returns the same distinct provenance as production.
	for _, tableName := range []string{"emp", "employees"} {
		tableDef := mock.ctxt.tables[tableName]
		require.NotNil(t, tableDef)
		for _, col := range tableDef.Cols {
			col.Typ.Table = tableName
		}
	}

	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.emp (empno, ename, job) "+
			"select empno, ename, job from constraint_test.employees where empno > 0")
	require.NoError(t, err)
	require.NotNil(t, logicPlan.GetQuery())

	keyProvenance := make(map[string]string)
	marked := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.TableDef != nil {
			for _, col := range node.TableDef.Cols {
				if col.Name == "empno" {
					keyProvenance[node.TableDef.Name] = col.Typ.Table
					break
				}
			}
		}
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_DEDUP &&
			node.DedupInputKeysUnique {
			marked++
		}
	}
	require.Equal(t, "employees", keyProvenance["employees"])
	require.Equal(t, "emp", keyProvenance["emp"])
	require.Equal(t, 1, marked, "table provenance must not change compatible key encodings")
}

func TestInsertSelectInputUniqueProofRequiresProtocolVersion21(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.CurrentContext().GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	countMarkedDedup := func(version int64) int {
		t.Helper()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		logicPlan, err := runOneStmt(mock, t,
			"insert into constraint_test.emp (empno, ename, job) "+
				"select empno, ename, job from constraint_test.emp where empno > 0")
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetQuery())

		marked := 0
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_DEDUP &&
				node.DedupInputKeysUnique {
				marked++
			}
		}
		return marked
	}

	require.Zero(t, countMarkedDedup(defines.MORPCVersion20),
		"the ordinary plan must remain active until every remote executor supports lookup-only RIGHT DEDUP")
	require.Equal(t, 1, countMarkedDedup(defines.MORPCVersion21))
}

func TestProveInsertInputKeysUniqueRejectsKeyTypeChange(t *testing.T) {
	source := makeInsertProofTable("source", []string{"id"})
	target := makeInsertProofTable("target", []string{"id"})
	source.Cols[0].Typ = planpb.Type{Id: int32(types.T_int64)}
	target.Cols[0].Typ = planpb.Type{Id: int32(types.T_int32)}
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, TableDef: source, BindingTags: []int32{10}},
		{NodeType: planpb.Node_PROJECT, Children: []int32{0}, BindingTags: []int32{20}, ProjectList: []*planpb.Expr{
			insertProofColExpr(10, 0), insertProofColExpr(10, 1),
		}},
	}}}

	require.False(t, builder.proveInsertInputKeysUnique(1, []string{"id", "payload"}, target))
}

func makeInsertProofTable(name string, pk []string) *planpb.TableDef {
	cols := []*planpb.ColDef{
		{Name: "id"},
		{Name: "tenant"},
		{Name: "payload"},
	}
	name2pos := map[string]int32{"id": 0, "tenant": 1, "payload": 2}
	return &planpb.TableDef{
		Name:          name,
		Cols:          cols,
		Name2ColIndex: name2pos,
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: pk[0],
			Names:       pk,
		},
	}
}

func insertProofColExpr(tag, pos int32) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos}}}
}
