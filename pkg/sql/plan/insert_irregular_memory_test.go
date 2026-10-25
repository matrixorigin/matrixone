// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Maintenance consumes the rows that will actually be inserted: IGNORE's
// conflicts have already been removed, and UPDATE's merged values are present.
// Plain INSERT/LOAD does not need old-index deletion or old-table payloads.
func TestInsertIrregularMaintenanceAction(t *testing.T) {
	for _, tc := range []struct {
		name, sql  string
		wantDelete bool
	}{
		{"insert", "insert into constraint_test.dept values (10,'SALES','CHICAGO')", false},
		{"ignore", "insert ignore into constraint_test.dept values (10,'SALES','CHICAGO')", false},
		{"update", "insert into constraint_test.dept values (10,'SALES','CHICAGO') on duplicate key update loc='NEW'", true},
	} {
		for _, regular := range []bool{false, true} {
			indexShape := "fulltext_only"
			if regular {
				indexShape = "fulltext_and_regular"
			}
			t.Run(tc.name+"/"+indexShape, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), tc.sql, 1)
				require.NoError(t, err)
				stmt := stmts[0].(*tree.Insert)
				b := NewQueryBuilder(planpb.Query_INSERT, mock.CurrentContext(), false, true)
				ctx := NewBindContext(b, nil)
				ref, def, err := b.compCtx.Resolve("constraint_test", "dept", nil)
				require.NoError(t, err)
				if !regular {
					def.Indexes = nil
				}
				// The mixed-index shape catches losing IGNORE's filtered image when
				// removing plain INSERT's unnecessary old-index deletion. Fulltext
				// alone proves filtering does not depend on regular-index projections.
				def.Indexes = append(def.Indexes, &planpb.IndexDef{IndexName: "ft_name", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexTableName: "__mo_index_ft_name", TableExist: true, Parts: []string{"dname"}})
				irregular := getIrregularIndexes(def)
				require.Len(t, irregular, 1)
				def.Name2ColIndex = make(map[string]int32, len(def.Cols))
				for i, col := range def.Cols {
					def.Name2ColIndex[col.Name] = int32(i)
				}
				dml := NewDMLContext()
				dml.objRefs = []*planpb.ObjectRef{ref}
				dml.tableDefs = []*planpb.TableDef{def}
				node, cols, skip, err := b.initInsertReplaceStmt(ctx, stmt.Rows, stmt.Columns, ref, def, false)
				require.NoError(t, err)
				_, err = b.appendDedupAndMultiUpdateNodesForBindInsert(ctx, dml, node, cols, skip, stmt.OnDuplicateUpdate, irregular)
				require.NoError(t, err)
				require.GreaterOrEqual(t, b.irregularMaintSourceStep, int32(0), "all actions still insert the new fulltext entries")
				require.Equal(t, tc.wantDelete, b.irregularMaintDeleteStep >= 0, "only ODKU needs to delete conflicting rows' old entries")
				var dedupActions []planpb.Node_OnDuplicateAction
				seen := make(map[int32]bool)
				var visit func(int32)
				visit = func(id int32) {
					if seen[id] {
						return
					}
					seen[id] = true
					n := b.qry.Nodes[id]
					if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP {
						dedupActions = append(dedupActions, n.OnDuplicateAction)
					}
					for _, child := range n.Children {
						visit(child)
					}
					for _, step := range n.SourceStep {
						visit(b.qry.Steps[step])
					}
				}
				visit(b.qry.Steps[b.irregularMaintSourceStep])
				switch tc.name {
				case "insert":
					require.Empty(t, dedupActions, "plain input source must not retain the old-table dedup subtree")
				case "ignore":
					require.NotEmpty(t, dedupActions, "maintenance must consume the post-dedup surviving rows")
					for _, action := range dedupActions {
						require.Equal(t, planpb.Node_IGNORE, action)
					}
				case "update":
					require.Contains(t, dedupActions, planpb.Node_UPDATE, "maintenance must consume the post-merge final image")
					require.Equal(t, b.irregularMaintSourceStep, b.irregularMaintDeleteStep)
				}
			})
		}
	}
}
