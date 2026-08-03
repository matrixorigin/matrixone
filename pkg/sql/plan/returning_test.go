// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestDMLReturningPlansUseDedicatedStep(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		headings []string
	}{
		{"insert final image", "insert into nation values (1, 'n', 2, 'c') returning n_nationkey, n_name as name", []string{"n_nationkey", "name"}},
		{"update new image", "update nation as n set n_name = 'changed' where n_nationkey = 1 returning n.n_nationkey, n.n_name", []string{"n_nationkey", "n_name"}},
		{"delete old image", "delete from nation where n_nationkey = 1 returning *", []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}},
		{"delete no filter", "delete from nation returning n_nationkey", []string{"n_nationkey"}},
		{"qualified star", "update nation as n set n_name = 'changed' where n_nationkey = 1 returning n.*", []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.True(t, query.HasReturning)
			require.GreaterOrEqual(t, query.ReturningStep, int32(0))
			require.Less(t, int(query.ReturningStep), len(query.Steps))
			root := query.Nodes[query.Steps[query.ReturningStep]]
			require.Equal(t, planpb.Node_PROJECT, root.NodeType)
			require.Len(t, root.Children, 1)
			returningScan := query.Nodes[root.Children[0]]
			require.Equal(t, planpb.Node_SINK_SCAN, returningScan.NodeType)
			require.Len(t, returningScan.SourceStep, 1)
			for _, col := range returningScan.TableDef.Cols {
				require.False(t, col.Hidden)
				require.NotEqual(t, "__mo_rowid", col.Name)
			}
			require.Equal(t, test.headings, query.Headings)
			require.Len(t, root.ProjectList, len(test.headings))
			resultCols := GetResultColumnsFromPlan(logicPlan)
			require.Len(t, resultCols, len(test.headings))
			for i := range resultCols {
				require.Equal(t, test.headings[i], resultCols[i].Name)
			}
			hasMutation := false
			for _, node := range query.Nodes {
				hasMutation = hasMutation || node.NodeType == planpb.Node_MULTI_UPDATE
			}
			require.True(t, hasMutation, "RETURNING must not replace the base-table mutation")
		})
	}
}

func TestDMLReturningRejectsV1NonGoals(t *testing.T) {
	tests := []struct {
		sql     string
		feature string
	}{
		{"insert ignore into nation values (1, 'n', 2, 'c') returning *", "INSERT IGNORE"},
		{"insert overwrite into nation values (1, 'n', 2, 'c') returning *", "INSERT OVERWRITE"},
		{"insert into nation values (1, 'n', 2, 'c') on duplicate key update n_name = values(n_name) returning *", "INSERT ON DUPLICATE KEY UPDATE"},
		{"update ignore nation set n_name = 'x' returning n_name", "UPDATE IGNORE"},
		{"update low_priority nation set n_name = 'x' returning n_name", "LOW_PRIORITY UPDATE"},
		{"update nation set n_name = 'x' returning rand()", "volatile function in RETURNING expression"},
		{"update nation set n_name = 'x' returning sleep(0)", "volatile function in RETURNING expression"},
		{"update nation join region on nation.n_regionkey = region.r_regionkey set nation.n_name = 'x' returning nation.n_name", "joined UPDATE"},
		{"update nation, region set nation.n_name = 'x' returning nation.n_name", "multi-table UPDATE"},
		{"update nation set n_name = 'x' from region where nation.n_regionkey = region.r_regionkey returning nation.n_name", "UPDATE FROM"},
		{"update nation set n_name = 'x' returning count(*)", "aggregate in RETURNING expression"},
		{"update nation set n_name = 'x' returning row_number() over ()", "window function in RETURNING expression"},
		{"update nation set n_name = 'x' returning @returning_value", "variable in RETURNING expression"},
		{"delete from nation returning (select 1)", "subquery in RETURNING expression"},
		{"delete quick from nation returning n_nationkey", "DELETE QUICK"},
		{"delete ignore from nation returning n_nationkey", "DELETE IGNORE"},
		{"delete from nation partition(p0) returning n_nationkey", "explicit PARTITION DML"},
		{"delete from nation using nation join region on nation.n_regionkey = region.r_regionkey returning nation.n_name", "DELETE USING"},
		{"update nation set n_name = 'x' returning old.n_name", "old/new pseudo namespace"},
		{"update nation set n_name = 'x' returning region.r_name", "non-target source in RETURNING expression"},
		{"with c as (select 1) update nation set n_name = 'x' returning n_name", "WITH DML"},
		{"replace into nation values (1, 'n', 2, 'c') returning *", "REPLACE"},
		{"merge into nation using region on nation.n_regionkey = region.r_regionkey when matched then delete returning *", "MERGE"},
	}

	for _, test := range tests {
		t.Run(test.feature, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), "DML RETURNING does not support "+test.feature)
		})
	}
}

func TestDMLReturningForeignKeyRoutesFailClosed(t *testing.T) {
	prepareEmpDept := func(t *testing.T, mock *MockOptimizer) {
		t.Helper()
		emp := mock.ctxt.tables["emp"]
		dept := mock.ctxt.tables["dept"]
		require.NotEmpty(t, emp.Fkeys)
		emp.TblId = 88887
		emp.Fkeys[0].ForeignTbl = dept.TblId
		emp.Fkeys[0].ForeignCols = []uint64{0}
		dept.RefChildTbls = []uint64{emp.TblId}
		mock.ctxt.id2name[emp.TblId] = "emp"
	}

	t.Run("child insert remains on modern path", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(t, mock)
		logicPlan, err := runOneStmt(mock, t, "insert into emp(empno, ename, deptno) values (1, 'e', 2) returning empno, deptno")
		require.NoError(t, err)
		require.True(t, logicPlan.GetQuery().HasReturning)
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
	})

	t.Run("referenced parent update rejects legacy path", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(t, mock)
		_, err := runOneStmt(mock, t, "update dept set deptno = 2 returning deptno")
		require.ErrorContains(t, err, "DML RETURNING does not support legacy UPDATE path")
	})

	t.Run("referenced parent delete rejects legacy path", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(t, mock)
		_, err := runOneStmt(mock, t, "delete from dept where deptno = 1 returning deptno")
		require.ErrorContains(t, err, "DML RETURNING does not support legacy DELETE path")
	})
}

func TestReturningFallbackFeature(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, "Iceberg table", returningFallbackFeature(
		moerr.NewUnsupportedDML(ctx, icebergRowLevelDMLUnsupportedCause), "legacy INSERT path",
	))
	require.Equal(t, "external table", returningFallbackFeature(
		moerr.NewUnsupportedDML(ctx, externalTableUnsupportedDMLCause), "legacy INSERT path",
	))
	require.Equal(t, "external table", returningFallbackFeature(
		moerr.NewInvalidInput(ctx, "cannot insert/update/delete from external table"), "legacy INSERT path",
	))
	require.Equal(t, "legacy INSERT path", returningFallbackFeature(
		moerr.NewUnsupportedDML(ctx, foreignKeyUnsupportedDMLCause), "legacy INSERT path",
	))
	require.Empty(t, returningFallbackFeature(moerr.NewInvalidInput(ctx, "other"), "legacy INSERT path"))
	require.Equal(t, "primary-key UPDATE on synchronous full-text/vector index", returningUpdatePlannerFeature(
		newUpdatePlannerRouteError(
			updatePlannerRejected,
			updateRouteReasonIrregularIndex,
			moerr.NewUnsupportedDML(ctx, "update primary key on a table with a synchronous full-text/vector index"),
		),
	))
	require.Empty(t, returningUpdatePlannerFeature(
		newLegacyUpdatePlannerRouteError(updateRouteReasonIrregularIndex, moerr.NewUnsupportedDML(ctx, "legacy")),
	))
}
