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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
)

func TestClassifyUpdatePlannerError(t *testing.T) {
	ctx := context.Background()
	baseErr := moerr.NewUnsupportedDML(ctx, "multi-table update")
	typedErr := newLegacyUpdatePlannerRouteError(
		updateRouteReasonMultiTarget,
		baseErr,
	)
	require.Equal(t, baseErr.Error(), typedErr.Error())
	require.ErrorIs(t, typedErr, baseErr)

	tests := []struct {
		name       string
		err        error
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
		wantErr    error
	}{
		{
			name:       "typed legacy route",
			err:        typedErr,
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonMultiTarget,
			wantErr:    baseErr,
		},
		{
			name:       "raw unsupported dml is unknown",
			err:        moerr.NewUnsupportedDML(ctx, "new shared helper route"),
			wantRoute:  updatePlannerUnknown,
			wantReason: updateRouteReasonUnknown,
		},
		{
			name:       "ordinary binder error is rejected",
			err:        moerr.NewInternalError(ctx, "binder failed"),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonBinderError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route, reason, gotErr := classifyUpdatePlannerError(test.err)
			require.Equal(t, test.wantRoute, route)
			require.Equal(t, test.wantReason, reason)
			if test.wantErr != nil {
				require.Same(t, test.wantErr, gotErr)
			} else {
				require.Same(t, test.err, gotErr)
			}
		})
	}
}

func TestClassifyUpdateTableResolutionError(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(
		ctx.GetContext(),
		dialect.MYSQL,
		"UPDATE nation SET n_name = 'x'",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	updateStmt := stmt.(*tree.Update)

	tests := []struct {
		name       string
		err        error
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
		wantText   string
	}{
		{
			name: "foreign key enters temporary legacy allowlist",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				foreignKeyUnsupportedDMLCause,
			),
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonForeignKey,
			wantText:   foreignKeyUnsupportedDMLMsg,
		},
		{
			name: "iceberg uses specialized planner",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				icebergRowLevelDMLUnsupportedCause,
			),
			wantRoute:  updatePlannerSpecialized,
			wantReason: updateRouteReasonIceberg,
			wantText:   icebergRowLevelDMLUnsupportedMsg,
		},
		{
			name: "writable external update is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				externalTableUnsupportedDMLCause,
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonExternalTable,
			wantText:   "invalid input: cannot insert/update/delete from external table",
		},
		{
			name: "unsupported table form is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				"unsupported table type",
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonTableForm,
			wantText:   "unsupported DML: unsupported table type",
		},
		{
			name: "empty table name is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				"empty table name",
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonEmptyTableName,
			wantText:   "unsupported DML: empty table name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classifiedErr := classifyUpdateTableResolutionError(ctx, updateStmt, test.err)
			route, reason, gotErr := classifyUpdatePlannerError(classifiedErr)
			require.Equal(t, test.wantRoute, route)
			require.Equal(t, test.wantReason, reason)
			require.Equal(t, test.wantText, gotErr.Error())
		})
	}
}

func TestBindUpdateProducesTypedPlannerRoutes(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		prepare    func(*MockOptimizer)
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
	}{
		{
			name:       "multi target",
			sql:        "UPDATE emp, dept SET emp.sal = 1, dept.loc = 'x'",
			wantRoute:  updatePlannerModern,
			wantReason: updateRouteReasonNone,
		},
		{
			name: "irregular index column",
			sql:  "UPDATE nation SET n_comment = 'x'",
			prepare: func(mock *MockOptimizer) {
				mock.ctxt.tables["nation"].Indexes = []*planpb.IndexDef{{
					IndexName:          "idx",
					IndexTableName:     "idx_entries",
					IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
					IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					Parts:              []string{"n_comment"},
					TableExist:         true,
				}}
			},
			wantRoute:  updatePlannerModern,
			wantReason: updateRouteReasonNone,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			if test.prepare != nil {
				test.prepare(mock)
			}

			stmt, err := parsers.ParseOne(
				mock.CurrentContext().GetContext(),
				dialect.MYSQL,
				test.sql,
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
			bindCtx := NewBindContext(builder, nil)
			_, err = builder.bindUpdate(stmt.(*tree.Update), bindCtx)
			if test.wantRoute == updatePlannerModern {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)

			route, reason, _ := classifyUpdatePlannerError(err)
			require.Equal(t, test.wantRoute, route, "bind error: %v", err)
			require.Equal(t, test.wantReason, reason, "bind error: %v", err)
		})
	}
}

func TestMultiTableUpdateNeverFallsBackToLegacyPlanner(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"UPDATE emp, dept SET emp.sal = 1, dept.loc = 'x'",
	)
	require.NoError(t, err)

	multiUpdates := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_MULTI_UPDATE {
			multiUpdates++
		}
	}
	require.Equal(t, 1, multiUpdates)
}

func TestBindUpdateForeignKeyRoutingByAffectedColumns(t *testing.T) {
	prepareEmpDept := func(mock *MockOptimizer) {
		emp := mock.ctxt.tables["emp"]
		dept := mock.ctxt.tables["dept"]
		require.NotEmpty(t, emp.Fkeys)
		emp.TblId = 88887
		emp.Fkeys[0].ForeignTbl = dept.TblId
		emp.Fkeys[0].ForeignCols = []uint64{0}
		dept.RefChildTbls = []uint64{emp.TblId}
		mock.ctxt.id2name[emp.TblId] = "emp"
	}
	disableForeignKeyChecks := func(mock *MockOptimizer) {
		mock.ctxt.SetContext(context.WithValue(
			mock.ctxt.GetContext(),
			defines.DisableFkCheck{},
			true,
		))
	}

	t.Run("unrelated child column uses multi update without parent probe", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET sal = 1")
		require.NoError(t, err)

		hasMultiUpdate := false
		hasParentScan := false
		parentID := mock.ctxt.tables["dept"].TblId
		for _, node := range logicPlan.GetQuery().Nodes {
			hasMultiUpdate = hasMultiUpdate || node.NodeType == planpb.Node_MULTI_UPDATE
			hasParentScan = hasParentScan ||
				(node.NodeType == planpb.Node_TABLE_SCAN &&
					node.TableDef != nil &&
					node.TableDef.TblId == parentID)
		}
		require.True(t, hasMultiUpdate)
		require.False(t, hasParentScan)
	})

	t.Run("nullable unique update on child table avoids legacy preinsert", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET ename = 'x'")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 0, countUpdateFkPlanNodes(query, planpb.Node_PRE_INSERT_UK))
		require.Equal(t, 0, countUpdateFkMarkJoins(query))
	})

	t.Run("affected child column uses multi update with parent mark join", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET deptno = 2")
		require.NoError(t, err)

		hasMultiUpdate := false
		hasParentScan := false
		hasMarkJoin := false
		hasAssert := false
		parentID := mock.ctxt.tables["dept"].TblId
		for _, node := range logicPlan.GetQuery().Nodes {
			hasMultiUpdate = hasMultiUpdate || node.NodeType == planpb.Node_MULTI_UPDATE
			hasParentScan = hasParentScan ||
				(node.NodeType == planpb.Node_TABLE_SCAN &&
					node.TableDef != nil &&
					node.TableDef.TblId == parentID)
			hasMarkJoin = hasMarkJoin ||
				(node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_MARK)
			for _, filter := range node.FilterList {
				if filter.GetF() != nil && filter.GetF().Func.ObjName == "assert" {
					hasAssert = true
				}
			}
		}
		require.True(t, hasMultiUpdate)
		require.True(t, hasParentScan)
		require.True(t, hasMarkJoin)
		require.True(t, hasAssert)
		require.True(t, updateFkPlanContainsTypedAssert(
			logicPlan.GetQuery(),
			foreignKeyNoReferencedRowAssert,
		))
	})

	t.Run("irregular index child foreign key update keeps typed error", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		mock.ctxt.tables["emp"].Indexes = []*planpb.IndexDef{{
			IndexName: "idx",
			IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
			Parts:     []string{"sal"},
		}}

		logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET deptno = 2, sal = 1")
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 1, countUpdateFkAsserts(query))
		require.True(t, updateFkPlanScansTable(query, mock.ctxt.tables["dept"].TblId))
		require.True(t, updateFkPlanContainsTypedAssert(
			query,
			foreignKeyNoReferencedRowAssert,
		))
	})

	t.Run("auto increment child key uses modern final row validation", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		emp := mock.ctxt.tables["emp"]
		for _, col := range emp.Cols {
			if col.Name == "deptno" {
				col.Typ.AutoIncr = true
			}
		}

		stmt, err := parsers.ParseOne(
			mock.CurrentContext().GetContext(),
			dialect.MYSQL,
			"UPDATE emp SET deptno = if(empno = 1, null, deptno)",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
		_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkPlanNodes(builder.qry, planpb.Node_PRE_INSERT))
		require.Equal(t, 1, countUpdateFkAsserts(builder.qry))
	})

	t.Run("disabled checks keep auto increment child key on modern route", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		disableForeignKeyChecks(mock)
		emp := mock.ctxt.tables["emp"]
		for _, col := range emp.Cols {
			if col.Name == "deptno" {
				col.Typ.AutoIncr = true
			}
		}

		logicPlan, err := runOneStmt(
			mock,
			t,
			"UPDATE emp SET deptno = if(empno = 1, null, deptno)",
		)
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		require.True(t, query.GetHasForeignKeyAction())
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_PRE_INSERT))
		require.Equal(t, 0, countUpdateFkMarkJoins(query))
	})

	t.Run("multi target auto increment child key stays modern", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		for _, col := range mock.ctxt.tables["emp"].Cols {
			if col.Name == "deptno" {
				col.Typ.AutoIncr = true
			}
		}

		logicPlan, err := runOneStmt(mock, t,
			"UPDATE emp, dept SET emp.deptno = DEFAULT, dept.loc = 'changed' "+
				"WHERE emp.deptno = dept.deptno")
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_PRE_INSERT))
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
	})

	t.Run("affected restricted parent key stays modern with child probe", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)

		logicPlan, err := runOneStmt(mock, t, "UPDATE dept SET deptno = 2")
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 1, countUpdateFkMarkJoins(query))
		require.Equal(t, 1, countUpdateFkAsserts(query))
		require.True(t, updateFkPlanContainsFunc(query, "<=>"))
		require.True(t, updateFkPlanContainsTypedAssert(
			query,
			foreignKeyRowIsReferencedAssert,
		))
	})

	t.Run("set default preserves restrict compatibility on modern path", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		mock.ctxt.tables["emp"].Fkeys[0].OnUpdate = planpb.ForeignKeyDef_SET_DEFAULT

		logicPlan, err := runOneStmt(mock, t, "UPDATE dept SET deptno = 2")
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 1, countUpdateFkMarkJoins(query))
		require.Equal(t, 1, countUpdateFkAsserts(query))
	})

	for _, test := range []struct {
		name   string
		action planpb.ForeignKeyDef_RefAction
	}{
		{name: "cascade parent key builds child multi update", action: planpb.ForeignKeyDef_CASCADE},
		{name: "set null parent key builds child multi update", action: planpb.ForeignKeyDef_SET_NULL},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			prepareEmpDept(mock)
			emp := mock.ctxt.tables["emp"]
			emp.Fkeys[0].OnUpdate = test.action

			logicPlan, err := runOneStmt(mock, t, "UPDATE dept SET deptno = 2")
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.Equal(t, 2, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
			require.True(t, query.HasForeignKeyAction)
			require.True(t, updateFkPlanContainsFunc(query, "<=>"))
			hasIndexedParentUpdate := false
			for _, node := range query.Nodes {
				if node.NodeType == planpb.Node_MULTI_UPDATE && len(node.UpdateCtxList) > 1 {
					hasIndexedParentUpdate = true
				}
				if node.NodeType != planpb.Node_MULTI_UPDATE {
					continue
				}
				if len(node.UpdateCtxList) > 0 && node.UpdateCtxList[0].ObjRef.ObjName == "emp" {
					require.Len(t, node.UpdateCtxList, 1,
						"cascade must not rewrite child indexes whose key parts are unchanged")
					for _, updateCtx := range node.UpdateCtxList {
						require.True(t, updateCtx.IgnoreAffectedRows)
					}
				}
				if len(node.UpdateCtxList) > 0 && node.UpdateCtxList[0].ObjRef.ObjName == "dept" {
					for _, updateCtx := range node.UpdateCtxList {
						require.False(t, updateCtx.IgnoreAffectedRows)
					}
				}
			}
			require.True(t, hasIndexedParentUpdate)
		})
	}

	for _, test := range []struct {
		name    string
		prepare func(*MockOptimizer)
	}{
		{
			name: "generated unique collision requires complete child row closure",
			prepare: func(mock *MockOptimizer) {
				setMockGeneratedColumn(t, mock, "emp", "sal", "deptno")
				mock.ctxt.tables["emp"].Indexes = append(mock.ctxt.tables["emp"].Indexes, &planpb.IndexDef{
					IndexName: "uk_sal", Parts: []string{"sal"}, Unique: true,
				})
			},
		},
		{
			name: "parent child width mismatch requires assignment cast",
			prepare: func(mock *MockOptimizer) {
				requireMockColumn(t, mock, "emp", "deptno").Typ.Width = 16
			},
		},
		{
			name: "composite cluster by dependency requires recomputation",
			prepare: func(mock *MockOptimizer) {
				emp := mock.ctxt.tables["emp"]
				emp.ClusterBy = &planpb.ClusterByDef{
					Name: planutil.BuildCompositeClusterByColumnName([]string{"deptno", "empno"}),
				}
			},
		},
		{
			name: "on update feeding generated column requires dependency closure",
			prepare: func(mock *MockOptimizer) {
				setMockOnUpdateExpr(t, mock, "emp", "job", "changed")
				setMockGeneratedColumn(t, mock, "emp", "ename", "job")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			prepareEmpDept(mock)
			mock.ctxt.tables["emp"].Fkeys[0].OnUpdate = planpb.ForeignKeyDef_CASCADE
			test.prepare(mock)

			stmt, err := parsers.ParseOne(
				mock.CurrentContext().GetContext(),
				dialect.MYSQL,
				"UPDATE dept SET deptno = 2",
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
			_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
			require.ErrorContains(t, err, "complete child update row closure")
			route, reason, _ := classifyUpdatePlannerError(err)
			require.Equal(t, updatePlannerRejected, route)
			require.Equal(t, updateRouteReasonForeignKey, reason)
		})
	}

	t.Run("disabled checks skip child probe and parent fallback", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		mock.ctxt.SetContext(context.WithValue(
			mock.ctxt.GetContext(),
			defines.DisableFkCheck{},
			true,
		))

		for _, sql := range []string{
			"UPDATE emp SET deptno = 2",
			"UPDATE dept SET deptno = 2",
		} {
			logicPlan, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			require.Equal(t, 0, countUpdateFkMarkJoins(logicPlan.GetQuery()))
			require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
		}
	})

	t.Run("unrelated referenced parent column stays modern", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)

		logicPlan, err := runOneStmt(mock, t, "UPDATE dept SET loc = 'x'")
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
		require.Equal(t, 0, countUpdateFkMarkJoins(logicPlan.GetQuery()))
	})

	t.Run("only affected constraint is probed", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		emp := mock.ctxt.tables["emp"]
		emp.Fkeys = append(emp.Fkeys, &planpb.ForeignKeyDef{
			Name:        "fk_mgr",
			Cols:        []uint64{3},
			ForeignTbl:  mock.ctxt.tables["dept"].TblId,
			ForeignCols: []uint64{0},
		})

		logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET deptno = 2")
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkMarkJoins(logicPlan.GetQuery()))
		require.Equal(t, 1, countUpdateFkAsserts(logicPlan.GetQuery()))
		require.True(t, updateFkPlanContainsTypedAssert(
			logicPlan.GetQuery(),
			foreignKeyNoReferencedRowAssert,
		))
		require.True(t, updateFkPlanContainsFunc(logicPlan.GetQuery(), "<=>"))
	})

	t.Run("parent cascade changing another child foreign key is rejected", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		emp := mock.ctxt.tables["emp"]
		emp.Fkeys[0].OnUpdate = planpb.ForeignKeyDef_CASCADE
		otherParent := mock.ctxt.tables["nation"]
		emp.Fkeys = append(emp.Fkeys, &planpb.ForeignKeyDef{
			Name:        "fk_emp_other_parent",
			Cols:        []uint64{7},
			ForeignTbl:  otherParent.TblId,
			ForeignCols: []uint64{0},
		})

		stmt, err := parsers.ParseOne(
			mock.CurrentContext().GetContext(),
			dialect.MYSQL,
			"UPDATE dept SET deptno = 2",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
		_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.ErrorContains(t, err, "child column constrained by another foreign key")
		route, _, _ := classifyUpdatePlannerError(err)
		require.Equal(t, updatePlannerRejected, route)
	})
}

func TestBindUpdateParentForeignKeySafetyGates(t *testing.T) {
	prepareEmpDept := func(mock *MockOptimizer, action planpb.ForeignKeyDef_RefAction) {
		setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_RESTRICT, action)
	}

	for _, action := range []planpb.ForeignKeyDef_RefAction{
		planpb.ForeignKeyDef_RESTRICT,
		planpb.ForeignKeyDef_CASCADE,
		planpb.ForeignKeyDef_SET_NULL,
	} {
		t.Run("optimistic "+action.String(), func(t *testing.T) {
			mock := NewMockOptimizer(true)
			prepareEmpDept(mock, action)
			setMockTxnMode(mock, txnpb.TxnMode_Optimistic)

			stmt, err := parsers.ParseOne(
				mock.CurrentContext().GetContext(), dialect.MYSQL,
				"UPDATE dept SET deptno = 2", 1)
			require.NoError(t, err)
			defer stmt.Free()

			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
			_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
			require.ErrorContains(t, err, "optimistic transaction")
			route, reason, _ := classifyUpdatePlannerError(err)
			require.Equal(t, updatePlannerRejected, route)
			require.Equal(t, updateRouteReasonForeignKey, reason)
		})
	}

	for _, test := range []struct {
		name        string
		action      planpb.ForeignKeyDef_RefAction
		windowCount int
		ambiguous   bool
	}{
		{name: "cascade guards distinct child outputs", action: planpb.ForeignKeyDef_CASCADE, windowCount: 2, ambiguous: true},
		{name: "set null only deduplicates child identity", action: planpb.ForeignKeyDef_SET_NULL, windowCount: 1},
	} {
		t.Run("non unique referenced prefix "+test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			prepareEmpDept(mock, test.action)
			dept := mock.ctxt.tables["dept"]
			dept.Pkey = &planpb.PrimaryKeyDef{
				Names: []string{"deptno", "dname"}, PkeyColName: "deptno",
			}

			stmt, err := parsers.ParseOne(
				mock.CurrentContext().GetContext(), dialect.MYSQL,
				"UPDATE dept SET deptno = 2", 1)
			require.NoError(t, err)
			defer stmt.Free()

			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
			_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
			require.NoError(t, err)
			require.Equal(t, test.ambiguous, updateFkPlanContainsTypedAssert(
				builder.qry, foreignKeyAmbiguousMappingAssert))
			windowCount := 0
			for _, node := range builder.qry.Nodes {
				if node.NodeType == planpb.Node_WINDOW {
					windowCount++
				}
			}
			require.Equal(t, test.windowCount, windowCount)
		})
	}
}

func TestBindUpdateForeignKeySensitivityIncludesImplicitFinalRowChanges(t *testing.T) {
	for _, checksDisabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "checks enabled", true: "checks disabled"}[checksDisabled], func(t *testing.T) {
			mock := NewMockOptimizer(true)
			setMockEmpDeptForeignKeyAction(
				t, mock, planpb.ForeignKeyDef_RESTRICT, planpb.ForeignKeyDef_RESTRICT)
			setMockOnUpdateExpr(t, mock, "emp", "deptno", "2")
			if checksDisabled {
				mock.ctxt.SetContext(context.WithValue(
					mock.ctxt.GetContext(), defines.DisableFkCheck{}, true))
			}

			logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET sal = sal + 1")
			require.NoError(t, err)
			require.True(t, logicPlan.GetQuery().HasForeignKeyAction,
				"implicit ON UPDATE FK column must make prepared plans cache-sensitive")
		})
	}
}

func TestSortForeignKeyLockTargetsUsesBaseBeforeHiddenTables(t *testing.T) {
	base := &planpb.LockTarget{TableId: 20, ObjRef: &planpb.ObjectRef{ObjName: "z_parent"}}
	hidden := &planpb.LockTarget{TableId: 10, ObjRef: &planpb.ObjectRef{ObjName: "__mo_index_a"}}
	targets := []*planpb.LockTarget{hidden, base}

	sortForeignKeyLockTargets(targets, map[uint64]struct{}{base.TableId: {}})
	require.Same(t, base, targets[0])
	require.Same(t, hidden, targets[1])
}

func TestUpdateParentForeignKeyLocksPrecedeChildConsumers(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_RESTRICT, planpb.ForeignKeyDef_CASCADE)
	child := mock.ctxt.tables["emp"]
	child.Indexes = append(child.Indexes, &planpb.IndexDef{
		IndexName: "building_idx", IndexTableName: "missing_building_idx",
		Parts: []string{"ename"}, TableExist: false,
		IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
	})

	logicPlan, err := runOneStmt(mock, t, "update dept set deptno = deptno + 10 where deptno = 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.Contains(t, query.DetectSqls, "UPDATE_PARENT_PLAN:")
	parentID := mock.ctxt.tables["dept"].TblId
	childID := child.TblId

	contains := func(root, target int32) bool {
		var visit func(int32) bool
		visit = func(nodeID int32) bool {
			if nodeID == target {
				return true
			}
			for _, childNodeID := range query.Nodes[nodeID].Children {
				if visit(childNodeID) {
					return true
				}
			}
			return false
		}
		return visit(root)
	}
	stepContaining := func(target int32) int {
		for step, root := range query.Steps {
			if contains(root, target) {
				return step
			}
		}
		return -1
	}
	var stepDependsOn func(int, int, map[int]bool) bool
	stepDependsOn = func(step, dependency int, visited map[int]bool) bool {
		if step == dependency {
			return true
		}
		if visited[step] {
			return false
		}
		visited[step] = true
		var nodeDependsOn func(int32) bool
		nodeDependsOn = func(nodeID int32) bool {
			for _, sourceStep := range query.Nodes[nodeID].SourceStep {
				if stepDependsOn(int(sourceStep), dependency, visited) {
					return true
				}
			}
			for _, childNodeID := range query.Nodes[nodeID].Children {
				if nodeDependsOn(childNodeID) {
					return true
				}
			}
			return false
		}
		return nodeDependsOn(query.Steps[step])
	}

	lockNodeID := int32(-1)
	for nodeID, node := range query.Nodes {
		for _, target := range node.LockTargets {
			if target.TableId == parentID && target.Mode == lockpb.LockMode_Exclusive {
				lockNodeID = int32(nodeID)
				require.Len(t, node.Children, 1)
				lockInput := query.Nodes[node.Children[0]]
				require.Less(t, int(target.PrimaryColIdxInBat), len(lockInput.ProjectList))
				assert.Equal(t, target.PrimaryColTyp.Id,
					lockInput.ProjectList[target.PrimaryColIdxInBat].Typ.Id)
				break
			}
		}
		if lockNodeID >= 0 {
			break
		}
	}
	require.NotEqual(t, int32(-1), lockNodeID)
	lockStep := stepContaining(lockNodeID)
	require.GreaterOrEqual(t, lockStep, 0)
	assert.Equal(t, planpb.Node_SINK, query.Nodes[query.Steps[lockStep]].NodeType)

	foundChildConsumer := false
	for nodeID, node := range query.Nodes {
		if node.NodeType != planpb.Node_TABLE_SCAN || node.TableDef == nil || node.TableDef.TblId != childID {
			continue
		}
		foundChildConsumer = true
		consumerStep := stepContaining(int32(nodeID))
		require.GreaterOrEqual(t, consumerStep, 0)
		assert.True(t, stepDependsOn(consumerStep, lockStep, make(map[int]bool)),
			"every child scan must depend on the materialized parent-key lock step")
	}
	assert.True(t, foundChildConsumer)
}

func TestBindUpdateSelfReferencingForeignKeyRouting(t *testing.T) {
	prepareSelfRef := func(mock *MockOptimizer) {
		tableDef := mock.ctxt.tables["self_ref"]
		tableDef.RefChildTbls = []uint64{tableDef.TblId}
		mock.ctxt.id2name[tableDef.TblId] = "self_ref"
	}

	t.Run("unrelated column stays modern without probe", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareSelfRef(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE self_ref SET name = 'x'")
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
		require.Equal(t, 0, countUpdateFkMarkJoins(logicPlan.GetQuery()))
	})

	t.Run("child key uses self mark probe", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareSelfRef(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE self_ref SET parent_id = 2")
		require.NoError(t, err)
		require.Equal(t, 1, countUpdateFkPlanNodes(logicPlan.GetQuery(), planpb.Node_MULTI_UPDATE))
		require.Equal(t, 0, countUpdateFkMarkJoins(logicPlan.GetQuery()))
		require.Equal(t, 0, countUpdateFkAsserts(logicPlan.GetQuery()))
		require.NotEmpty(t, logicPlan.GetQuery().DetectSqls)
	})

	t.Run("restricted referenced key uses final self validation", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareSelfRef(mock)
		logicPlan, err := runOneStmt(mock, t, "UPDATE self_ref SET id = 2")
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.Equal(t, 1, countUpdateFkPlanNodes(query, planpb.Node_MULTI_UPDATE))
		require.Equal(t, 1, countUpdateFkMarkJoins(query))
		require.Equal(t, 1, countUpdateFkAsserts(query))
		require.NotEmpty(t, query.DetectSqls)
		require.True(t, updateFkPlanContainsFunc(query, "isnull"))
	})

	t.Run("self cascade uses typed legacy validation", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareSelfRef(mock)
		mock.ctxt.tables["self_ref"].Fkeys[0].OnUpdate = planpb.ForeignKeyDef_CASCADE
		stmt, err := parsers.ParseOne(
			mock.CurrentContext().GetContext(),
			dialect.MYSQL,
			"UPDATE self_ref SET id = 2",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
		_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.Error(t, err)
		route, reason, _ := classifyUpdatePlannerError(err)
		require.Equal(t, updatePlannerLegacy, route)
		require.Equal(t, updateRouteReasonForeignKey, reason)
	})

	t.Run("disabled checks omit self detect sql", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareSelfRef(mock)
		mock.ctxt.SetContext(context.WithValue(
			mock.ctxt.GetContext(),
			defines.DisableFkCheck{},
			true,
		))
		logicPlan, err := runOneStmt(mock, t, "UPDATE self_ref SET parent_id = 99")
		require.NoError(t, err)
		require.Empty(t, logicPlan.GetQuery().DetectSqls)
		require.Equal(t, 0, countUpdateFkMarkJoins(logicPlan.GetQuery()))
		require.Equal(t, 0, countUpdateFkAsserts(logicPlan.GetQuery()))
	})
}

func TestBindUpdateAutoIncrementRunsBeforeForeignKeys(t *testing.T) {
	prepareEmpDept := func(mock *MockOptimizer) {
		emp := mock.ctxt.tables["emp"]
		dept := mock.ctxt.tables["dept"]
		emp.TblId = 88887
		emp.Fkeys[0].ForeignTbl = dept.TblId
		emp.Fkeys[0].ForeignCols = []uint64{0}
		dept.RefChildTbls = []uint64{emp.TblId}
		mock.ctxt.id2name[emp.TblId] = "emp"
	}
	bindDirect := func(t *testing.T, mock *MockOptimizer, sql string) *planpb.Query {
		stmt, err := parsers.ParseOne(mock.CurrentContext().GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
		_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.NoError(t, err)
		return builder.qry
	}
	firstNode := func(query *planpb.Query, match func(*planpb.Node) bool) int {
		for i, node := range query.Nodes {
			if match(node) {
				return i
			}
		}
		return -1
	}

	t.Run("child generated key uses modern final row validation", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		for _, col := range mock.ctxt.tables["emp"].Cols {
			if col.Name == "deptno" {
				col.Typ.AutoIncr = true
			}
		}
		stmt, err := parsers.ParseOne(
			mock.CurrentContext().GetContext(),
			dialect.MYSQL,
			"UPDATE emp SET deptno = DEFAULT",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		query := bindDirect(t, mock, "UPDATE emp SET deptno = DEFAULT")
		preInsertPos := firstNode(query, func(node *planpb.Node) bool {
			return node.NodeType == planpb.Node_PRE_INSERT
		})
		require.NotEqual(t, -1, preInsertPos)
	})

	t.Run("parent action on generated key uses legacy planner", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		mock.ctxt.tables["dept"].Cols[0].Typ.AutoIncr = true
		mock.ctxt.tables["emp"].Fkeys[0].OnUpdate = planpb.ForeignKeyDef_CASCADE
		stmt, err := parsers.ParseOne(
			mock.CurrentContext().GetContext(),
			dialect.MYSQL,
			"UPDATE dept SET deptno = DEFAULT",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
		_, err = builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.ErrorContains(t, err, "auto-increment referenced key")
		route, reason, _ := classifyUpdatePlannerError(err)
		require.Equal(t, updatePlannerLegacy, route)
		require.Equal(t, updateRouteReasonForeignKey, reason)
	})

	t.Run("disabled checks preserve pre-insert input schema", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		prepareEmpDept(mock)
		mock.ctxt.tables["emp"].Cols[0].Typ.AutoIncr = true
		mock.ctxt.SetContext(context.WithValue(
			mock.ctxt.GetContext(),
			defines.DisableFkCheck{},
			true,
		))

		query := bindDirect(t, mock, "UPDATE emp SET empno = DEFAULT")
		require.NotNil(t, query)
		require.NotEqual(t, -1, firstNode(query, func(node *planpb.Node) bool {
			return node.NodeType == planpb.Node_PRE_INSERT
		}))
	})
}

func TestLegacyInsertForeignKeyKeepsGenericAssert(t *testing.T) {
	mock := NewMockOptimizer(true)
	emp := mock.ctxt.tables["emp"]
	dept := mock.ctxt.tables["dept"]
	emp.TblId = 88887
	emp.Fkeys[0].ForeignTbl = dept.TblId
	emp.Fkeys[0].ForeignCols = []uint64{0}
	mock.ctxt.id2name[emp.TblId] = "emp"

	stmt, err := parsers.ParseOne(
		mock.CurrentContext().GetContext(),
		dialect.MYSQL,
		"INSERT INTO emp (empno, deptno) VALUES (1, 10)",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := buildInsert(stmt.(*tree.Insert), mock.CurrentContext(), false, false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.True(t, updateFkPlanContainsAssertWithArity(query, 2))
	require.False(t, updateFkPlanContainsTypedAssert(query, foreignKeyNoReferencedRowAssert))
}

func TestResolveSingleTablePreservesForeignKeyPolicy(t *testing.T) {
	mock := NewMockOptimizer(true)
	stmt, err := parsers.ParseOne(
		mock.CurrentContext().GetContext(),
		dialect.MYSQL,
		"UPDATE emp SET sal = 1",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	tableExpr := stmt.(*tree.Update).Tables[0]
	dmlCtx := NewDMLContext()
	require.Error(t, dmlCtx.ResolveSingleTable(
		mock.CurrentContext(),
		tableExpr,
		nil,
		map[string]bool{},
		false,
	))

	mock.ctxt.SetContext(context.WithValue(
		mock.ctxt.GetContext(),
		defines.DisableFkCheck{},
		true,
	))
	require.NoError(t, NewDMLContext().ResolveSingleTable(
		mock.CurrentContext(),
		tableExpr,
		nil,
		map[string]bool{},
		true,
	))
}

func countUpdateFkPlanNodes(query *planpb.Query, nodeType planpb.Node_NodeType) int {
	count := 0
	for _, node := range query.Nodes {
		if node.NodeType == nodeType {
			count++
		}
	}
	return count
}

func countUpdateFkMarkJoins(query *planpb.Query) int {
	count := 0
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_MARK {
			count++
		}
	}
	return count
}

func countUpdateFkAsserts(query *planpb.Query) int {
	count := 0
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			if filter.GetF() != nil && filter.GetF().Func.ObjName == "assert" {
				count++
			}
		}
	}
	return count
}

func updateFkPlanContainsTypedAssert(query *planpb.Query, errType string) bool {
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			if fn == nil || fn.Func.ObjName != "assert" || len(fn.Args) != 3 {
				continue
			}
			if lit := fn.Args[2].GetLit(); lit != nil && lit.GetSval() == errType {
				return true
			}
		}
	}
	return false
}

func updateFkPlanContainsAssertWithArity(query *planpb.Query, arity int) bool {
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			if fn != nil && fn.Func.ObjName == "assert" && len(fn.Args) == arity {
				return true
			}
		}
	}
	return false
}

func updateFkPlanScansTable(query *planpb.Query, tableID uint64) bool {
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.TblId == tableID {
			return true
		}
	}
	return false
}

func updateFkPlanContainsFunc(query *planpb.Query, name string) bool {
	var contains func(*planpb.Expr) bool
	contains = func(expr *planpb.Expr) bool {
		fn := expr.GetF()
		if fn == nil {
			return false
		}
		if fn.Func.ObjName == name {
			return true
		}
		for _, arg := range fn.Args {
			if contains(arg) {
				return true
			}
		}
		return false
	}

	for _, node := range query.Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
		} {
			for _, expr := range exprs {
				if contains(expr) {
					return true
				}
			}
		}
	}
	return false
}
