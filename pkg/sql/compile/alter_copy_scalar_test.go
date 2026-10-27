// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func scalarAlterCopyTestPlan() *plan.AlterTable {
	id := &plan.ColDef{Name: "id", ColId: 1, Typ: plan.Type{Id: int32(types.T_int32)}}
	added := &plan.ColDef{Name: "d", Typ: plan.Type{Id: int32(types.T_int32)}}
	return &plan.AlterTable{AlgorithmType: plan.AlterTable_COPY, Database: "db",
		TableDef:     &plan.TableDef{Name: "t", TableType: catalog.SystemOrdinaryRel, Cols: []*plan.ColDef{id}},
		CopyTableDef: &plan.TableDef{Name: "replica", TableType: catalog.SystemOrdinaryRel, Cols: []*plan.ColDef{plan2.DeepCopyColDef(id), added}},
		Actions:      []*plan.AlterTable_Action{{Action: &plan.AlterTable_Action_AddColumn{AddColumn: &plan.AlterAddColumn{Name: "d", Type: added.Typ, Pos: 1, PreName: "id"}}}}}
}

func TestScalarAlterCopyCertificateRequiresCompleteMatchingPlan(t *testing.T) {
	for name, mutate := range map[string]func(*plan.AlterTable){
		"ordinary":               func(*plan.AlterTable) {},
		"empty":                  func(q *plan.AlterTable) { q.Actions = nil },
		"nil_action":             func(q *plan.AlterTable) { q.Actions[0] = nil },
		"nil_payload":            func(q *plan.AlterTable) { q.Actions[0].Action = &plan.AlterTable_Action_AddColumn{} },
		"nil_oneof":              func(q *plan.AlterTable) { q.Actions[0].Action = (*plan.AlterTable_Action_AddColumn)(nil) },
		"unknown":                func(q *plan.AlterTable) { q.Actions[0].Action = &plan.AlterTable_Action_AlterReplaceDef{} },
		"name":                   func(q *plan.AlterTable) { q.Actions[0].GetAddColumn().Name = "other" },
		"type":                   func(q *plan.AlterTable) { q.Actions[0].GetAddColumn().Type.AutoIncr = true },
		"position":               func(q *plan.AlterTable) { q.Actions[0].GetAddColumn().Pos = 0 },
		"predecessor":            func(q *plan.AlterTable) { q.Actions[0].GetAddColumn().PreName = "other" },
		"extra":                  func(q *plan.AlterTable) { q.Actions = append(q.Actions, q.Actions[0]) },
		"replaced_source_column": func(q *plan.AlterTable) { q.CopyTableDef.Cols[0].ColId = 999 },
		"modified_source_column": func(q *plan.AlterTable) { q.CopyTableDef.Cols[0].Comment = "new" },
		"primary":                func(q *plan.AlterTable) { q.CopyTableDef.Cols[1].Primary = true },
		"allocator":              func(q *plan.AlterTable) { q.CopyTableDef.Cols[1].Typ.AutoIncr = true },
		"unique":                 func(q *plan.AlterTable) { q.CopyTableDef.Cols[1].Unique = true },
		"index":                  func(q *plan.AlterTable) { q.CopyTableDef.Indexes = []*plan.IndexDef{{IndexName: "idx"}} },
		"partition":              func(q *plan.AlterTable) { q.CopyTableDef.Partition = &plan.Partition{} },
		"fk":                     func(q *plan.AlterTable) { q.CopyTableDef.Fkeys = []*plan.ForeignKeyDef{{}} },
		"checks":                 func(q *plan.AlterTable) { q.TableDef.Checks = []*plan.CheckDef{{}} },
		"generated":              func(q *plan.AlterTable) { q.CopyTableDef.Cols[1].GeneratedCol = &plan.GeneratedCol{} },
		"fk_repair":              func(q *plan.AlterTable) { q.UpdateFkSqls = []string{"repair"} },
		"charset":                func(q *plan.AlterTable) { q.CopyTableDef.DefaultCharset = 99 },
		"nil_definition":         func(q *plan.AlterTable) { q.TableDef.Defs = []*plan.TableDef_DefType{nil} },
		"shared": func(q *plan.AlterTable) {
			q.TableDef.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.PropFromPublication, Value: "true"}}}}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := scalarAlterCopyTestPlan()
			mutate(q)
			require.Equal(t, name == "ordinary", scalarAlterCopyPlan(q, q.TableDef))
		})
	}
	for _, allocator := range []bool{false, true} {
		q := scalarAlterCopyTestPlan()
		q.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
		q.CopyTableDef.Pkey = plan2.DeepCopyPrimaryKeyDef(q.TableDef.Pkey)
		q.TableDef.Cols[0].Primary, q.CopyTableDef.Cols[0].Primary = true, true
		q.TableDef.Cols[0].Typ.AutoIncr, q.CopyTableDef.Cols[0].Typ.AutoIncr = allocator, allocator
		require.True(t, scalarAlterCopyPlan(q, q.TableDef))
	}
}

func TestScalarAlterCopyCreateAuthorityBeforeDisabledLocks(t *testing.T) {
	c := newAlterCopyPrecheckCompile(t, gomock.NewController(t), &alterCopyInsertSpyExecutor{})
	c.disableLock = true
	c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 7)
	q := scalarAlterCopyTestPlan()
	create := &plan.CreateTable{Database: "db", TableDef: q.CopyTableDef}
	wrap := func(q *plan.CreateTable) *plan.Plan {
		return &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: q}}}}
	}
	original := c.proc.Ctx
	var escaped context.Context
	require.NoError(t, c.withScalarAlterCopyCreate("db", q.CopyTableDef, func() error {
		escaped = c.proc.Ctx
		_, err := c.prepareCreateNamePlan(wrap(create))
		require.NoError(t, err)
		for _, bad := range []*plan.CreateTable{
			{Database: "other", TableDef: create.TableDef},
			{Database: "db", TableDef: &plan.TableDef{Name: "other"}},
			{Database: "db", TableDef: create.TableDef, UpdateFkSqls: []string{"repair"}},
			{Database: "db", TableDef: create.TableDef, CreateAsSelectSql: "select 1"},
			{Database: "db", TableDef: create.TableDef, IndexTables: []*plan.TableDef{{Name: "hidden"}}},
		} {
			_, err := c.prepareCreateNamePlan(wrap(bad))
			require.Error(t, err)
		}
		for _, name := range []string{"id", catalog.FakePrimaryKeyColName, "unknown"} {
			bad := &plan.CreateTable{Database: "db", TableDef: plan2.DeepCopyTableDef(create.TableDef, true)}
			bad.TableDef.Cols = append(bad.TableDef.Cols, &plan.ColDef{Name: name, Hidden: true})
			_, err := c.prepareCreateNamePlan(wrap(bad))
			require.Error(t, err)
		}
		owner := c.proc.Base.TxnOperator
		c.proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		_, err = c.prepareCreateNamePlan(wrap(create))
		require.Error(t, err)
		c.proc.Base.TxnOperator = owner
		c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 8)
		_, err = c.prepareCreateNamePlan(wrap(create))
		require.Error(t, err)
		return nil
	}))
	require.Same(t, original, c.proc.Ctx)
	c.proc.Ctx = escaped
	_, err := c.prepareCreateNamePlan(wrap(create))
	require.Error(t, err)
	c.proc.Ctx = original
	require.PanicsWithValue(t, "injected", func() {
		_ = c.withScalarAlterCopyCreate("db", q.CopyTableDef, func() error { panic("injected") })
	})
	require.Same(t, original, c.proc.Ctx)
}

func TestScalarAlterCopyReplicaIdentity(t *testing.T) {
	for _, mode := range []string{"owned", "public", "foreign_generation", "database", "logical"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = op
			created := map[uint64]bool{2: true}
			if mode == "foreign_generation" {
				created = nil
			}
			op.EXPECT().GetWorkspace().Return(creationViewWorkspace{created: created}).AnyTimes()
			rel := mock_frontend.NewMockRelation(ctrl)
			id, dbID, logicalID := uint64(2), uint64(9), uint64(1)
			switch mode {
			case "public":
				id = 1
			case "database":
				dbID = 10
			case "logical":
				logicalID = 3
			}
			rel.EXPECT().GetTableID(gomock.Any()).Return(id).AnyTimes()
			rel.EXPECT().GetDBID(gomock.Any()).Return(dbID).AnyTimes()
			rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{TblId: id, LogicalId: logicalID}).AnyTimes()
			err := (&Compile{proc: proc}).validateScalarAlterCopyReplica(rel, 1, 9, 1)
			if mode == "owned" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestScalarAlterCopyImplicitNullDefaultNormalization(t *testing.T) {
	a := &plan.ColDef{Name: "extra", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}}
	b := plan2.DeepCopyColDef(a)
	b.Default.OriginString = "null"
	b.Default.Expr = &plan.Expr{Typ: a.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	require.True(t, sameAlterCopyColumn(a, b))
	b.Default.Expr.GetLit().Isnull = false
	b.Default.Expr.GetLit().Value = &plan.Literal_Sval{Sval: "null"}
	require.False(t, sameAlterCopyColumn(a, b), "the string 'null' is not an implicit NULL default")
	b = plan2.DeepCopyColDef(a)
	b.Default.NullAbility = false
	require.False(t, sameAlterCopyColumn(a, b))
}
