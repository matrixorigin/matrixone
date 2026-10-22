//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func m0TestPlanTable() *plan.TableDef {
	table := &plan.TableDef{Name: "customer", Version: 1, Name2ColIndex: make(map[string]int32)}
	for i := 0; i < 6; i++ {
		name := []string{"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance"}[i]
		table.Cols = append(table.Cols, &plan.ColDef{
			Name: name, ColId: uint64(i + 1), Seqnum: uint32(i),
			Typ: plan.Type{Id: int32(types.T_int32)},
		})
		table.Name2ColIndex[name] = int32(i)
	}
	key := &plan.ColDef{
		Name: catalog.CPrimaryKeyColName, ColId: 7, Seqnum: 6, Hidden: true, Primary: true,
		Typ: plan.Type{Id: int32(types.T_varchar), Width: 32},
	}
	table.Cols = append(table.Cols, key)
	table.Name2ColIndex[key.Name] = 6
	appended := &plan.ColDef{
		Name: "tier", ColId: 8, Seqnum: 7,
		Typ:     plan.Type{Id: int32(types.T_varchar), Width: 8},
		Default: &plan.Default{NullAbility: true},
	}
	table.Cols = append(table.Cols, appended)
	table.Name2ColIndex[appended.Name] = 7
	rowid := MakeRowIdColDef()
	rowid.ColId = 9
	table.Cols = append(table.Cols, rowid)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: key.Name, CompPkeyCol: key}
	return table
}

func TestM0PlanAdmissionRejectsIdentitiesAndBackfill(t *testing.T) {
	table := m0TestPlanTable()
	base, err := m0PlanBase(table)
	require.NoError(t, err)
	require.Equal(t, uint32(7), base)

	permuted := DeepCopyTableDef(table, true)
	permuted.Cols[6].Seqnum, permuted.Cols[7].Seqnum = 7, 6
	_, err = m0PlanBase(permuted)
	require.Error(t, err)

	backfill := DeepCopyTableDef(table, true)
	backfill.Cols[7].Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "gold"}},
		}},
	}
	_, err = m0PlanBase(backfill)
	require.NoError(t, err)
	require.Error(t, validateM0BoundAddColumn(backfill.Cols[7]))
	require.True(t, M0AppendDomain(table))

	notNull := DeepCopyTableDef(table, true)
	notNull.Cols[7].Default.NullAbility = false
	require.Error(t, validateM0BoundAddColumn(notNull.Cols[7]))

	typedNotNull := DeepCopyTableDef(table, true)
	typedNotNull.Cols[7].Typ.NotNullable = true
	require.Error(t, validateM0BoundAddColumn(typedNotNull.Cols[7]))

	enumColumn := DeepCopyTableDef(table, true).Cols[7]
	enumColumn.Typ.Id = int32(types.T_enum)
	require.Error(t, validateM0BoundAddColumn(enumColumn))

	setColumn := DeepCopyTableDef(table, true).Cols[7]
	setColumn.Typ.Enumvalues = "a,b,"
	require.Error(t, validateM0BoundAddColumn(setColumn))
	commentColumn := DeepCopyTableDef(table, true).Cols[7]
	commentColumn.Comment = "kept"
	require.Error(t, validateM0BoundAddColumn(commentColumn))

	inboundFK := DeepCopyTableDef(table, true)
	inboundFK.RefChildTbls = []uint64{42}
	_, err = m0PlanBase(inboundFK)
	require.Error(t, err)
}

func TestM0FormalAddBuildsInplaceAddColumnAction(t *testing.T) {
	table := m0TestPlanTable()
	proc := newPlanTestProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true}).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		Mode:      txn.TxnMode_Pessimistic,
		Isolation: txn.TxnIsolation_RC,
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	mock := NewMockOptimizer(false, proc)
	mock.ctxt.tables[table.Name] = table
	mock.ctxt.objects[table.Name] = &ObjectRef{
		SchemaName: "constraint_test",
		ObjName:    table.Name,
	}

	logicPlan, err := buildSingleStmt(
		mock,
		t,
		"alter table constraint_test.customer add column credit_lim_t0_s0 decimal(10,2)",
	)
	require.NoError(t, err)
	alter := logicPlan.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.Equal(t, plan.AlterTable_INPLACE, alter.AlgorithmType)
	require.Len(t, alter.Actions, 1)
	add := alter.Actions[0].GetAddColumn()
	require.NotNil(t, add)
	require.Equal(t, "credit_lim_t0_s0", add.Name)
	require.Equal(t, int32(-1), add.Pos)

	_, err = buildSingleStmt(mock, t,
		"alter table constraint_test.customer add column commented int comment 'kept'")
	require.ErrorContains(t, err, "only supports metadata-only ADD COLUMN")

	_, err = buildSingleStmt(
		mock,
		t,
		"alter table constraint_test.customer comment='changed'",
	)
	require.ErrorContains(t, err, "only supports metadata-only ADD COLUMN")
}

func TestM0ClonePreservesInternalLayoutAndVersion(t *testing.T) {
	source := m0TestPlanTable()
	create := &Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: &plan.TableDef{}}},
	}}}
	preserved, err := preserveM0CloneLayout(source, create, "clone_db", "customer_clone", false)
	require.NoError(t, err)
	require.True(t, preserved)
	target := create.GetDdl().GetCreateTable().TableDef
	require.Equal(t, uint32(1), target.Version)
	require.Equal(t, source.Pkey.CompPkeyCol.Seqnum, target.Pkey.CompPkeyCol.Seqnum)
	for i := range source.Cols {
		require.Equal(t, source.Cols[i].Name, target.Cols[i].Name)
		require.Equal(t, source.Cols[i].ColId, target.Cols[i].ColId)
		require.Equal(t, source.Cols[i].Seqnum, target.Cols[i].Seqnum)
	}
}
