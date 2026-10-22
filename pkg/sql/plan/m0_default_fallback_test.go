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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestM0DefaultCopyAdmission(t *testing.T) {
	for _, definition := range []string{
		"flag boolean default false",
		"flag boolean default true",
		"flag boolean default (1 = 0)",
		"flag int not null default 7",
	} {
		t.Run(definition, func(t *testing.T) {
			table := m0TestPlanTable()
			table.Version = 0
			table.Cols = append(table.Cols[:7], table.Cols[8:]...)
			delete(table.Name2ColIndex, "tier")
			table.DbName = "constraint_test"
			table.Pkey.Names = []string{"c_w_id", "c_d_id", "c_id"}
			for i, col := range table.Cols[:6] {
				col.Default = &plan.Default{NullAbility: i >= 3}
			}
			proc := newPlanTestProcess(t)
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true}).AnyTimes()
			op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC}).AnyTimes()
			proc.Base.TxnOperator = op
			mock := NewMockOptimizer(false, proc)
			mock.ctxt.tables[table.Name] = table
			mock.ctxt.objects[table.Name] = &ObjectRef{SchemaName: "constraint_test", ObjName: table.Name}
			statement := "alter table constraint_test.customer add column " + definition
			built, err := buildSingleStmt(mock, t, statement)
			require.NoError(t, err)
			require.Equal(t, plan.AlterTable_COPY, built.GetDdl().GetAlterTable().AlgorithmType)
			_, err = buildSingleStmt(mock, t, statement+", algorithm=inplace")
			require.Error(t, err)
			require.Zero(t, table.Version)
			require.Len(t, table.Cols, 8, "planning must not mutate the source definition")
		})
	}
}

func TestAppendMissingPlanDeepCopy(t *testing.T) {
	source := m0TestPlanTable()
	source.Cols[len(source.Cols)-1].Seqnum = uint32(len(source.Cols) - 1)
	source.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"),
		BaseWidth: 7, PkSeqnum: 6, Columns: []*plan.ColumnBirthFill{{Seqnum: 7, BirthAppendOrdinal: 1,
			Type: source.Cols[7].Typ, Kind: plan.MissingFillKind_MISSING_FILL_NULL_V1}}}
	require.NoError(t, plan.ValidateAppendMissingTable(source))
	clone := DeepCopyTableDef(source, true)
	clone.AppendMissingState.LayoutId[0] = 'x'
	clone.AppendMissingState.Columns[0].Type.Width++
	require.Equal(t, byte('0'), source.AppendMissingState.LayoutId[0])
	require.Equal(t, source.Cols[7].Typ.Width, source.AppendMissingState.Columns[0].Type.Width)
}

func TestAppendMissingAddActionDeepCopy(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_bool)}
	original := &plan.AlterAddColumn{Name: "flag", Type: typ, Pos: -1, Default: &plan.Default{NullAbility: true, OriginString: "false",
		Expr: &plan.Expr{Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}}
	source := &plan.DataDefinition{DdlType: plan.DataDefinition_ALTER_TABLE, Definition: &plan.DataDefinition_AlterTable{AlterTable: &plan.AlterTable{
		Actions: []*plan.AlterTable_Action{{Action: &plan.AlterTable_Action_AddColumn{AddColumn: original}}},
	}}}
	copied := DeepCopyDataDefinition(source).GetAlterTable().Actions[0].GetAddColumn()
	require.Equal(t, original, copied)
	copied.Default.Expr.GetLit().Value = &plan.Literal_Bval{Bval: true}
	require.True(t, plan.IsLiteralFalseDefault(&plan.ColDef{Typ: typ, Default: original.Default}))
}
