//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func m0BirthExitPlanTable() *TableDef {
	table := m0TestPlanTable()
	table.DbName = "constraint_test"
	for i, col := range table.Cols[:6] {
		col.Default = &plan.Default{NullAbility: i >= 3}
	}
	table.Cols[len(table.Cols)-1].Seqnum = uint32(len(table.Cols) - 1)
	table.Pkey.Names = []string{"c_w_id", "c_d_id", "c_id"}
	table.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"),
		BaseWidth: 7, PkSeqnum: 6, Columns: []*plan.ColumnBirthFill{{Seqnum: 7, BirthAppendOrdinal: 1, Type: table.Cols[7].Typ, Kind: plan.MissingFillKind_MISSING_FILL_NULL_V1}}}
	return table
}

func TestM0BirthCopyExitPlan(t *testing.T) {
	for _, suffix := range []string{
		"alter column tier set default 'new'",
		"drop column tier",
		"add column next_flag boolean default true",
		"comment='a''b'",
		"rename to customer_next",
	} {
		t.Run(suffix, func(t *testing.T) {
			table := m0BirthExitPlanTable()
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables[table.Name] = table
			mock.ctxt.objects[table.Name] = &ObjectRef{SchemaName: table.DbName, ObjName: table.Name}
			built, err := buildSingleStmt(mock, t, "alter table constraint_test.customer "+suffix)
			require.NoError(t, err)
			alter := built.GetDdl().GetAlterTable()
			require.Equal(t, plan.AlterTable_COPY, alter.AlgorithmType)
			require.NotNil(t, alter.TableDef.AppendMissingState)
			require.Nil(t, alter.CopyTableDef.AppendMissingState)
			require.Zero(t, alter.CopyTableDef.Version)
			require.NotNil(t, table.AppendMissingState, "planner must not change source")
			require.Equal(t, uint32(1), table.Version)
			require.Contains(t, alter.InsertTmpDataSql, "customer")
			_, err = buildSingleStmt(mock, t, "alter table constraint_test.customer "+suffix+", algorithm=inplace")
			require.Error(t, err)
		})
	}
}

func TestM0BirthCopyExitRejectsUnknownState(t *testing.T) {
	table := m0BirthExitPlanTable()
	table.AppendMissingState.FormatVersion++
	_, err := buildCopyTableDef(context.Background(), table)
	require.Error(t, err)
}
