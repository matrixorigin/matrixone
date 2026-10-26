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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestM0PlanHookDispatchContract(t *testing.T) {
	previous := activeM0PlanHooks
	activeM0PlanHooks = m0PlanHooks{}
	t.Cleanup(func() { activeM0PlanHooks = previous })
	table := &TableDef{Name: "source"}
	create := &Plan{}
	options := []tree.AlterTableOption{nil}
	selected, err := m0SelectAddColumn(nil, options, table)
	require.NoError(t, err)
	require.False(t, selected)
	preserved, err := m0PreserveCloneLayout(table, create, "db", "child", false)
	require.NoError(t, err)
	require.False(t, preserved)
	require.Error(t, m0ValidateBoundAddColumn(nil))

	ctx := &MockCompilerContext{}
	col := &ColDef{Name: "added"}
	cause := moerr.NewInternalErrorNoCtx("hook failure")
	calls := [3]int{}
	activeM0PlanHooks = m0PlanHooks{
		selectAdd: func(gotCtx CompilerContext, gotOptions []tree.AlterTableOption, gotTable *TableDef) (bool, error) {
			calls[0]++
			require.Same(t, ctx, gotCtx)
			require.Equal(t, options, gotOptions)
			require.Same(t, table, gotTable)
			return true, cause
		},
		preserveClone: func(gotSource *TableDef, gotPlan *Plan, database, name string, temporary bool) (bool, error) {
			calls[1]++
			require.Same(t, table, gotSource)
			require.Same(t, create, gotPlan)
			require.Equal(t, "db", database)
			require.Equal(t, "child", name)
			require.True(t, temporary)
			return true, cause
		},
		validateAdd: func(got *ColDef) error {
			calls[2]++
			require.Same(t, col, got)
			return cause
		},
	}
	selected, err = m0SelectAddColumn(ctx, options, table)
	require.True(t, selected)
	require.Same(t, cause, err)
	preserved, err = m0PreserveCloneLayout(table, create, "db", "child", true)
	require.True(t, preserved)
	require.Same(t, cause, err)
	require.Same(t, cause, m0ValidateBoundAddColumn(col))
	require.Equal(t, [3]int{1, 1, 1}, calls)
}

func TestM0PlanDomainAndColumnResolutionContract(t *testing.T) {
	previous := activeM0PlanHooks
	activeM0PlanHooks = m0PlanHooks{}
	t.Cleanup(func() { activeM0PlanHooks = previous })
	newTable := func() *TableDef {
		key := &ColDef{Name: catalog.CPrimaryKeyColName, Hidden: true, Primary: true}
		return &TableDef{
			Cols:          []*ColDef{nil, key},
			Pkey:          &PrimaryKeyDef{PkeyColName: key.Name, CompPkeyCol: key},
			Name2ColIndex: map[string]int32{"added": 7},
		}
	}
	table := newTable()
	require.False(t, M0AppendDomain(table))
	require.Equal(t, 2, M0ResolveColumnPosition(table, "added", 2))
	activeM0PlanHooks.selectAdd = func(CompilerContext, []tree.AlterTableOption, *TableDef) (bool, error) {
		return false, nil
	}
	require.True(t, M0AppendDomain(table))
	require.False(t, M0AppendDomain(nil))
	require.Equal(t, 2, M0ResolveColumnPosition(nil, "added", 2))
	require.Equal(t, 2, M0ResolveColumnPosition(&TableDef{}, "added", 2))
	require.Equal(t, 7, M0ResolveColumnPosition(table, "DB.Customer.ADDED", 2))
	require.Equal(t, 7, M0ResolveColumnPosition(table, "added", 2))
	require.Equal(t, 2, M0ResolveColumnPosition(table, "missing", 2))
	for _, tc := range []struct {
		name   string
		mutate func(*TableDef)
		want   bool
	}{
		{"clustered", func(d *TableDef) { d.ClusterBy = &ClusterByDef{} }, false},
		{"no hidden key", func(d *TableDef) { d.Cols = nil }, false},
		{"visible key", func(d *TableDef) { d.Cols[1].Hidden = false }, false},
		{"wrong key name", func(d *TableDef) { d.Pkey.PkeyColName = "wrong" }, false},
		{"non primary anchor", func(d *TableDef) { d.Pkey.CompPkeyCol.Primary = false }, false},
		{"mismatched anchor", func(d *TableDef) { d.Pkey.CompPkeyCol = &ColDef{Name: "wrong", Hidden: true, Primary: true} }, false},
		{"no key descriptor", func(d *TableDef) { d.Pkey = nil }, true},
		{"no composite descriptor", func(d *TableDef) { d.Pkey.CompPkeyCol = nil }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := newTable()
			tc.mutate(table)
			require.Equal(t, tc.want, M0AppendDomain(table))
		})
	}
}

func TestM0PlanBoundAddDispatch(t *testing.T) {
	for _, scenario := range []string{"bound action", "validator rejects", "validator absent", "selector rejects"} {
		t.Run(scenario, func(t *testing.T) {
			previous := activeM0PlanHooks
			t.Cleanup(func() { activeM0PlanHooks = previous })
			cause := moerr.NewInvalidInputNoCtx("rejected by admission")
			selected, validated := 0, 0
			activeM0PlanHooks = m0PlanHooks{
				selectAdd: func(_ CompilerContext, _ []tree.AlterTableOption, _ *TableDef) (bool, error) {
					selected++
					if scenario == "selector rejects" {
						return false, cause
					}
					return true, nil
				},
				validateAdd: func(col *ColDef) error {
					validated++
					require.Equal(t, "dispatch_flag", col.Name)
					require.True(t, plan.IsLiteralFalseDefault(col), "validate the bound type/default, not the AST")
					if scenario == "validator rejects" {
						return cause
					}
					return nil
				},
			}
			if scenario == "validator absent" {
				activeM0PlanHooks.validateAdd = nil
			}
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			built, err := buildSingleStmt(mock, t, "alter table t1 add column dispatch_flag boolean default false")
			require.Equal(t, 1, selected)
			switch scenario {
			case "validator absent":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.Nil(t, built)
				require.Zero(t, validated)
			case "selector rejects":
				require.Same(t, cause, err)
				require.Nil(t, built)
				require.Zero(t, validated)
			case "validator rejects":
				require.Same(t, cause, err)
				require.Nil(t, built)
				require.Equal(t, 1, validated)
			default:
				require.NoError(t, err)
				require.Equal(t, 1, validated)
				alter := built.GetDdl().GetAlterTable()
				require.Equal(t, plan.AlterTable_INPLACE, alter.AlgorithmType)
				require.Len(t, alter.Actions, 1)
				add := alter.Actions[0].GetAddColumn()
				require.Equal(t, "dispatch_flag", add.Name)
				require.Equal(t, int32(-1), add.Pos)
				require.True(t, plan.IsLiteralFalseDefault(&ColDef{Typ: add.Type, Default: add.Default}))
				require.Nil(t, FindColumn(alter.TableDef.Cols, add.Name), "binding must not mutate the source")
			}
		})
	}
}

func TestM0CopyExitCommentPreservesProperties(t *testing.T) {
	comment := &plan.Property{Key: catalog.SystemRelAttr_Comment, Value: "old"}
	other := &plan.Property{Key: "other", Value: "kept"}
	table := &TableDef{Defs: []*plan.TableDef_DefType{
		{},
		{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{other, comment}}}},
	}}
	setM0CopyExitComment(table, "new")
	require.Len(t, table.Defs, 2)
	require.Equal(t, "new", comment.Value)
	require.Equal(t, "kept", other.Value)
	empty := &TableDef{}
	setM0CopyExitComment(empty, "created")
	require.Equal(t, []*plan.Property{{Key: catalog.SystemRelAttr_Comment, Value: "created"}}, empty.Defs[0].GetProperties().Properties)
}

// Shared layout projection and COPY contracts apply in both build modes.
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
