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

package disttae

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func m0TestAppendTableDef() *plan.TableDef {
	return &plan.TableDef{
		Version: 1,
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0},
			{Name: "b", Seqnum: 1},
			{Name: pkgcatalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true},
			{Name: "c", Seqnum: 3, Default: &plan.Default{NullAbility: true}},
			{Name: pkgcatalog.Row_ID, Seqnum: 4},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: pkgcatalog.CPrimaryKeyColName,
			CompPkeyCol: &plan.ColDef{
				Name: pkgcatalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true,
			},
		},
	}
}

func TestM0AppendBaseValidatesCompletePlanDomain(t *testing.T) {
	table := m0TestAppendTableDef()
	previous := activeM0Enabled
	activeM0Enabled = true
	t.Cleanup(func() { activeM0Enabled = previous })

	base, ok := m0AppendBaseFromPlan(table)
	require.True(t, ok)
	require.Equal(t, uint32(3), base)

	table.Cols[3].Default.NullAbility = false
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Default.NullAbility = true
	table.Cols[3].Seqnum = 2
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Seqnum = 3
	table.Cols[3].Default = nil
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Default = &plan.Default{NullAbility: true}
	table.RefChildTbls = []uint64{42}
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	activeM0Enabled = false
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)
}

func TestM0AppendBaseRejectsUnboundLayouts(t *testing.T) {
	previous := activeM0Enabled
	activeM0Enabled = true
	t.Cleanup(func() { activeM0Enabled = previous })
	_, ok := m0AppendBaseFromPlan(nil)
	require.False(t, ok)
	for _, tc := range []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{"temporary", func(d *plan.TableDef) { d.IsTemporary = true }},
		{"missing key", func(d *plan.TableDef) { d.Pkey = nil }},
		{"missing composite key", func(d *plan.TableDef) { d.Pkey.CompPkeyCol = nil }},
		{"clustered", func(d *plan.TableDef) { d.ClusterBy = &plan.ClusterByDef{} }},
		{"partition", func(d *plan.TableDef) { d.TableType = pkgcatalog.SystemPartitionRel }},
		{"index", func(d *plan.TableDef) { d.Indexes = []*plan.IndexDef{{}} }},
		{"foreign key", func(d *plan.TableDef) { d.Fkeys = []*plan.ForeignKeyDef{{}} }},
		{"check", func(d *plan.TableDef) { d.Checks = []*plan.CheckDef{{}} }},
		{"short layout", func(d *plan.TableDef) { d.Cols = d.Cols[:1] }},
		{"visible key", func(d *plan.TableDef) { d.Pkey.CompPkeyCol.Hidden = false }},
		{"non primary key", func(d *plan.TableDef) { d.Pkey.CompPkeyCol.Primary = false }},
		{"wrong key name", func(d *plan.TableDef) { d.Pkey.PkeyColName = "a" }},
		{"missing rowid", func(d *plan.TableDef) { d.Cols[4].Name = "ordinary" }},
		{"key outside layout", func(d *plan.TableDef) { d.Pkey.CompPkeyCol.Seqnum = 5 }},
		{"wrong version", func(d *plan.TableDef) { d.Version = 0 }},
		{"nil column", func(d *plan.TableDef) { d.Cols[0] = nil }},
		{"wrong anchor", func(d *plan.TableDef) { d.Cols[2].Name = "wrong" }},
		{"visible anchor", func(d *plan.TableDef) { d.Cols[2].Hidden = false }},
		{"non primary anchor", func(d *plan.TableDef) { d.Cols[2].Primary = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := m0TestAppendTableDef()
			tc.mutate(table)
			base, ok := m0AppendBaseFromPlan(table)
			require.False(t, ok)
			require.Zero(t, base)
		})
	}
}

func TestM0PlanColumnSupportsOnlyNullableNullSuffix(t *testing.T) {
	require.False(t, m0PlanColumnSupportsAppend(nil))
	for _, tc := range []struct {
		name   string
		mutate func(*plan.ColDef)
		want   bool
	}{
		{"implicit null", func(*plan.ColDef) {}, true},
		{"explicit null", func(c *plan.ColDef) {
			c.Default.Expr = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		}, true},
		{"non null literal", func(c *plan.ColDef) {
			c.Default.Expr = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
		}, false},
		{"non literal", func(c *plan.ColDef) { c.Default.Expr = &plan.Expr{} }, false},
		{"hidden", func(c *plan.ColDef) { c.Hidden = true }, false},
		{"primary", func(c *plan.ColDef) { c.Primary = true }, false},
		{"cluster key", func(c *plan.ColDef) { c.ClusterBy = true }, false},
		{"auto increment", func(c *plan.ColDef) { c.Typ.AutoIncr = true }, false},
		{"generated", func(c *plan.ColDef) { c.GeneratedCol = &plan.GeneratedCol{} }, false},
		{"on update", func(c *plan.ColDef) { c.OnUpdate = &plan.OnUpdate{} }, false},
		{"missing default", func(c *plan.ColDef) { c.Default = nil }, false},
		{"default not null", func(c *plan.ColDef) { c.Default.NullAbility = false }, false},
		{"type not null", func(c *plan.ColDef) { c.Typ.NotNullable = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := &plan.ColDef{Default: &plan.Default{NullAbility: true}}
			tc.mutate(col)
			require.Equal(t, tc.want, m0PlanColumnSupportsAppend(col))
		})
	}
}

func TestM0AlterAdmissionFailureDoesNotPublish(t *testing.T) {
	previousEnabled, previousHook := activeM0Enabled, activeM0CNAddColumn
	activeM0Enabled = true
	t.Cleanup(func() { activeM0Enabled, activeM0CNAddColumn = previousEnabled, previousHook })
	add := api.NewAddColumnReq(7, 42, "added", plan.Type{}, -1)
	cause := moerr.NewInternalErrorNoCtx("private definition rejected")
	for _, tc := range []struct {
		name      string
		requests  []*api.AlterTableReq
		wantCalls int
	}{
		{"private preparation failure", []*api.AlterTableReq{add}, 1},
		{"duplicate add", []*api.AlterTableReq{add, add}, 0},
		{"mixed actions", []*api.AlterTableReq{api.NewRenameTableReq(7, 42, "tbl", "renamed"), add}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workspace := newTransactionWithActivePKTableForTest(t, "pk")
			workspace.op.(*mock_frontend.MockTxnOperator).EXPECT().IsSnapOp().Return(false)
			table := workspace.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			definition := table.tableDef
			calls := 0
			activeM0CNAddColumn = func(got *txnTable, _ context.Context, request *api.AlterTableReq) ([]engine.TableDef, error) {
				calls++
				require.Same(t, table, got)
				require.Same(t, add, request)
				return nil, cause
			}
			err := table.AlterTable(context.Background(), nil, tc.requests)
			if tc.wantCalls != 0 {
				require.Same(t, cause, err)
			} else {
				require.ErrorContains(t, err, "one independent ALTER action")
			}
			require.Equal(t, tc.wantCalls, calls)
			require.Same(t, definition, table.tableDef)
			require.Equal(t, "tbl", table.tableName)
			require.Zero(t, table.version)
			require.Empty(t, workspace.writes)
			require.Empty(t, workspace.restoreTxnTableFunc)
			_, stable := workspace.CatalogVisibility()
			require.True(t, stable, "early rejection must leave the catalog readable")
		})
	}
}

func TestM0BirthStateCNDomainNeedsDescriptor(t *testing.T) {
	saved := activeM0Enabled
	activeM0Enabled = true
	t.Cleanup(func() { activeM0Enabled = saved })
	table := m0TestAppendTableDef()
	table.Cols[len(table.Cols)-1].Hidden = true
	for i, c := range table.Cols {
		c.ColId = uint64(i + 1)
	}
	table.Pkey.CompPkeyCol = table.Cols[2]
	col := table.Cols[3]
	col.Typ = plan.Type{Id: plan.AppendMissingBoolOID}
	col.Default = &plan.Default{NullAbility: true, OriginString: "false", Expr: &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}
	_, ok := m0AppendBaseFromPlan(table)
	require.False(t, ok)
	table.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 3, PkSeqnum: 2,
		Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1, Type: col.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
	base, ok := m0AppendBaseFromPlan(table)
	require.True(t, ok)
	require.Equal(t, uint32(3), base)
	table.AppendMissingState.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)
	table.AppendMissingState.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1
	col.Default.OriginString = "0"
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)
}

func TestM0MissingColumnMetadata(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_bool)}
	state := &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 1, PkSeqnum: 0,
		Columns: []*plan.ColumnBirthFill{
			{Seqnum: 1, BirthAppendOrdinal: 1, Type: typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1},
			{Seqnum: 2, BirthAppendOrdinal: 2, Type: typ, Kind: plan.MissingFillKind_MISSING_FILL_NULL_V1},
		}}
	proof, err := objectio.NewM0ObjectProof("old", 1, 3, 42, 2, state)
	require.NoError(t, err)
	ctx := objectio.WithM0ProvenObject(t.Context(), proof)
	col := &plan.ColDef{Seqnum: 1, Typ: typ}
	zm, nulls, err := m0MissingColumnMetadata(ctx, "old", col, 2)
	require.NoError(t, err)
	require.Zero(t, nulls)
	require.Equal(t, false, zm.GetMin())
	require.Equal(t, false, zm.GetMax())
	zm, nulls, err = m0MissingColumnMetadata(ctx, "old", col, 0)
	require.NoError(t, err)
	require.Zero(t, nulls)
	require.False(t, zm.IsInited(), "no logical values for a zero-row object")
	col.Seqnum = 2
	zm, nulls, err = m0MissingColumnMetadata(ctx, "old", col, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, nulls)
	require.False(t, zm.IsInited())
	for _, tc := range []struct {
		ctx  context.Context
		name string
		seq  uint32
		typ  plan.Type
	}{
		{t.Context(), "old", 1, typ},
		{ctx, "other", 1, typ},
		{ctx, "old", 0, typ},
		{ctx, "old", 3, typ},
		{ctx, "old", 1, plan.Type{Id: int32(types.T_int32)}},
	} {
		_, _, err := m0MissingColumnMetadata(tc.ctx, tc.name, &plan.ColDef{Seqnum: tc.seq, Typ: tc.typ}, 2)
		require.ErrorIs(t, err, objectio.ErrMissingColumnWithoutProof)
	}
}
