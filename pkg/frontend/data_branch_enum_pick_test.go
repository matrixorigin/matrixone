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

package frontend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type pickEnumPlanCompiler struct {
	*sqlplan.MockCompilerContext
}

func (c *pickEnumPlanCompiler) Resolve(dbName, tableName string, _ *sqlplan.Snapshot, borrow *sqlplan.TableResolution) (*plan.ObjectRef, *plan.TableDef, error) {
	borrow.Reset()
	if !strings.EqualFold(tableName, "lookup_t") {
		return c.MockCompilerContext.Resolve(dbName, tableName, nil, borrow)
	}
	if dbName == "" {
		dbName = c.DefaultDatabase()
	}
	return &plan.ObjectRef{Obj: 9901, ObjName: tableName, SchemaName: dbName}, &plan.TableDef{
		TblId: 9901, Name: tableName, DbName: dbName, TableType: catalog.SystemOrdinaryRel,
		Name2ColIndex: map[string]int32{"e": 0, "id": 1},
		Cols: []*plan.ColDef{
			{Name: "e", Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: ",x", EnumCollation: types.EnumCollationUTF8Bin}},
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
	}, nil
}

func TestPickEnumLiteralRespectsDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8MB4UnicodeCI)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(mp)
	require.NoError(t, vector.AppendBytes(params, []byte("2"), false, mp))
	require.NoError(t, params.SetStringSource(types.StringSourceCOMStmt))
	proc := testutil.NewProcess(t)
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamNone})
	for _, tc := range []struct {
		literal string
		want    types.Enum
		bad     bool
	}{
		{"'2'", 1, false}, {"2", 2, false}, {"'x'", 2, false},
		{"0", 0, false}, {"?", 1, false}, {"'missing'", 0, true},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "data branch pick s into d keys("+tc.literal+")", 1)
			require.NoError(t, err)
			vec := vector.NewVec(typ)
			defer vec.Free(mp)
			err = appendPickKeyExpr(vec, stmt.(*tree.DataBranchPick).Keys.KeyExprs[0], typ, "２,x", time.UTC, mp, params, proc)
			if tc.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](vec, 0))
		})
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "data branch pick s into d keys(?)", 1)
	require.NoError(t, err)
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger})
	numeric := vector.NewVec(typ)
	defer numeric.Free(mp)
	require.NoError(t, appendPickKeyExpr(numeric, stmt.(*tree.DataBranchPick).Keys.KeyExprs[0], typ, "２,x", time.UTC, mp, params, proc))
	require.Equal(t, types.Enum(2), vector.GetFixedAtNoTypeCheck[types.Enum](numeric, 0))
}

func TestPickEnumRejectsAmbiguousDisplaySubquery(t *testing.T) {
	for _, tc := range []struct {
		name, definition string
		identity         uint32
		wantError        bool
	}{
		{"empty binary member", "", types.EnumCollationUTF8Bin, true},
		{"zero-weight unicode member", "\u0301,x", types.EnumCollationUTF8MB4UnicodeCI, true},
		{"no empty member", "A,a", types.EnumCollationUTF8Bin, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_enum.ToType()
			typ.Charset = uint8(tc.identity)
			var tbl tableStuff
			tbl.def.colTypes = []types.Type{typ}
			tbl.def.enumValues = []string{tc.definition}
			tbl.def.pkColIdxes = []int{0}
			display := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: int64(function.CAST_INDEX_TO_VALUE) << 32},
				Args: []*plan.Expr{{}, {Typ: plan.Type{Id: int32(types.T_enum)}}},
			}}}
			query := &plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{{ProjectList: []*plan.Expr{display}}}}
			err := validatePickEnumSubquery(query, tbl)
			if tc.wantError {
				require.ErrorContains(t, err, "cannot distinguish ordinal 0")
			} else {
				require.NoError(t, err)
			}
			display.GetF().SyntaxExplicitCast = true
			require.NoError(t, validatePickEnumSubquery(query, tbl), "authored display calls deliberately request text")
			display.GetF().SyntaxExplicitCast = false
			// An explicit cast is an intentional text/ordinal boundary, rather
			// than a bare display projection.
			query.Nodes[0].ProjectList[0] = &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: int64(function.CAST) << 32}, Args: []*plan.Expr{display},
			}}}
			require.NoError(t, validatePickEnumSubquery(query, tbl))
			query.Nodes[0].ProjectList[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_enum)}}
			err = validatePickEnumSubquery(query, tbl)
			if tc.wantError {
				require.ErrorContains(t, err, "ENUM domain is unavailable")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPickEnumValidatesOnlyReturnedKeyLineage(t *testing.T) {
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8Bin)
	var tbl tableStuff
	tbl.def.colTypes = []types.Type{typ}
	tbl.def.enumValues = []string{"", "x"}
	tbl.def.pkColIdxes = []int{0}

	display := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: int64(function.CAST_INDEX_TO_VALUE) << 32},
		Args: []*plan.Expr{{}, {Typ: plan.Type{Id: int32(types.T_enum)}}},
	}}}
	numeric := &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint16)}}
	col := func(tag, pos int32, typ plan.Type) *plan.Expr {
		return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: pos}}}
	}

	t.Run("derived bare enum output follows its source slot", func(t *testing.T) {
		query := &plan.Query{
			Steps: []int32{0, 1},
			Nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{display}},
				{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*plan.Expr{col(0, 0, plan.Type{Id: int32(types.T_varchar)})}},
			},
		}
		err := validatePickEnumSubquery(query, tbl)
		require.ErrorContains(t, err, "cannot distinguish ordinal 0")
	})

	t.Run("CTE sink scan follows only its output column", func(t *testing.T) {
		query := &plan.Query{
			Steps: []int32{0, 1, 2},
			Nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{numeric, display}},
				{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}, ProjectList: []*plan.Expr{col(0, 1, plan.Type{Id: int32(types.T_varchar)})}},
				{NodeType: plan.Node_PROJECT, Children: []int32{1}, ProjectList: []*plan.Expr{col(0, 0, plan.Type{Id: int32(types.T_varchar)})}},
			},
		}
		err := validatePickEnumSubquery(query, tbl)
		require.ErrorContains(t, err, "cannot distinguish ordinal 0")
	})

	t.Run("unrelated label filter does not taint numeric key", func(t *testing.T) {
		query := &plan.Query{
			Steps: []int32{0, 1},
			Nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{display}},
				{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*plan.Expr{numeric}, FilterList: []*plan.Expr{display}},
			},
		}
		require.NoError(t, validatePickEnumSubquery(query, tbl))
	})
}

func TestPickEnumValidatesOptimizerOutputLineage(t *testing.T) {
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8Bin)
	single := tableStuff{}
	single.def.colTypes = []types.Type{typ}
	single.def.enumValues = []string{",x"}
	single.def.pkColIdxes = []int{0}
	single.def.pkColIdx = 0

	composite := tableStuff{}
	composite.def.colTypes = []types.Type{typ, types.T_int32.ToType()}
	composite.def.enumValues = []string{",x", ""}
	composite.def.pkColIdxes = []int{0, 1}
	composite.def.pkColIdx = 0
	compositeEnum := tableStuff{}
	compositeEnum.def.colTypes = []types.Type{typ, typ}
	compositeEnum.def.enumValues = []string{",x", ",x"}
	compositeEnum.def.pkColIdxes = []int{0, 1}
	compositeEnum.def.pkColIdx = 0

	compiler := &pickEnumPlanCompiler{MockCompilerContext: sqlplan.NewMockCompilerContext(true, testutil.NewProcess(t))}
	for _, tc := range []struct {
		name, query string
		tbl         tableStuff
		wantError   bool
	}{
		{"derived bare ENUM output", "select e from (select e from lookup_t) q", single, true},
		{"CTE bare ENUM output", "with q as (select e from lookup_t) select e from q", single, true},
		{"UNION left ENUM output", "select e from lookup_t union all select 'x'", single, true},
		{"UNION right ENUM output", "select 'x' from lookup_t union all select e from lookup_t", single, true},
		{"aggregate ENUM display output", "select max(e) from lookup_t", single, true},
		{"correlated scalar bare ENUM output", "select (select i.e from lookup_t i where i.id >= o.id limit 1) from lookup_t o", single, true},
		{"numeric output with pushed ENUM predicate", "select id from lookup_t where e='x'", single, false},
		{"numeric key with unrelated label filter", "select k from (select cast(e as unsigned) k, e as label from lookup_t) q where length(label)=0", single, false},
		{"composite key with numeric ENUM slot", "select cast(e as unsigned), id from lookup_t", composite, false},
		{"composite key with ambiguous ENUM slot", "select cast(e as unsigned), e from lookup_t", compositeEnum, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orderedSQL := "select * from (" + tc.query + ") as __mo_pick_sub order by 1"
			if len(tc.tbl.def.pkColIdxes) > 1 {
				orderedSQL = "select * from (" + tc.query + ") as __mo_pick_sub order by 1, 2"
			}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, orderedSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			query, err := sqlplan.NewBaseOptimizer(compiler).Optimize(stmt, false, nil)
			require.NoError(t, err, orderedSQL)
			if tc.name == "correlated scalar bare ENUM output" {
				require.NotNil(t, query.Correlation, "the regression must exercise a scalar dependent program")
				require.NotEmpty(t, query.Correlation.Programs, "the regression must register the correlated scalar output")
			}
			for _, node := range query.Nodes {
				require.Empty(t, node.BindingTags, "the test must exercise optimizer-local column positions")
			}
			err = validatePickEnumSubquery(query, tc.tbl)
			if tc.wantError {
				require.Error(t, err, orderedSQL)
			} else {
				require.NoError(t, err, orderedSQL)
			}
		})
	}
}

func TestPickEnumSubqueryRemapsDomainAndPreservesZero(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8Bin)
	src := vector.NewVec(typ)
	defer src.Free(mp)
	for _, v := range []types.Enum{0, 1, 2} {
		require.NoError(t, vector.AppendFixed(src, v, false, mp))
	}
	source := &plan.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: types.EnumCollationUTF8Bin}
	unchanged, owned, err := coercePickEnumKeyVector(nil, src, typ, "A,a", source, mp)
	require.NoError(t, err)
	require.False(t, owned)
	require.Same(t, src, unchanged)
	remapped, owned, err := coercePickEnumKeyVector(nil, src, typ, "a,A", source, mp)
	require.NoError(t, err)
	require.True(t, owned)
	defer remapped.Free(mp)
	require.Equal(t, []types.Enum{0, 2, 1}, vector.MustFixedColNoTypeCheck[types.Enum](remapped))
	_, _, err = coercePickEnumKeyVector(nil, src, typ, "a,A", nil, mp)
	require.ErrorContains(t, err, "domain is unavailable")

	text := vector.NewVec(types.T_varchar.ToType())
	defer text.Free(mp)
	require.NoError(t, vector.AppendBytes(text, []byte("a"), false, mp))
	converted, owned, err := coercePickEnumKeyVector(nil, text, typ, "A,a", nil, mp)
	require.NoError(t, err)
	require.True(t, owned)
	defer converted.Free(mp)
	require.Equal(t, types.Enum(2), vector.GetFixedAtNoTypeCheck[types.Enum](converted, 0))

	empty := vector.NewVec(typ)
	defer empty.Free(mp)
	require.NoError(t, vector.AppendFixed(empty, types.Enum(0), false, mp))
	require.NoError(t, vector.AppendFixed(empty, types.Enum(1), false, mp))
	emptyType := &plan.Type{Id: int32(types.T_enum), Enumvalues: "", EnumCollation: types.EnumCollationUTF8Bin}
	_, _, err = coercePickEnumKeyVector(nil, empty, typ, "x,", emptyType, mp)
	require.ErrorContains(t, err, "domain is unavailable")
}
