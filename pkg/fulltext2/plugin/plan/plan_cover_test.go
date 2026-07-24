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

// CPU-only unit coverage for the fulltext2 plan-layer hooks. plan.go's inert
// vector-sort redirects (CanApply / ApplyForSort), schema.go's hidden-table
// builder (BuildFullTextIndexDefs) + buildFullText2Params, and tablefunc.go's
// pure TVF builders (buildFullText2Create / buildFullText2Search /
// getFullText2Params) are all reachable without a live cluster. The
// happy-path builders call two planplugin helper vars (production wires them
// in pkg/sql/plan's init) which the init() below stubs to avoid an import
// cycle.
package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func init() {
	if planplugin.MakeHiddenColDefByName == nil {
		planplugin.MakeHiddenColDefByName = func(name string) *plan.ColDef {
			return &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(types.T_varchar)}}
		}
	}
	if planplugin.DeepCopyColDefList == nil {
		planplugin.DeepCopyColDefList = func(in []*plan.ColDef) []*plan.ColDef { return in }
	}
}

// --- stubs -----------------------------------------------------------------

// stubCompilerContext is the minimal planplugin.CompilerContext (GetContext +
// ResolveVariable).
type stubCompilerContext struct{ ctx context.Context }

func (c stubCompilerContext) GetContext() context.Context { return c.ctx }
func (c stubCompilerContext) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

var _ planplugin.CompilerContext = stubCompilerContext{}

func newStubCompilerContext() stubCompilerContext {
	return stubCompilerContext{ctx: context.Background()}
}

// stubPlanBuilder is the minimal planplugin.PlanBuilder. The fulltext2
// tablefunc builders only use GenNewBindTag / AppendNode / GetContext; the
// vector-sort redirect methods are never reached from these tests, so they
// panic.
type stubPlanBuilder struct {
	ctx     context.Context
	nodes   []*plan.Node
	nextTag int32
}

func newStubPlanBuilder() *stubPlanBuilder {
	return &stubPlanBuilder{ctx: context.Background()}
}

func (b *stubPlanBuilder) GetContext() context.Context { return b.ctx }

func (b *stubPlanBuilder) GenNewBindTag() int32 {
	b.nextTag++
	return b.nextTag
}

func (b *stubPlanBuilder) AppendNode(node *plan.Node, _ planplugin.BindContext) int32 {
	id := int32(len(b.nodes))
	b.nodes = append(b.nodes, node)
	return id
}

func (b *stubPlanBuilder) ApplyIndicesForSortUsingHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) CanApplyHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) CanApplyCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) CanApplyIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used")
}
func (b *stubPlanBuilder) CanApplyIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("not used")
}

var _ planplugin.PlanBuilder = (*stubPlanBuilder)(nil)

// --- builders for AST inputs ----------------------------------------------

// ftIndex builds a *tree.FullTextIndex over the given column names (single or
// multi-column) with no IndexOption.
func ftIndex(name string, cols ...string) *tree.FullTextIndex {
	kp := make([]*tree.KeyPart, 0, len(cols))
	for _, c := range cols {
		kp = append(kp, &tree.KeyPart{ColName: tree.NewUnresolvedName(tree.NewCStr(c, 0))})
	}
	return &tree.FullTextIndex{Name: name, KeyParts: kp}
}

// textColMap returns a colMap with a varchar pk and a text-typed indexed col.
func textColMap(pkName string, cols ...string) map[string]*plan.ColDef {
	m := map[string]*plan.ColDef{
		pkName: {Name: pkName, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}
	for _, c := range cols {
		m[c] = &plan.ColDef{Name: c, Typ: plan.Type{Id: int32(types.T_text)}}
	}
	return m
}

func numValTblFunc(s string) *tree.TableFunction {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{nv}}}
}

func nonNumValTblFunc() *tree.TableFunction {
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
}

// makeArgs builds n plan.Expr args: [param(varchar), ints...].
func makeArgs(n int) []*plan.Expr {
	out := make([]*plan.Expr, 0, n)
	out = append(out, &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "{}"}}},
	})
	for i := 1; i < n; i++ {
		out = append(out, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(i)}}},
		})
	}
	return out
}

// --- plan.go: inert vector-sort hooks --------------------------------------

func TestCanApply_AlwaysFalse(t *testing.T) {
	ok, err := Hooks{}.CanApply(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestApplyForSort_Inert(t *testing.T) {
	id, applied, err := Hooks{}.ApplyForSort(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 42, planplugin.ApplyForSortOpts{})
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, int32(42), id)
}

// --- plan.go: BuildSecondaryIndexDefs (always errors) ----------------------

func TestBuildSecondaryIndexDefs_NotSupported(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), nil, nil, nil, "id")
	require.Error(t, err)
	require.Nil(t, idxDefs)
	require.Nil(t, tblDefs)
}

// --- schema.go: BuildFullTextIndexDefs error paths -------------------------

func TestBuildFullTextIndexDefs_EmptyPkey(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "body"), textColMap("id", "body"), nil, "")
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_FakePkey(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "body"), textColMap("id", "body"), nil, catalog.FakePrimaryKeyColName)
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_DuplicateColumn(t *testing.T) {
	existed := []*plan.IndexDef{{
		IndexAlgo: tree.INDEX_TYPE_FULLTEXT2.ToString(),
		Parts:     []string{"body"},
	}}
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "body"), textColMap("id", "body"), existed, "id")
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_ColNotExist(t *testing.T) {
	// indexed column missing from colMap.
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "nope"), textColMap("id", "body"), nil, "id")
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_UnsupportedColType(t *testing.T) {
	m := textColMap("id", "body")
	m["body"].Typ.Id = int32(types.T_int64) // fulltext2 only supports char/varchar/text/json/datalink
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "body"), m, nil, "id")
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_BadParser(t *testing.T) {
	idx := ftIndex("ft", "body")
	idx.IndexOption = &tree.IndexOption{ParserName: "bogus"}
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), idx, textColMap("id", "body"), nil, "id")
	require.Error(t, err)
}

func TestBuildFullTextIndexDefs_PositionFreeNonGojieba(t *testing.T) {
	idx := ftIndex("ft", "body")
	idx.IndexOption = &tree.IndexOption{ParserName: "ngram", PositionFree: true}
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), idx, textColMap("id", "body"), nil, "id")
	require.Error(t, err)
}

// --- schema.go: BuildFullTextIndexDefs happy path --------------------------

func TestBuildFullTextIndexDefs_OK(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "body"), textColMap("id", "body"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)

	// index defs: both stamped algo=fulltext2, told apart by IndexAlgoTableType.
	algo := tree.INDEX_TYPE_FULLTEXT2.ToString()
	require.Equal(t, algo, idxDefs[0].IndexAlgo)
	require.Equal(t, algo, idxDefs[1].IndexAlgo)
	require.Equal(t, catalog.FullText2Index_TblType_Storage, idxDefs[0].IndexAlgoTableType)
	require.Equal(t, catalog.FullText2Index_TblType_Metadata, idxDefs[1].IndexAlgoTableType)
	require.Equal(t, []string{"body"}, idxDefs[0].Parts)
	require.NotEmpty(t, idxDefs[0].IndexAlgoParams)

	// storage table: 4 declared cols + hidden composite pk; metadata: 6 cols.
	require.Equal(t, catalog.FullText2Index_TblType_Storage, tblDefs[0].TableType)
	require.Equal(t, catalog.FullText2Index_TblType_Metadata, tblDefs[1].TableType)
	require.Len(t, tblDefs[0].Cols, 5)
	require.Len(t, tblDefs[1].Cols, 6)
	require.NotNil(t, tblDefs[0].Pkey)
	require.NotNil(t, tblDefs[1].Pkey)
}

func TestBuildFullTextIndexDefs_MultiColumnOK(t *testing.T) {
	idxDefs, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), ftIndex("ft", "title", "body"), textColMap("id", "title", "body"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.ElementsMatch(t, []string{"title", "body"}, idxDefs[0].Parts)
}

func TestBuildFullTextIndexDefs_GojiebaPositionFreeOK(t *testing.T) {
	idx := ftIndex("ft", "body")
	idx.IndexOption = &tree.IndexOption{ParserName: "gojieba", PositionFree: true}
	idxDefs, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), idx, textColMap("id", "body"), nil, "id")
	require.NoError(t, err)
	params, perr := catalog.IndexParamsStringToMap(idxDefs[0].IndexAlgoParams)
	require.NoError(t, perr)
	require.Equal(t, "gojieba", params["parser"])
	require.Equal(t, "true", params[catalog.IndexAlgoParamPositionFree])
}

// --- schema.go: buildFullText2Params directly ------------------------------

func TestBuildFullText2Params_DefaultParser(t *testing.T) {
	s, err := buildFullText2Params(ftIndex("ft", "body"))
	require.NoError(t, err)
	m, err := catalog.IndexParamsStringToMap(s)
	require.NoError(t, err)
	require.Equal(t, "ngram", m["parser"]) // default parser always recorded
}

func TestBuildFullText2Params_AllOptions(t *testing.T) {
	idx := ftIndex("ft", "body")
	idx.IndexOption = &tree.IndexOption{
		ParserName:          "gojieba",
		MaxIndexCapacity:    1000000,
		MaxPostingsCapacity: 4000000,
		PositionFree:        true,
		AutoUpdate:          true,
		Day:                 1,
		Hour:                2,
		Second:              3,
	}
	s, err := buildFullText2Params(idx)
	require.NoError(t, err)
	m, err := catalog.IndexParamsStringToMap(s)
	require.NoError(t, err)
	require.Equal(t, "gojieba", m["parser"])
	require.Equal(t, "1000000", m[catalog.IndexAlgoParamMaxIndexCapacity])
	require.Equal(t, "4000000", m[catalog.IndexAlgoParamMaxPostingsCapacity])
	require.Equal(t, "true", m[catalog.IndexAlgoParamPositionFree])
	require.Equal(t, "true", m[catalog.AutoUpdate])
	require.Equal(t, "1", m[catalog.Day])
	require.Equal(t, "2", m[catalog.Hour])
	require.Equal(t, "3", m[catalog.Second])
}

func TestBuildFullText2Params_PositionFreeNonGojiebaError(t *testing.T) {
	idx := ftIndex("ft", "body")
	idx.IndexOption = &tree.IndexOption{ParserName: "ngram", PositionFree: true}
	_, err := buildFullText2Params(idx)
	require.Error(t, err)
}

// --- tablefunc.go: getFullText2Params --------------------------------------

func TestGetFullText2Params_OK(t *testing.T) {
	b := newStubPlanBuilder()
	nv := tree.NewNumVal[string](`{"parser":"ngram"}`, `{"parser":"ngram"}`, false, tree.P_char)
	out, err := getFullText2Params(b, &tree.FuncExpr{Exprs: tree.Exprs{nv}})
	require.NoError(t, err)
	require.Equal(t, `{"parser":"ngram"}`, out)
}

func TestGetFullText2Params_Error(t *testing.T) {
	b := newStubPlanBuilder()
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	_, err := getFullText2Params(b, &tree.FuncExpr{Exprs: tree.Exprs{un}})
	require.Error(t, err)
}

// --- tablefunc.go: buildFullText2Create ------------------------------------

func TestBuildFullText2Create_TooFewArgs(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildFullText2Create(b, numValTblFunc(`{}`), nil, makeArgs(3), nil)
	require.Error(t, err)
}

func TestBuildFullText2Create_BadParams(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildFullText2Create(b, nonNumValTblFunc(), nil, makeArgs(4), nil)
	require.Error(t, err)
}

func TestBuildFullText2Create_OK(t *testing.T) {
	b := newStubPlanBuilder()
	id, err := buildFullText2Create(b, numValTblFunc(`{"parser":"ngram"}`), nil, makeArgs(4), nil)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, FullText2CreateFuncName, node.TableDef.TblFunc.Name)
	require.True(t, node.TableDef.TblFunc.IsSingle)
	require.Len(t, node.TblFuncExprList, 3) // param stripped from the 4 args
}

// --- tablefunc.go: buildFullText2Search ------------------------------------

func TestBuildFullText2Search_BadArgCount(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildFullText2Search(b, numValTblFunc(`{}`), nil, makeArgs(3), nil)
	require.Error(t, err)
	_, err = buildFullText2Search(b, numValTblFunc(`{}`), nil, makeArgs(5), nil)
	require.Error(t, err)
}

func TestBuildFullText2Search_BadParams(t *testing.T) {
	b := newStubPlanBuilder()
	_, err := buildFullText2Search(b, nonNumValTblFunc(), nil, makeArgs(4), nil)
	require.Error(t, err)
}

func TestBuildFullText2Search_OK(t *testing.T) {
	b := newStubPlanBuilder()
	id, err := buildFullText2Search(b, numValTblFunc(`{"parser":"ngram"}`), nil, makeArgs(4), nil)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, plan.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, FullText2SearchFuncName, node.TableDef.TblFunc.Name)
	require.False(t, node.TableDef.TblFunc.IsSingle)
	require.Len(t, node.TblFuncExprList, 3) // param stripped from the 4 args
}
