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

// Unit coverage for plan.go (Hooks redirects), schema.go (BuildSecondaryIndexDefs /
// BuildFullTextIndexDefs) and tablefunc.go. Mirrors ivfpq/plugin/plan.
package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// init wires the planplugin helper vars BuildSecondaryIndexDefs calls. Production
// wires them in pkg/sql/plan's init, which cannot be imported here; these are shape-only.
func init() {
	if planplugin.CreateIndexDef == nil {
		planplugin.CreateIndexDef = func(_ planplugin.CompilerContext, _ *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, _ bool) (*planpb.IndexDef, error) {
			return &planpb.IndexDef{
				IndexTableName:     indexTableName,
				IndexAlgoTableType: indexAlgoTableType,
				Parts:              indexParts,
			}, nil
		}
	}
	if planplugin.MakeHiddenColDefByName == nil {
		planplugin.MakeHiddenColDefByName = func(name string) *planpb.ColDef {
			return &planpb.ColDef{Name: name, Typ: planpb.Type{Id: int32(types.T_varchar)}}
		}
	}
	if planplugin.DeepCopyColDefList == nil {
		planplugin.DeepCopyColDefList = func(in []*planpb.ColDef) []*planpb.ColDef { return in }
	}
}

// stubPlanBuilder records appended nodes. The Apply*/CanApply* variants panic with
// their algorithm name; the redirect tests assert the recovered value.
type stubPlanBuilder struct {
	ctx     context.Context
	nodes   []*planpb.Node
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

func (b *stubPlanBuilder) AppendNode(node *planpb.Node, _ planplugin.BindContext) int32 {
	id := int32(len(b.nodes))
	b.nodes = append(b.nodes, node)
	return id
}

func (b *stubPlanBuilder) ApplyIndicesForSortUsingHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("hnsw")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("cagra")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("ivfpq")
}
func (b *stubPlanBuilder) ApplyIndicesForSortUsingIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, _ int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	panic("ivfflat")
}
func (b *stubPlanBuilder) CanApplyHnsw(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("hnsw")
}
func (b *stubPlanBuilder) CanApplyCagra(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("cagra")
}
func (b *stubPlanBuilder) CanApplyIvfpq(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("ivfpq")
}
func (b *stubPlanBuilder) CanApplyIvfflat(_ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	panic("ivfflat")
}

var _ planplugin.PlanBuilder = (*stubPlanBuilder)(nil)

func newStubCompilerContext() vecViewCtx { return vecViewCtx{} }

// vecColMap returns a colMap with an int64 pk and a vecf32 vector column, the
// shape BuildSecondaryIndexDefs expects on the happy path.
func vecColMap(pkName, vecName string) map[string]*planpb.ColDef {
	return map[string]*planpb.ColDef{
		pkName:  {Name: pkName, Typ: planpb.Type{Id: int32(types.T_int64)}},
		vecName: {Name: vecName, Typ: planpb.Type{Id: int32(types.T_array_float32)}},
	}
}

func indexOn(colName string) *tree.Index {
	un := tree.NewUnresolvedName(tree.NewCStr(colName, 0))
	return &tree.Index{KeyParts: []*tree.KeyPart{{ColName: un}}}
}

// --- plan.go: redirects ----------------------------------------------------

func TestHooks_Redirects(t *testing.T) {
	for _, c := range []struct {
		name string
		call func()
	}{
		{"CanApply", func() {
			_, _ = Hooks{}.CanApply(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{})
		}},
		{"ApplyForSort", func() {
			_, _, _ = Hooks{}.ApplyForSort(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 0, planplugin.ApplyForSortOpts{})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() { require.Equal(t, "hnsw", recover()) }()
			c.call()
		})
	}
}

// --- schema.go: BuildSecondaryIndexDefs ------------------------------------

// The happy path builds the two HNSW tables (metadata + storage).
func TestBuildSecondaryIndexDefs_OK(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)
	require.Equal(t, catalog.Hnsw_TblType_Metadata, tblDefs[0].TableType)
	require.Equal(t, catalog.Hnsw_TblType_Storage, tblDefs[1].TableType)
	for _, td := range tblDefs {
		require.NotEmpty(t, td.Cols)
		for _, col := range td.Cols {
			require.NotNil(t, col, "every column slot is filled")
		}
	}
}

// Keep the metadata shape compatible with existing CDC/rebuild writers.
func TestBuildSecondaryIndexDefs_MetadataCompatibility(t *testing.T) {
	_, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "id")
	require.NoError(t, err)

	require.Len(t, tblDefs[0].Cols, 4)
	names := make(map[string]planpb.Type, len(tblDefs[0].Cols))
	for _, col := range tblDefs[0].Cols {
		names[col.Name] = col.Typ
	}
	for _, want := range []string{
		catalog.Hnsw_TblCol_Metadata_Timestamp,
		catalog.Hnsw_TblCol_Metadata_Filesize,
	} {
		typ, ok := names[want]
		require.True(t, ok, "metadata table declares %s", want)
		require.Equal(t, int32(types.T_int64), typ.Id)
	}
}

func TestBuildSecondaryIndexDefs_EmptyPkey(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_FakePkey(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, catalog.FakePrimaryKeyColName)
	require.Error(t, err)
}

// HNSW requires a bigint primary key.
func TestBuildSecondaryIndexDefs_NonBigintPkey(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["id"].Typ.Id = int32(types.T_varchar)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.Error(t, err)
}

// HNSW has no INCLUDE support.
func TestBuildSecondaryIndexDefs_IncludeRefused(t *testing.T) {
	idx := indexOn("vec")
	idx.IndexOption = &tree.IndexOption{
		IncludeColumns: []*tree.UnresolvedName{tree.NewUnresolvedName(tree.NewCStr("payload", 0))},
	}
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_MultiColumn(t *testing.T) {
	idx := indexOn("vec")
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedName(tree.NewCStr("vec2", 0))})
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_ColumnNotExist(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("nope"), vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_NotAVector(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["vec"].Typ.Id = int32(types.T_int64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.Error(t, err)
}

// Unlike CAGRA/IVF-PQ, HNSW accepts a vecf64 base column.
func TestBuildSecondaryIndexDefs_F64Base(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["vec"].Typ.Id = int32(types.T_array_float64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.NoError(t, err)
}

// A second HNSW index on the same column is rejected.
func TestBuildSecondaryIndexDefs_DuplicateColumn(t *testing.T) {
	existed := []*planpb.IndexDef{{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		Parts:     []string{"vec"},
	}}
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), existed, "id")
	require.Error(t, err)
}

// --- schema.go: BuildFullTextIndexDefs -------------------------------------

// The fulltext hook errors for a vector plugin.
func TestBuildFullTextIndexDefs_NotSupported(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(
		newStubCompilerContext(), &tree.FullTextIndex{}, vecColMap("id", "body"), nil, "id")
	require.Error(t, err)
}

// --- tablefunc.go ----------------------------------------------------------

func newStringNumValFn(s string) *tree.FuncExpr {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.FuncExpr{Exprs: tree.Exprs{nv}}
}

func newNonNumValFn() *tree.FuncExpr {
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	return &tree.FuncExpr{Exprs: tree.Exprs{un}}
}

func makeBuildArgs(n int) []*planpb.Expr {
	out := make([]*planpb.Expr, 0, n)
	out = append(out, &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "{}"}}},
	})
	for i := 1; i < n; i++ {
		out = append(out, &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: int64(i)}}},
		})
	}
	return out
}

func makeNumValTblFunc(s string) *tree.TableFunction {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{nv}}}
}

func nonLiteralTblFunc() *tree.TableFunction {
	un := tree.NewUnresolvedName(tree.NewCStr("x", 0))
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: tree.Exprs{un}}}
}

func TestGetHnswParams_OK(t *testing.T) {
	out, err := getHnswParams(newStubPlanBuilder(), newStringNumValFn(`{"m":"48"}`))
	require.NoError(t, err)
	require.Equal(t, `{"m":"48"}`, out)
}

func TestGetHnswParams_Error(t *testing.T) {
	_, err := getHnswParams(newStubPlanBuilder(), newNonNumValFn())
	require.Error(t, err)
}

func TestBuildHnswCreate_TooFewArgs(t *testing.T) {
	_, err := buildHnswCreate(newStubPlanBuilder(), makeNumValTblFunc(`{}`), nil, makeBuildArgs(3), nil)
	require.Error(t, err)
}

func TestBuildHnswCreate_BadParams(t *testing.T) {
	_, err := buildHnswCreate(newStubPlanBuilder(), nonLiteralTblFunc(), nil, makeBuildArgs(4), nil)
	require.Error(t, err)
}

// The happy path appends a FUNCTION_SCAN, moves the first argument into Param,
// and marks the node single-threaded.
func TestBuildHnswCreate_OK(t *testing.T) {
	b := newStubPlanBuilder()
	id, err := buildHnswCreate(b, makeNumValTblFunc(`{"m":"48"}`), nil, makeBuildArgs(4), nil)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, planpb.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, HNSWCreateFuncName, node.TableDef.TblFunc.Name)
	require.Equal(t, `{"m":"48"}`, string(node.TableDef.TblFunc.Param))
	require.Len(t, node.TblFuncExprList, 3)
	require.True(t, node.TableDef.TblFunc.IsSingle)
}

// Search takes 3 args, or 4 with a filter payload.
func TestBuildHnswSearch_BadArgCount(t *testing.T) {
	_, err := buildHnswSearch(newStubPlanBuilder(), makeNumValTblFunc(`{}`), nil, makeBuildArgs(2), nil)
	require.Error(t, err)
	_, err = buildHnswSearch(newStubPlanBuilder(), makeNumValTblFunc(`{}`), nil, makeBuildArgs(5), nil)
	require.Error(t, err)
}

func TestBuildHnswSearch_BadParams(t *testing.T) {
	_, err := buildHnswSearch(newStubPlanBuilder(), nonLiteralTblFunc(), nil, makeBuildArgs(3), nil)
	require.Error(t, err)
}

// Search is not single-threaded, unlike create.
func TestBuildHnswSearch_OK(t *testing.T) {
	for _, n := range []int{3, 4} {
		b := newStubPlanBuilder()
		id, err := buildHnswSearch(b, makeNumValTblFunc(`{"m":"48"}`), nil, makeBuildArgs(n), nil)
		require.NoError(t, err)
		node := b.nodes[id]
		require.Equal(t, planpb.Node_FUNCTION_SCAN, node.NodeType)
		require.Equal(t, HNSWSearchFuncName, node.TableDef.TblFunc.Name)
		require.Len(t, node.TblFuncExprList, n-1)
		require.False(t, node.TableDef.TblFunc.IsSingle)
	}
}
