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

// Unit coverage for plan.go (Hooks redirects) and
// schema.go (BuildSecondaryIndexDefs / BuildFullTextIndexDefs). These
// CPU-side plan-construction paths were previously exercised only by the
// GPU-gated BVT suite, so they read 0% in non-GPU CI; the tests below run
// without a GPU. stubPlanBuilder + the init() that wires
// DeepCopyColDefList live in tablefunc_test.go (same package).
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

// init wires the two planplugin helper vars the BuildSecondaryIndexDefs
// happy path calls. Production wires them in pkg/sql/plan's init, but
// importing that from a plugin test would create a cycle, so the test
// substitutes shallow stand-ins (only the shape of the returned value
// matters for these tests).
func init() {
	if planplugin.CreateIndexDef == nil {
		planplugin.CreateIndexDef = func(_ planplugin.CompilerContext, _ *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, _ bool) (*plan.IndexDef, error) {
			return &plan.IndexDef{
				IndexTableName:     indexTableName,
				IndexAlgoTableType: indexAlgoTableType,
				Parts:              indexParts,
			}, nil
		}
	}
	if planplugin.MakeHiddenColDefByName == nil {
		planplugin.MakeHiddenColDefByName = func(name string) *plan.ColDef {
			return &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(types.T_varchar)}}
		}
	}
}

// stubCompilerContext is the minimal planplugin.CompilerContext — the
// interface is a single GetContext() method.
type stubCompilerContext struct{ ctx context.Context }

func (c stubCompilerContext) GetContext() context.Context { return c.ctx }
func (c stubCompilerContext) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

var _ planplugin.CompilerContext = stubCompilerContext{}

func newStubCompilerContext() stubCompilerContext {
	return stubCompilerContext{ctx: context.Background()}
}

// vecColMap returns a colMap with an int64 pk column and a vecf32 vector
// column, the shape BuildSecondaryIndexDefs expects on the happy path.
func vecColMap(pkName, vecName string) map[string]*plan.ColDef {
	return map[string]*plan.ColDef{
		pkName:  {Name: pkName, Typ: plan.Type{Id: int32(types.T_int64)}},
		vecName: {Name: vecName, Typ: plan.Type{Id: int32(types.T_array_float32)}},
	}
}

// indexOn builds a single-column *tree.Index over colName.
func indexOn(colName string) *tree.Index {
	un := tree.NewUnresolvedName(tree.NewCStr(colName, 0))
	return &tree.Index{KeyParts: []*tree.KeyPart{{ColName: un}}}
}

// --- plan.go ---------------------------------------------------------------

func TestCanApply_Redirects(t *testing.T) {
	// CanApplyCagra on the shared stub panics; recover confirms the
	// redirect line executed (and routes to the cagra variant).
	defer func() {
		require.NotNil(t, recover(), "CanApply must reach pb.CanApplyCagra")
	}()
	_, _ = Hooks{}.CanApply(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{})
}

func TestApplyForSort_Redirects(t *testing.T) {
	defer func() {
		require.NotNil(t, recover(), "ApplyForSort must reach pb.ApplyIndicesForSortUsingCagra")
	}()
	_, _, _ = Hooks{}.ApplyForSort(newStubPlanBuilder(), &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 0, planplugin.ApplyForSortOpts{})
}

// --- schema.go: BuildSecondaryIndexDefs error paths ------------------------

func TestBuildSecondaryIndexDefs_EmptyPkey(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_FakePkey(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, catalog.FakePrimaryKeyColName)
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_PkNotInColMap(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "missing")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_PkNotInt64(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["id"].Typ.Id = int32(types.T_varchar)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_MultiColumn(t *testing.T) {
	idx := indexOn("vec")
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedName(tree.NewCStr("vec2", 0))})
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_VecColNotExist(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("nope"), vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_NotVecf32(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["vec"].Typ.Id = int32(types.T_int64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_DuplicateColumn(t *testing.T) {
	existed := []*plan.IndexDef{{
		IndexAlgo: catalog.MoIndexCagraAlgo.ToString(),
		Parts:     []string{"vec"},
	}}
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), existed, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_OK(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)
	require.Equal(t, catalog.Cagra_TblType_Metadata, tblDefs[0].TableType)
	require.Equal(t, catalog.Cagra_TblType_Storage, tblDefs[1].TableType)
	require.Len(t, tblDefs[0].Cols, 4)
	require.Len(t, tblDefs[1].Cols, 5)
	require.NotNil(t, tblDefs[0].Pkey)
	require.NotNil(t, tblDefs[1].Pkey)
}

// indexOnQuant builds a single-column *tree.Index over colName with a
// QUANTIZATION option.
func indexOnQuant(colName, quant string) *tree.Index {
	idx := indexOn(colName)
	idx.IndexOption = &tree.IndexOption{Quantization: quant}
	return idx
}

// f16ColMap returns a colMap with an int64 pk and a vecf16 base column.
func f16ColMap(pkName, vecName string) map[string]*plan.ColDef {
	m := vecColMap(pkName, vecName)
	m[vecName].Typ.Id = int32(types.T_array_float16)
	return m
}

// TestBuildSecondaryIndexDefs_F16Base: a vecf16 base column is accepted.
func TestBuildSecondaryIndexDefs_F16Base(t *testing.T) {
	idxDefs, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), f16ColMap("id", "vec"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
}

// TestBuildSecondaryIndexDefs_UnsupportedBase: only vecf32 / vecf16 are valid
// base columns; vecf64 / vecbf16 / vecint8 / vecuint8 are rejected.
func TestBuildSecondaryIndexDefs_UnsupportedBase(t *testing.T) {
	for _, oid := range []types.T{
		types.T_array_float64, types.T_array_bf16, types.T_array_int8, types.T_array_uint8,
	} {
		colMap := vecColMap("id", "vec")
		colMap["vec"].Typ.Id = int32(oid)
		_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
		require.Error(t, err, "base type %s must be rejected", oid)
	}
}

// TestBuildSecondaryIndexDefs_F16UpcastRejected: vecf16 base + QUANTIZATION
// float32 is an upcast (4 > 2 bytes) and must be rejected by the downcast
// guard. (The accepted downcast path — f16 -> int8/uint8 — is exercised
// end-to-end by the GPU functional BVT, since the full def build past the
// guard needs a richer compiler context than this stub provides.)
func TestBuildSecondaryIndexDefs_F16UpcastRejected(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOnQuant("vec", "float32"), f16ColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

// TestBuildSecondaryIndexDefs_BF16QuantRejected: QUANTIZATION 'bf16' has no GPU
// bfloat16 storage (cuVS has no bfloat16 index/quantizer), so it must be rejected
// rather than silently falling back to f32 storage — even though it passes the
// downcast width guard (bf16 is 2 bytes). Rejected on both f32 and f16 bases.
func TestBuildSecondaryIndexDefs_BF16QuantRejected(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOnQuant("vec", "bf16"), vecColMap("id", "vec"), nil, "id")
	require.Error(t, err, "f32 base + bf16 quant must be rejected")
	_, _, err = Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOnQuant("vec", "bf16"), f16ColMap("id", "vec"), nil, "id")
	require.Error(t, err, "f16 base + bf16 quant must be rejected")
}

// --- schema.go: BuildFullTextIndexDefs -------------------------------------

func TestBuildFullTextIndexDefs_Unsupported(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), nil, nil, nil, "")
	require.Error(t, err)
}
