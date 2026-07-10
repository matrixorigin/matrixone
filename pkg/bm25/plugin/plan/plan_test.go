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

// CPU-side unit coverage for plan.go (inert CanApply/ApplyForSort) and schema.go
// (BuildSecondaryIndexDefs / BuildFullTextIndexDefs). bm25's BVTs run via mo-tester
// (integration), which does not contribute to Go per-package line coverage, so
// without these the package reads ~0% in CI. The pk-type validation tests below are
// also the regression guard for the "unsupported pk silently aborts CDC" fix.
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

// init wires the planplugin helper vars BuildSecondaryIndexDefs calls on the happy
// path. Production wires them in pkg/sql/plan's init; importing that from a plugin
// test would be a cycle, so we substitute shallow stand-ins (only the shape of the
// returned value matters here).
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

type stubCompilerContext struct{ ctx context.Context }

func (c stubCompilerContext) GetContext() context.Context { return c.ctx }
func (c stubCompilerContext) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

var _ planplugin.CompilerContext = stubCompilerContext{}

func newStubCompilerContext() stubCompilerContext {
	return stubCompilerContext{ctx: context.Background()}
}

// bm25ColMap returns a colMap with an int64 pk column and a text column — the shape
// BuildSecondaryIndexDefs expects on the happy path.
func bm25ColMap(pkName, txtName string, pkType types.T) map[string]*plan.ColDef {
	return map[string]*plan.ColDef{
		pkName:  {Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
		txtName: {Name: txtName, Typ: plan.Type{Id: int32(types.T_text)}},
	}
}

func indexOn(colName string) *tree.Index {
	un := tree.NewUnresolvedName(tree.NewCStr(colName, 0))
	return &tree.Index{KeyParts: []*tree.KeyPart{{ColName: un}}}
}

// --- plan.go: inert vector hooks -------------------------------------------

func TestCanApply_Inert(t *testing.T) {
	ok, err := Hooks{}.CanApply(nil, &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{})
	require.NoError(t, err)
	require.False(t, ok, "bm25 must not apply for ORDER BY <score> LIMIT rewrites")
}

func TestApplyForSort_Inert(t *testing.T) {
	id, changed, err := Hooks{}.ApplyForSort(nil, &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 42, planplugin.ApplyForSortOpts{})
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, int32(42), id, "ApplyForSort must return the node id unchanged")
}

// --- schema.go: BuildSecondaryIndexDefs error paths ------------------------

func TestBuildSecondaryIndexDefs_MultiColumn(t *testing.T) {
	idx := indexOn("body")
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedName(tree.NewCStr("body2", 0))})
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, bm25ColMap("id", "body", types.T_int64), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_ColNotExist(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("nope"), bm25ColMap("id", "body", types.T_int64), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_NotTextColumn(t *testing.T) {
	colMap := bm25ColMap("id", "body", types.T_int64)
	colMap["body"].Typ.Id = int32(types.T_int64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), colMap, nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_DuplicateColumn(t *testing.T) {
	existed := []*plan.IndexDef{{
		IndexAlgo: catalog.MoIndexBm25Algo.ToString(),
		Parts:     []string{"body"},
	}}
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), bm25ColMap("id", "body", types.T_int64), existed, "id")
	require.Error(t, err)
}

// TestBuildSecondaryIndexDefs_UnsupportedPk: a single-column pk of a type encodePk
// cannot round-trip must be rejected at CREATE (else the CDC sink silently aborts).
func TestBuildSecondaryIndexDefs_UnsupportedPk(t *testing.T) {
	for _, pk := range []types.T{
		types.T_int8, types.T_int16, types.T_uint8, types.T_uint16,
		types.T_float32, types.T_float64, types.T_bit, types.T_bool,
	} {
		colMap := bm25ColMap("id", "body", pk)
		_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), colMap, nil, "id")
		require.Error(t, err, "pk type %s must be rejected at CREATE", pk)
	}
}

// TestBuildSecondaryIndexDefs_SupportedPk: every pk type encodePk handles is accepted
// (mirror of encodePk's switch — keep the two in lockstep).
func TestBuildSecondaryIndexDefs_SupportedPk(t *testing.T) {
	for _, pk := range []types.T{
		types.T_int64, types.T_uint64, types.T_int32, types.T_uint32,
		types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_uuid,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128,
	} {
		colMap := bm25ColMap("id", "body", pk)
		idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), colMap, nil, "id")
		require.NoError(t, err, "pk type %s must be accepted", pk)
		require.Len(t, idxDefs, 2)
		require.Len(t, tblDefs, 2)
	}
}

// TestBuildSecondaryIndexDefs_CompositePkOK: a composite pk is delivered as the
// packed CPrimaryKey varchar, which is NOT in colMap; validation must skip it (not
// reject) so composite-pk tables can be bm25-indexed.
func TestBuildSecondaryIndexDefs_CompositePkOK(t *testing.T) {
	colMap := bm25ColMap("id", "body", types.T_int64)
	idxDefs, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), colMap, nil, catalog.CPrimaryKeyColName)
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
}

func TestBuildSecondaryIndexDefs_OK(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("body"), bm25ColMap("id", "body", types.T_int64), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)
	require.Equal(t, catalog.Bm25Index_TblType_Storage, tblDefs[0].TableType)
	require.Equal(t, catalog.Bm25Index_TblType_Metadata, tblDefs[1].TableType)
	require.NotNil(t, tblDefs[0].Pkey)
	require.NotNil(t, tblDefs[1].Pkey)
}

// --- schema.go: BuildFullTextIndexDefs -------------------------------------

func TestBuildFullTextIndexDefs_Unsupported(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(newStubCompilerContext(), nil, nil, nil, "")
	require.Error(t, err)
}
