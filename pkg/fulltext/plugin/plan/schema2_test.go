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

// CPU-side unit coverage for the VERSION=2 schema branch of BuildFullTextIndexDefs
// (fulltext v2 builds a chunked storage + metadata layout instead of the classic
// v1 postings table). The end-to-end path is BVT-only (mo-tester), which does not
// contribute to Go per-package coverage, so this pins the hidden-table shape.
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

// init wires the planplugin helper var buildFullText2IndexDefs uses on the happy
// path. Production wires it in pkg/sql/plan's init; importing that here would be a
// cycle, so we substitute a shallow stand-in (only the returned shape matters).
func init() {
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

func ftIndex(col string, version int64) *tree.FullTextIndex {
	return &tree.FullTextIndex{
		Name:        "ft",
		KeyParts:    []*tree.KeyPart{{ColName: tree.NewUnresolvedName(tree.NewCStr(col, 0))}},
		IndexOption: &tree.IndexOption{Version: version},
	}
}

func ftColMap(pkName, txtName string) map[string]*plan.ColDef {
	return map[string]*plan.ColDef{
		pkName:  {Name: pkName, Typ: plan.Type{Id: int32(types.T_int64)}},
		txtName: {Name: txtName, Typ: plan.Type{Id: int32(types.T_text)}},
	}
}

// TestBuildFullTextIndexDefs_V2 pins the VERSION=2 hidden-table shape: two defs
// (storage + metadata), both still algo="fulltext", distinguished by
// IndexAlgoTableType, carrying the version param.
func TestBuildFullTextIndexDefs_V2(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildFullTextIndexDefs(
		newStubCompilerContext(), ftIndex("body", 2), ftColMap("id", "body"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)

	require.Equal(t, catalog.FullText2Index_TblType_Storage, tblDefs[0].TableType)
	require.Equal(t, catalog.FullText2Index_TblType_Metadata, tblDefs[1].TableType)
	require.NotNil(t, tblDefs[0].Pkey)
	require.NotNil(t, tblDefs[1].Pkey)
	// storage table has the (index_id, chunk_id, data, tag, cpk) columns.
	require.Len(t, tblDefs[0].Cols, 5)

	for _, d := range idxDefs {
		require.Equal(t, tree.INDEX_TYPE_FULLTEXT.ToString(), d.IndexAlgo, "v2 stays algo=fulltext")
		require.EqualValues(t, 2, catalog.IndexAlgoParamVersionOf(d.IndexAlgoParams))
	}
	require.Equal(t, catalog.FullText2Index_TblType_Storage, idxDefs[0].IndexAlgoTableType)
	require.Equal(t, catalog.FullText2Index_TblType_Metadata, idxDefs[1].IndexAlgoTableType)
}

// TestBuildFullTextIndexDefs_V1 confirms the classic path is unchanged: one
// postings table, no version param.
func TestBuildFullTextIndexDefs_V1(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildFullTextIndexDefs(
		newStubCompilerContext(), ftIndex("body", 0), ftColMap("id", "body"), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 1)
	require.Len(t, tblDefs, 1)
	require.Equal(t, catalog.FullTextIndex_TblType, tblDefs[0].TableType)
	require.EqualValues(t, 1, catalog.IndexAlgoParamVersionOf(idxDefs[0].IndexAlgoParams))
}
