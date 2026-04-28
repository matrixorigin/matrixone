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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// validateIncludeColumns ----------------------------------------------------

func unresolvedCol(name string) *tree.UnresolvedName {
	return tree.NewUnresolvedColName(name)
}

func TestValidateIncludeColumns_Empty(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	require.NoError(t, validateIncludeColumns(ctx, nil, nil, "v", "id"))
	require.NoError(t, validateIncludeColumns(ctx, []*tree.UnresolvedName{}, nil, "v", "id"))
}

func TestValidateIncludeColumns_OK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{
		"price": {Typ: plan.Type{Id: int32(types.T_float32)}},
		"cat":   {Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	require.NoError(t, validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("price"), unresolvedCol("cat")},
		colMap, "v", "id"))
}

func TestValidateIncludeColumns_VecColumnRejected(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"v": {Typ: plan.Type{Id: int32(types.T_array_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("v")},
		colMap, "v", "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "indexed vector column")
}

func TestValidateIncludeColumns_PKRejected(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"id": {Typ: plan.Type{Id: int32(types.T_int64)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("id")},
		colMap, "v", "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key")
}

func TestValidateIncludeColumns_Duplicate(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"price": {Typ: plan.Type{Id: int32(types.T_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("price"), unresolvedCol("price")},
		colMap, "v", "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestValidateIncludeColumns_NotExist(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"price": {Typ: plan.Type{Id: int32(types.T_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("missing")},
		colMap, "v", "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")
}

func TestValidateIncludeColumns_UnsupportedType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"name": {Typ: plan.Type{Id: int32(types.T_varchar)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("name")},
		colMap, "v", "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestValidateIncludeColumns_AllSupportedNumericTypes(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{
		"a": {Typ: plan.Type{Id: int32(types.T_int32)}},
		"b": {Typ: plan.Type{Id: int32(types.T_int64)}},
		"c": {Typ: plan.Type{Id: int32(types.T_float32)}},
		"d": {Typ: plan.Type{Id: int32(types.T_float64)}},
	}
	require.NoError(t, validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{
			unresolvedCol("a"), unresolvedCol("b"),
			unresolvedCol("c"), unresolvedCol("d"),
		},
		colMap, "v", "id"))
}

// build*SecondaryIndexDef ---------------------------------------------------

// vectorIndexInfoFixture produces a minimal *tree.Index for a 1-column vector
// index, parameterised by KeyType and (optionally) include columns.
func vectorIndexInfoFixture(vecCol string, kt tree.IndexType, includes ...string) *tree.Index {
	idx := &tree.Index{
		KeyType: kt,
		KeyParts: []*tree.KeyPart{
			{ColName: tree.NewUnresolvedColName(vecCol)},
		},
	}
	if len(includes) > 0 {
		idx.IndexOption = &tree.IndexOption{}
		for _, c := range includes {
			idx.IndexOption.IncludeColumns = append(idx.IndexOption.IncludeColumns, unresolvedCol(c))
		}
	}
	return idx
}

func vectorColMap() map[string]*ColDef {
	return map[string]*ColDef{
		"id":    {Typ: plan.Type{Id: int32(types.T_int64)}},
		"v":     {Typ: plan.Type{Id: int32(types.T_array_float32)}},
		"price": {Typ: plan.Type{Id: int32(types.T_float32)}},
	}
}

// CAGRA --------------------------------------------------------------------

func TestBuildCagraSecondaryIndexDef_NoPK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	_, _, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		vectorColMap(), nil, "")
	require.Error(t, err)

	_, _, err = buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		vectorColMap(), nil, catalog.FakePrimaryKeyColName)
	require.Error(t, err)
}

func TestBuildCagraSecondaryIndexDef_PKWrongType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	colMap["id"] = &ColDef{Typ: plan.Type{Id: int32(types.T_varchar)}}
	_, _, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key must be int64")
}

func TestBuildCagraSecondaryIndexDef_MultiCol(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	idx := vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA)
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedColName("price")})
	_, _, err := buildCagraSecondaryIndexDef(ctx, idx, vectorColMap(), nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi column")
}

func TestBuildCagraSecondaryIndexDef_ColMissing(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	delete(colMap, "v")
	_, _, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")
}

func TestBuildCagraSecondaryIndexDef_WrongVecType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	colMap["v"] = &ColDef{Typ: plan.Type{Id: int32(types.T_int64)}}
	_, _, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "VECF32")
}

func TestBuildCagraSecondaryIndexDef_DuplicateOnSameColumn(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	existing := []*plan.IndexDef{{IndexAlgo: "cagra", Parts: []string{"v"}}}
	_, _, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA),
		vectorColMap(), existing, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multiple CAGRA")
}

func TestBuildCagraSecondaryIndexDef_BadIncludeColumn(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	// "id" is the PK — validateIncludeColumns rejects.
	idx := vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA, "id")
	_, _, err := buildCagraSecondaryIndexDef(ctx, idx, vectorColMap(), nil, "id")
	require.Error(t, err)
}

func TestBuildCagraSecondaryIndexDef_OK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	idxDefs, tblDefs, err := buildCagraSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_CAGRA, "price"),
		vectorColMap(), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)
	require.Equal(t, catalog.Cagra_TblType_Metadata, tblDefs[0].TableType)
	require.Equal(t, catalog.Cagra_TblType_Storage, tblDefs[1].TableType)
}

// IVFPQ --------------------------------------------------------------------

func TestBuildIvfpqSecondaryIndexDef_NoPK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	_, _, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		vectorColMap(), nil, "")
	require.Error(t, err)

	_, _, err = buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		vectorColMap(), nil, catalog.FakePrimaryKeyColName)
	require.Error(t, err)
}

func TestBuildIvfpqSecondaryIndexDef_PKWrongType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	colMap["id"] = &ColDef{Typ: plan.Type{Id: int32(types.T_varchar)}}
	_, _, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key must be int64")
}

func TestBuildIvfpqSecondaryIndexDef_MultiCol(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	idx := vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ)
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedColName("price")})
	_, _, err := buildIvfpqSecondaryIndexDef(ctx, idx, vectorColMap(), nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi column")
}

func TestBuildIvfpqSecondaryIndexDef_ColMissing(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	delete(colMap, "v")
	_, _, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")
}

func TestBuildIvfpqSecondaryIndexDef_WrongVecType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := vectorColMap()
	colMap["v"] = &ColDef{Typ: plan.Type{Id: int32(types.T_int64)}}
	_, _, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		colMap, nil, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "VECF32")
}

func TestBuildIvfpqSecondaryIndexDef_DuplicateOnSameColumn(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	existing := []*plan.IndexDef{{IndexAlgo: "ivfpq", Parts: []string{"v"}}}
	_, _, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ),
		vectorColMap(), existing, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multiple IVFPQ")
}

func TestBuildIvfpqSecondaryIndexDef_BadIncludeColumn(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	idx := vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ, "id")
	_, _, err := buildIvfpqSecondaryIndexDef(ctx, idx, vectorColMap(), nil, "id")
	require.Error(t, err)
}

func TestBuildIvfpqSecondaryIndexDef_OK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	idxDefs, tblDefs, err := buildIvfpqSecondaryIndexDef(ctx,
		vectorIndexInfoFixture("v", tree.INDEX_TYPE_IVFPQ, "price"),
		vectorColMap(), nil, "id")
	require.NoError(t, err)
	require.Len(t, idxDefs, 2)
	require.Len(t, tblDefs, 2)
	require.Equal(t, catalog.Ivfpq_TblType_Metadata, tblDefs[0].TableType)
	require.Equal(t, catalog.Ivfpq_TblType_Storage, tblDefs[1].TableType)
}
