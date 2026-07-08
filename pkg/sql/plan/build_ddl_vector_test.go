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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// validateIncludeColumns ----------------------------------------------------
//
// Tests for the algo-independent INCLUDE-column validator. CAGRA / IVF-PQ
// specific tests live in build_ddl_vector_gpu_test.go (//go:build gpu).

func unresolvedCol(name string) *tree.UnresolvedName {
	return tree.NewUnresolvedColName(name)
}

// includeTestSupportedTypes mirrors the CAGRA / IVF-PQ plugin's
// SupportedIncludeColumnTypes() set, threaded into validateIncludeColumns.
var includeTestSupportedTypes = []types.T{
	types.T_int32, types.T_int64, types.T_float32, types.T_float64,
}

func TestValidateIncludeColumns_Empty(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	require.NoError(t, validateIncludeColumns(ctx, nil, nil, "v", "id", includeTestSupportedTypes))
	require.NoError(t, validateIncludeColumns(ctx, []*tree.UnresolvedName{}, nil, "v", "id", includeTestSupportedTypes))
}

func TestValidateIncludeColumns_OK(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{
		"price": {Typ: plan.Type{Id: int32(types.T_float32)}},
		"cat":   {Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	require.NoError(t, validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("price"), unresolvedCol("cat")},
		colMap, "v", "id", includeTestSupportedTypes))
}

func TestValidateIncludeColumns_VecColumnRejected(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"v": {Typ: plan.Type{Id: int32(types.T_array_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("v")},
		colMap, "v", "id", includeTestSupportedTypes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "indexed vector column")
}

func TestValidateIncludeColumns_PKRejected(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"id": {Typ: plan.Type{Id: int32(types.T_int64)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("id")},
		colMap, "v", "id", includeTestSupportedTypes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key")
}

func TestValidateIncludeColumns_Duplicate(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"price": {Typ: plan.Type{Id: int32(types.T_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("price"), unresolvedCol("price")},
		colMap, "v", "id", includeTestSupportedTypes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestValidateIncludeColumns_NotExist(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"price": {Typ: plan.Type{Id: int32(types.T_float32)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("missing")},
		colMap, "v", "id", includeTestSupportedTypes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not exist")
}

func TestValidateIncludeColumns_UnsupportedType(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	colMap := map[string]*ColDef{"name": {Typ: plan.Type{Id: int32(types.T_varchar)}}}
	err := validateIncludeColumns(ctx,
		[]*tree.UnresolvedName{unresolvedCol("name")},
		colMap, "v", "id", includeTestSupportedTypes)
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
		colMap, "v", "id", includeTestSupportedTypes))
}
