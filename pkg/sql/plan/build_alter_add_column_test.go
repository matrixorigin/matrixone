// Copyright 2024 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDropColumnWithIndex(t *testing.T) {
	var def TableDef
	def.Indexes = []*IndexDef{
		{IndexName: "idx",
			IndexAlgo:  "fulltext",
			TableExist: true,
			Unique:     false,
			Parts:      []string{"body", "title"},
		},
	}

	err := handleDropColumnWithIndex(context.TODO(), "body", &def)
	require.Nil(t, err)
	require.Equal(t, 1, len(def.Indexes[0].Parts))
	require.Equal(t, "title", def.Indexes[0].Parts[0])
}

func TestDropColumnRemovesEveryAdjacentSingleColumnIndex(t *testing.T) {
	def := TableDef{Indexes: []*IndexDef{
		{IndexName: "uk_body", Unique: true, Parts: []string{"body"}},
		{
			IndexName: "idx_body",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"body", catalog.CreateAlias("id")},
		},
		{
			IndexName: "idx_body_title",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"body", "title", catalog.CreateAlias("id")},
		},
	}}

	require.NoError(t, handleDropColumnWithIndex(context.Background(), "body", &def))
	require.Len(t, def.Indexes, 1)
	require.Equal(t, "idx_body_title", def.Indexes[0].IndexName)
	require.Equal(t, []string{"title", catalog.CreateAlias("id")}, def.Indexes[0].Parts)
}

func TestCheckGeometryKeyPartTypes(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_geometry)}

	err := checkPrimaryKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in primary key")

	err = checkUniqueKeyPartType(context.Background(), typ, "g")
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")
}

func TestCheckEnumPrimaryKeyPartType(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_enum), Enumvalues: "ACW,BT,XS3"}
	require.NoError(t, checkPrimaryKeyPartType(context.Background(), typ, "source"))
}

func TestCheckNativeUnicodePrimaryKeyPartType(t *testing.T) {
	typ := plan.Type{
		Id:      int32(types.T_varchar),
		Charset: uint32(types.CharsetUTF8MB4UnicodeCI),
	}
	err := checkPrimaryKeyPartType(context.Background(), typ, "source")
	require.Error(t, err)
	require.Contains(t, err.Error(), "native Unicode collation column 'source'")
}

func TestCheckNativeUnicodeUniqueKeyPartType(t *testing.T) {
	typ := plan.Type{
		Id:      int32(types.T_varchar),
		Charset: uint32(types.CharsetUTF8MB4UnicodeCI),
	}
	err := checkUniqueKeyPartType(context.Background(), typ, "source")
	require.Error(t, err)
	require.Contains(t, err.Error(), "native Unicode collation column 'source'")
}

func TestCheckAddColumnWithUniqueKeyVisibility(t *testing.T) {
	tests := []struct {
		name    string
		option  *tree.IndexOption
		visible bool
	}{
		{
			name:    "default is visible",
			visible: true,
		},
		{
			name: "explicit invisible is preserved",
			option: &tree.IndexOption{
				Visible: tree.VISIBLE_TYPE_INVISIBLE,
			},
			visible: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			indexDef, err := checkAddColumWithUniqueKey(context.Background(), &TableDef{}, &tree.UniqueIndex{
				Name:        "idx_a",
				KeyParts:    []*tree.KeyPart{{ColName: tree.NewUnresolvedColName("a")}},
				IndexOption: tc.option,
			})
			require.NoError(t, err)
			got, isSet := catalog.GetIndexVisibility(indexDef)
			require.True(t, isSet)
			require.Equal(t, tc.visible, got)
			require.Equal(t, tc.visible, indexDef.Visible)
		})
	}
}

// TestCheckVectorPrimaryKeyPartTypes verifies ALTER ... ADD PRIMARY KEY rejects
// every vector element type — including the narrow types (bf16/f16/int8/uint8),
// which previously slipped through admission and hit the txn duplicate checker's
// default panic on insert.
func TestCheckVectorPrimaryKeyPartTypes(t *testing.T) {
	for _, id := range []types.T{
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
		types.T_array_float8, types.T_array_float4,
	} {
		require.True(t, id.IsArray(), "%s should be a vector type", id)
		err := checkPrimaryKeyPartType(context.Background(), plan.Type{Id: int32(id)}, "v")
		require.Error(t, err, "type %s must be rejected in primary key", id)
		require.Contains(t, err.Error(), "VECTOR column 'v' cannot be in primary key")
	}
}

func TestCheckIndexedColumnTypeChangeGeometry(t *testing.T) {
	tableDef := &TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*plan.IndexDef{
			{IndexName: "u_g", Unique: true, Parts: []string{"g"}},
			{IndexName: "idx_g", Parts: []string{"g"}},
		},
	}

	oldCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_varchar)}}
	newCol := &ColDef{Name: "g", OriginName: "g", Typ: plan.Type{Id: int32(types.T_geometry)}}

	err := checkIndexedColumnTypeChange(context.Background(), tableDef, oldCol, newCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot be in unique index")

	pkOldCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int64)}}
	pkNewCol := &ColDef{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_geometry)}}
	err = checkIndexedColumnTypeChange(context.Background(), tableDef, pkOldCol, pkNewCol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'id' cannot be in primary key")
}

// TestLowPrecisionFloatKeyParts checks that bf16, float16, float8 and float4 columns are
// rejected as primary key, unique key, index and cluster by parts, and accepted as plain
// columns.
func TestLowPrecisionFloatKeyParts(t *testing.T) {
	ctx := context.Background()
	for _, id := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		typ := plan.Type{Id: int32(id)}
		err := checkPrimaryKeyPartType(ctx, typ, "k")
		require.ErrorContains(t, err, id.String()+" column 'k' cannot be in primary key")
		err = checkUniqueKeyPartType(ctx, typ, "k")
		require.ErrorContains(t, err, id.String()+" column 'k' cannot be in unique index")
		col := &ColDef{Typ: typ}
		key := &tree.KeyPart{ColName: tree.NewUnresolvedColName("k")}
		for kind, want := range map[string]string{
			"primary":   "cannot be in primary key",
			"unique":    "cannot be in unique index",
			"secondary": "cannot be in index",
		} {
			err = checkIndexColumnSupportability(ctx, col, key, kind)
			require.ErrorContains(t, err, id.String()+" column 'k' "+want)
		}
		require.ErrorContains(t, lowPrecisionKeyError(ctx, int32(id), "k", "cluster"), "cannot be a cluster by key")
	}
	for _, id := range []types.T{types.T_float32, types.T_float64, types.T_int32} {
		require.NoError(t, lowPrecisionKeyError(ctx, int32(id), "k", "primary"))
		require.NoError(t, checkPrimaryKeyPartType(ctx, plan.Type{Id: int32(id)}, "k"))
	}

	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, typ := range []string{"bf16", "float16", "float8", "float4"} {
		for _, sql := range []string{
			"create table lp (k " + typ + " primary key, v int)",
			"create table lp (id int primary key, k " + typ + " unique key)",
			"create table lp (k " + typ + ", j int, primary key (k, j))",
			"create table lp (id int primary key, k " + typ + ", unique key uk (k))",
			"create table lp (id int primary key, k " + typ + ", key ik (k))",
			"create table lp (id int, k " + typ + ") cluster by (k)",
			"create table lp (id int, k " + typ + ") cluster by (id, k)",
		} {
			_, err := runOneStmt(mock, t, sql)
			require.ErrorContains(t, err, "cannot be", sql)
		}
		_, err := runOneStmt(mock, t, "create table lp (id int primary key, k "+typ+")")
		require.NoError(t, err, typ)
	}
	// vecf8/vecf4 cells equal by decoded value can differ in bytes: no cluster by key
	for _, typ := range []string{"vecf8(4)", "vecf4(4)"} {
		for _, sql := range []string{
			"create table lp (id int, v " + typ + ") cluster by (v)",
			"create table lp (id int, v " + typ + ") cluster by (id, v)",
		} {
			_, err := runOneStmt(mock, t, sql)
			require.ErrorContains(t, err, "cannot be a cluster by key", sql)
		}
	}
	_, err := runOneStmt(mock, t, "create table lp (id int, v vecf32(4)) cluster by (v)")
	require.NoError(t, err)
}

// TestBuildColumnDomainExprLowPrecisionFloat checks that a column-domain rewrite over a
// bf16/float16/float8/float4 column builds a disjunction of equalities, since these types
// have no IN kernel.
func TestBuildColumnDomainExprLowPrecisionFloat(t *testing.T) {
	ctx := context.Background()
	for _, id := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		col := &plan.Expr{
			Typ:  plan.Type{Id: int32(id)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "f"}},
		}
		vals := []*plan.Expr{makePlan2Float32ConstExprWithType(0.5), makePlan2Float32ConstExprWithType(1.5)}
		expr, err := buildColumnDomainExpr(ctx, col, vals)
		require.NoError(t, err, id.String())
		require.Equal(t, "or", expr.GetF().Func.ObjName, id.String())
	}
}
