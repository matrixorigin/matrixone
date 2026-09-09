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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func v2PlannerTextType(charset uint32) planpb.Type {
	return planpb.Type{Id: int32(types.T_varchar), Width: 128, Charset: charset}
}

func v2PlannerMetadata() *planpb.UniqueKeyCodecVersion {
	metadata := collationkey.NewCollationAwareMetadata()
	return &planpb.UniqueKeyCodecVersion{
		Value:                metadata.Version,
		RegistryVersion:      metadata.RegistryVersion,
		RegistryDigest:       metadata.RegistryDigest,
		MaxEncodedKeyBytes:   metadata.MaxEncodedKeyBytes,
		ActivationGeneration: metadata.ActivationGeneration,
	}
}

func TestMakeCollationKeyV2ExprUsesPlannerOnlyFunction(t *testing.T) {
	value := &planpb.Expr{Typ: v2PlannerTextType(uint32(types.CharsetUTF8))}
	expr, err := makeCollationKeyV2Expr(value, 4)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), expr.Typ.Id)
	require.Equal(t, uint32(types.CharsetBinary), expr.Typ.Charset)
	require.NotNil(t, expr.GetF())
	require.Equal(t, function.CollationKeyV2FunctionEncodedID, expr.GetF().GetFunc().GetObj())
	require.Len(t, expr.GetF().GetArgs(), 3)
	require.Equal(t, int64(4), expr.GetF().GetArgs()[1].GetLit().GetI64Val())
	require.Equal(t, int64(types.CharsetUTF8), expr.GetF().GetArgs()[2].GetLit().GetI64Val())

	_, err = makeCollationKeyV2Expr(&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_char), Charset: uint32(types.CharsetUTF8)}}, 0)
	require.Error(t, err)
}

func TestMakeCollationCompositeKeyV2ExprFramesAllParts(t *testing.T) {
	values := []*planpb.Expr{
		{Typ: v2PlannerTextType(uint32(types.CharsetUTF8))},
		{Typ: v2PlannerTextType(uint32(types.CharsetUTF8MB4Bin))},
	}
	expr, err := makeCollationCompositeKeyV2Expr(values, []int{2, 8})
	require.NoError(t, err)
	require.Equal(t, function.CollationCompositeKeyV2FunctionEncodedID, expr.GetF().GetFunc().GetObj())
	require.Len(t, expr.GetF().GetArgs(), 6)
	require.Equal(t, int64(2), expr.GetF().GetArgs()[1].GetLit().GetI64Val())
	require.Equal(t, int64(types.CharsetUTF8), expr.GetF().GetArgs()[2].GetLit().GetI64Val())
	require.Equal(t, int64(8), expr.GetF().GetArgs()[4].GetLit().GetI64Val())
	require.Equal(t, int64(types.CharsetUTF8MB4Bin), expr.GetF().GetArgs()[5].GetLit().GetI64Val())

	_, err = makeCollationCompositeKeyV2Expr(values, []int{1})
	require.Error(t, err)
}

func TestMakeUniqueIndexKeyExprFromInputExprsRejectsUnsupportedV2Part(t *testing.T) {
	value := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_char), Charset: uint32(types.CharsetUTF8)}}
	_, err := makeCollationKeyV2Expr(value, 0)
	require.Error(t, err)
}

func TestMakeUniqueIndexKeyExprFromInputExprsRejectsMalformedShape(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	table := &planpb.TableDef{
		Name:                  "t_v2_shape",
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}
	index := &planpb.IndexDef{Parts: []string{"a", "b"}, Unique: true}
	value := &planpb.Expr{Typ: v2PlannerTextType(uint32(types.CharsetUTF8))}

	_, err := builder.makeUniqueIndexKeyExprFromInputExprs(table, index, []*planpb.Expr{value}, nil)
	require.Error(t, err)

	_, err = builder.makeUniqueIndexKeyExprFromInputExprs(
		table, &planpb.IndexDef{Parts: []string{"a"}, Unique: true},
		[]*planpb.Expr{value}, map[string]int{"a": 0},
	)
	require.Error(t, err)
}

func TestUniqueKeyPrefixLengthRejectsInvalidDeclaredValue(t *testing.T) {
	_, err := uniqueKeyPrefixLength(map[string]int{"name": 0}, "name")
	require.Error(t, err)
	_, err = uniqueKeyPrefixLength(map[string]int{"name": -1}, catalog.CreateAlias("name"))
	require.Error(t, err)
	length, err := uniqueKeyPrefixLength(nil, "name")
	require.NoError(t, err)
	require.Zero(t, length)
}

func TestTableUsesCollationKeyV2ValidatesMetadata(t *testing.T) {
	ctx := context.Background()
	useV2, err := tableUsesCollationKeyV2(ctx, &planpb.TableDef{UniqueKeyCodecVersion: v2PlannerMetadata()})
	require.NoError(t, err)
	require.True(t, useV2)

	bad := v2PlannerMetadata()
	bad.RegistryDigest[0]++
	_, err = tableUsesCollationKeyV2(ctx, &planpb.TableDef{UniqueKeyCodecVersion: bad})
	require.Error(t, err)
}

func TestV2CompositePrimaryKeyUsesFramedIdentityInPreInsert(t *testing.T) {
	hidden := &planpb.ColDef{
		Name:   catalog.CPrimaryKeyColName,
		Typ:    collationKeyV2StorageType(),
		Hidden: true,
	}
	table := &planpb.TableDef{
		Name: "t_v2_pk",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: v2PlannerTextType(uint32(types.CharsetUTF8))},
			hidden,
		},
		Pkey: &planpb.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: hidden,
		},
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}

	expr, err := makeCompPkeyExprForTable(context.Background(), table, map[string]int32{"id": 0})
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Equal(t, int32(types.T_blob), expr.Typ.Id)
	require.Equal(t, function.CollationKeyV2FunctionEncodedID, expr.GetF().GetFunc().GetObj())
	hiddenTypes, _ := getHiddenColumnForPreInsert(table)
	require.Len(t, hiddenTypes, 1)
	require.Equal(t, collationKeyV2StorageType(), hiddenTypes[0])
}

func TestV2CompositePrimaryKeyRejectsMissingSource(t *testing.T) {
	hidden := &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: collationKeyV2StorageType(), Hidden: true}
	table := &planpb.TableDef{
		Name: "t_v2_pk_missing",
		Cols: []*planpb.ColDef{hidden},
		Pkey: &planpb.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: hidden,
		},
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}
	_, err := makeCompPkeyExprForTable(context.Background(), table, map[string]int32{})
	require.Error(t, err)
}

func TestV2PrimaryKeyRejectsMissingHiddenIdentity(t *testing.T) {
	table := &planpb.TableDef{
		Name: "t_v2_pk_without_hidden",
		Cols: []*planpb.ColDef{{Name: "id", Typ: v2PlannerTextType(uint32(types.CharsetUTF8))}},
		Pkey: &planpb.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}
	_, err := makeCompPkeyExprForTable(context.Background(), table, map[string]int32{"id": 0})
	require.Error(t, err)
}

func TestV2CompositePrimaryKeyFramesEverySourcePart(t *testing.T) {
	hidden := &planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: collationKeyV2StorageType(), Hidden: true}
	table := &planpb.TableDef{
		Name: "t_v2_composite_pk",
		Cols: []*planpb.ColDef{
			{Name: "tenant", Typ: v2PlannerTextType(uint32(types.CharsetUTF8))},
			{Name: "name", Typ: v2PlannerTextType(uint32(types.CharsetUTF8MB4Bin))},
			hidden,
		},
		Pkey: &planpb.PrimaryKeyDef{
			Names:       []string{"tenant", "name"},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: hidden,
		},
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}

	expr, err := makeCompPkeyExprForTable(context.Background(), table, map[string]int32{"tenant": 0, "name": 1})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), expr.Typ.Id)
	require.Equal(t, function.CollationCompositeKeyV2FunctionEncodedID, expr.GetF().GetFunc().GetObj())
	require.Len(t, expr.GetF().GetArgs(), 6)
	require.Equal(t, int64(types.CharsetUTF8), expr.GetF().GetArgs()[2].GetLit().GetI64Val())
	require.Equal(t, int64(types.CharsetUTF8MB4Bin), expr.GetF().GetArgs()[5].GetLit().GetI64Val())
}

func TestCollationKeyV2ExpressionRejectsEveryMalformedBoundary(t *testing.T) {
	valid := &planpb.Expr{Typ: v2PlannerTextType(uint32(types.CharsetUTF8))}
	for _, value := range []*planpb.Expr{nil, {Typ: planpb.Type{Id: int32(types.T_char), Charset: uint32(types.CharsetUTF8)}}, {Typ: v2PlannerTextType(99)}} {
		_, err := makeCollationKeyV2Expr(value, 0)
		require.Error(t, err)
	}
	_, err := makeCollationKeyV2Expr(valid, -1)
	require.Error(t, err)
	_, err = makeCollationKeyV2Expr(valid, int(^uint32(0)))
	require.NoError(t, err)
	_, err = makeCollationKeyV2Expr(valid, int(^uint32(0))+1)
	require.Error(t, err)

	for _, part := range []string{"", catalog.CreateAlias("")} {
		_, err := uniqueKeyPrefixLength(nil, part)
		require.Error(t, err)
	}
	_, err = uniqueKeyPrefixLength(map[string]int{"name": int(^uint32(0)) + 1}, "name")
	require.Error(t, err)
	require.NoError(t, func() error {
		_, err := uniqueKeyPrefixLength(map[string]int{"name": 3}, catalog.CreateAlias("name"))
		return err
	}())

	table := &planpb.TableDef{Name: "t", UniqueKeyCodecVersion: v2PlannerMetadata()}
	index := &planpb.IndexDef{Parts: []string{"name"}, Unique: true}
	for _, values := range [][]*planpb.Expr{nil, {nil}, {valid}} {
		if len(values) == 1 && values[0] == valid {
			continue
		}
		require.Error(t, validateUniqueKeyInputExprs(table, index, values))
	}
	require.Error(t, validateUniqueKeyInputExprs(nil, index, []*planpb.Expr{valid}))
	require.Error(t, validateUniqueKeyInputExprs(table, nil, []*planpb.Expr{valid}))
	require.Error(t, validateUniqueKeyInputExprs(table, &planpb.IndexDef{}, nil))
	require.Error(t, validateUniqueKeyInputExprs(table, &planpb.IndexDef{Parts: []string{""}}, []*planpb.Expr{valid}))

	_, err = makeCollationCompositeKeyV2Expr(nil, nil)
	require.Error(t, err)
	_, err = makeCollationCompositeKeyV2Expr([]*planpb.Expr{valid, nil}, []int{0, 0})
	require.Error(t, err)
	_, err = makeCollationCompositeKeyV2Expr([]*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}, valid}, []int{0, 0})
	require.Error(t, err)
	_, err = makeCollationCompositeKeyV2Expr([]*planpb.Expr{valid, valid}, []int{-1, 0})
	require.Error(t, err)
}

func TestV2UniqueKeyExpressionBuildersCoverSingleAndCompositePaths(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	table := &planpb.TableDef{
		Name:                  "t_v2_expr",
		UniqueKeyCodecVersion: v2PlannerMetadata(),
	}
	value := &planpb.Expr{Typ: v2PlannerTextType(uint32(types.CharsetUTF8))}
	node := &planpb.Node{ProjectList: []*planpb.Expr{value, value}}
	single := &planpb.IndexDef{Parts: []string{"a"}, Unique: true}
	composite := &planpb.IndexDef{Parts: []string{"a", "b"}, Unique: true}
	for _, index := range []*planpb.IndexDef{single, composite} {
		expr, err := builder.makeInsertUniqueIndexKeyExpr(node, 3, table, index,
			map[string]int32{"t_v2_expr.a": 0, "t_v2_expr.b": 1}, map[string]int{"a": 2})
		require.NoError(t, err)
		require.NotNil(t, expr)
	}
	_, err := builder.makeInsertUniqueIndexKeyExpr(node, 3, table, single, map[string]int32{}, nil)
	require.Error(t, err)
	_, err = builder.makeInsertUniqueIndexKeyExpr(nil, 3, table, single, nil, nil)
	require.Error(t, err)

	for _, index := range []*planpb.IndexDef{single, composite} {
		values := []*planpb.Expr{value}
		if len(index.Parts) == 2 {
			values = append(values, value)
		}
		expr, err := builder.makeUniqueIndexKeyExprFromInputExprs(table, index, values, nil)
		require.NoError(t, err)
		require.NotNil(t, expr)
	}
	_, err = builder.makeUniqueIndexKeyExprFromInputExprs(table, single, []*planpb.Expr{value}, map[string]int{"a": 0})
	require.Error(t, err)

	pk := &planpb.TableDef{Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}}}
	_, err = makePrimaryKeyV2IdentityExpr(nil, value)
	require.Error(t, err)
	_, err = makePrimaryKeyV2IdentityExpr(pk, nil)
	require.Error(t, err)
	pk.Pkey.Names = []string{"a", "b"}
	_, err = makePrimaryKeyV2IdentityExpr(pk, value)
	require.Error(t, err)
	_, err = makePrimaryKeyV2IdentityExprs(pk, []*planpb.Expr{value})
	require.Error(t, err)
	pk.Pkey.Names = []string{"a"}
	expr, err := makePrimaryKeyV2IdentityExprs(pk, []*planpb.Expr{value})
	require.NoError(t, err)
	require.NotNil(t, expr)
}
