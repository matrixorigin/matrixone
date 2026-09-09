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
