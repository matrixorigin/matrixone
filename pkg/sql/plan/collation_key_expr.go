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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// tableUsesCollationKeyV2 reports whether a physical relation explicitly owns
// the framed v2 key format. Admission remains responsible for checking the
// cluster activation fence; this helper only validates the relation-local
// metadata before constructing an expression.
func tableUsesCollationKeyV2(ctx context.Context, tableDef *planpb.TableDef) (bool, error) {
	if tableDef == nil || tableDef.UniqueKeyCodecVersion == nil {
		return false, nil
	}
	version := tableDef.UniqueKeyCodecVersion
	metadata := collationkey.RelationMetadata{
		Version:              version.Value,
		RegistryVersion:      version.RegistryVersion,
		RegistryDigest:       version.RegistryDigest,
		MaxEncodedKeyBytes:   version.MaxEncodedKeyBytes,
		ActivationGeneration: version.ActivationGeneration,
	}
	if err := metadata.Validate(); err != nil {
		return false, moerr.NewInternalErrorf(ctx, "invalid unique-key codec metadata: %v", err)
	}
	return metadata.IsV2(), nil
}

func collationKeyV2ValueCharset(typ planpb.Type) (int64, error) {
	if typ.Id != int32(types.T_varchar) && typ.Id != int32(types.T_text) {
		return 0, moerr.NewNotSupportedNoCtxf("unique-key codec v2 does not support type %d", typ.Id)
	}
	switch typ.Charset {
	case uint32(types.CharsetUTF8), uint32(types.CharsetUTF8MB4Bin):
		return int64(typ.Charset), nil
	default:
		return 0, moerr.NewNotSupportedNoCtxf("unique-key codec v2 does not support charset %d", typ.Charset)
	}
}

func collationKeyV2OutputType(input planpb.Type) planpb.Type {
	return planpb.Type{
		Id:          int32(types.T_blob),
		Width:       types.MaxBlobLen,
		Charset:     uint32(types.CharsetBinary),
		NotNullable: input.NotNullable,
	}
}

func collationKeyV2StorageType() planpb.Type {
	return planpb.Type{
		Id:      int32(types.T_blob),
		Width:   types.MaxBlobLen,
		Charset: uint32(types.CharsetBinary),
	}
}

func makeCollationKeyV2Expr(value *planpb.Expr, prefix int) (*planpb.Expr, error) {
	if value == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil value in collation key v2 expression")
	}
	charset, err := collationKeyV2ValueCharset(value.Typ)
	if err != nil {
		return nil, err
	}
	if prefix < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative unique-key prefix")
	}
	return &planpb.Expr{
		Typ: collationKeyV2OutputType(value.Typ),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     function.CollationKeyV2FunctionEncodedID,
				ObjName: "__mo_collation_key_v2",
			},
			Args: []*planpb.Expr{
				DeepCopyExpr(value),
				makePlan2Int64ConstExprWithType(int64(prefix)),
				makePlan2Int64ConstExprWithType(charset),
			},
		}},
	}, nil
}

func makeCollationCompositeKeyV2Expr(values []*planpb.Expr, prefixes []int) (*planpb.Expr, error) {
	if len(values) < 2 || len(values) != len(prefixes) {
		return nil, moerr.NewInternalErrorNoCtx("invalid collation composite key v2 parts")
	}
	args := make([]*planpb.Expr, 0, len(values)*3)
	for i, value := range values {
		if value == nil {
			return nil, moerr.NewInternalErrorNoCtx("nil value in collation composite key v2 expression")
		}
		charset, err := collationKeyV2ValueCharset(value.Typ)
		if err != nil {
			return nil, err
		}
		if prefixes[i] < 0 {
			return nil, moerr.NewInvalidInputNoCtx("negative unique-key prefix")
		}
		args = append(args,
			DeepCopyExpr(value),
			makePlan2Int64ConstExprWithType(int64(prefixes[i])),
			makePlan2Int64ConstExprWithType(charset),
		)
	}
	return &planpb.Expr{
		Typ: collationKeyV2OutputType(values[0].Typ),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     function.CollationCompositeKeyV2FunctionEncodedID,
				ObjName: "__mo_collation_composite_key_v2",
			},
			Args: args,
		}},
	}, nil
}

func (builder *QueryBuilder) makeInsertUniqueIndexKeyExpr(
	selectNode *planpb.Node,
	selectTag int32,
	tableDef *planpb.TableDef,
	idxDef *planpb.IndexDef,
	colName2Idx map[string]int32,
	prefixLengths map[string]int,
) (*planpb.Expr, error) {
	useV2, err := tableUsesCollationKeyV2(builder.GetContext(), tableDef)
	if err != nil || !useV2 {
		if len(idxDef.Parts) == 1 {
			return builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, idxDef.Parts[0], prefixLengths)
		}
		args := make([]*planpb.Expr, len(idxDef.Parts))
		for i, part := range idxDef.Parts {
			args[i], err = builder.makeInsertIndexPartExpr(selectNode, selectTag, tableDef, colName2Idx, part, prefixLengths)
			if err != nil {
				return nil, err
			}
		}
		return BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
	}

	values := make([]*planpb.Expr, len(idxDef.Parts))
	prefixes := make([]int, len(idxDef.Parts))
	for i, part := range idxDef.Parts {
		partName := catalog.ResolveAlias(part)
		pos, ok := colName2Idx[tableDef.Name+"."+partName]
		if !ok || pos < 0 || int(pos) >= len(selectNode.ProjectList) {
			return nil, moerr.NewInternalErrorf(builder.GetContext(), "cannot locate v2 unique-key part %s", partName)
		}
		values[i] = &planpb.Expr{
			Typ: selectNode.ProjectList[pos].Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: selectTag,
				ColPos: pos,
				Name:   partName,
			}},
		}
		prefixes[i] = prefixLengths[partName]
	}
	if len(values) == 1 {
		return makeCollationKeyV2Expr(values[0], prefixes[0])
	}
	return makeCollationCompositeKeyV2Expr(values, prefixes)
}

func (builder *QueryBuilder) makeUniqueIndexKeyExprFromInputExprs(
	tableDef *planpb.TableDef,
	idxDef *planpb.IndexDef,
	values []*planpb.Expr,
	prefixLengths map[string]int,
) (*planpb.Expr, error) {
	useV2, err := tableUsesCollationKeyV2(builder.GetContext(), tableDef)
	if err != nil || !useV2 {
		if len(values) == 1 {
			return builder.makeIndexPartExprFromInputExpr(values[0], catalog.ResolveAlias(idxDef.Parts[0]), prefixLengths)
		}
		parts := make([]*planpb.Expr, len(values))
		for i, value := range values {
			parts[i], err = builder.makeIndexPartExprFromInputExpr(value, catalog.ResolveAlias(idxDef.Parts[i]), prefixLengths)
			if err != nil {
				return nil, err
			}
		}
		return BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", parts)
	}
	prefixes := make([]int, len(values))
	for i := range values {
		prefixes[i] = prefixLengths[catalog.ResolveAlias(idxDef.Parts[i])]
	}
	if len(values) == 1 {
		return makeCollationKeyV2Expr(values[0], prefixes[0])
	}
	return makeCollationCompositeKeyV2Expr(values, prefixes)
}

func makePrimaryKeyV2IdentityExpr(tableDef *planpb.TableDef, value *planpb.Expr) (*planpb.Expr, error) {
	if tableDef == nil || tableDef.Pkey == nil || value == nil {
		return nil, moerr.NewInternalErrorNoCtx("invalid v2 primary-key identity")
	}
	if len(tableDef.Pkey.Names) > 1 {
		return nil, moerr.NewNotSupportedNoCtx("composite v2 primary-key identity requires part expressions")
	}
	return makeCollationKeyV2Expr(value, 0)
}

func makePrimaryKeyV2IdentityExprs(tableDef *planpb.TableDef, values []*planpb.Expr) (*planpb.Expr, error) {
	if tableDef == nil || tableDef.Pkey == nil || len(values) != len(tableDef.Pkey.Names) || len(values) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid v2 primary-key identity parts")
	}
	if len(values) == 1 {
		return makeCollationKeyV2Expr(values[0], 0)
	}
	prefixes := make([]int, len(values))
	return makeCollationCompositeKeyV2Expr(values, prefixes)
}
