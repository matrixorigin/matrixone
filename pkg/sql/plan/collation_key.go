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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// MakeCollationKeyExpr constructs the physical representation of one already
// resolved comparison operand. Both stored-value producers and query probes
// must pass the same format and effective type. This function does not resolve
// SQL coercibility, extract an index prefix, or admit creation of a new format.
// In particular it must not be called solely because a column has Charset set.
func MakeCollationKeyExpr(ctx context.Context, input *pb.Expr, effectiveType pb.Type, format types.KeyFormat) (*pb.Expr, error) {
	if input == nil {
		return nil, moerr.NewInvalidInput(ctx, "nil collation key operand")
	}
	if err := types.ValidateKeyFormat(uint32(format)); err != nil {
		return nil, moerr.NewInvalidInput(ctx, err.Error())
	}
	if err := types.ValidateCollationTypeMetadata(
		types.T(effectiveType.Id), effectiveType.Charset, effectiveType.CollationVersion,
	); err != nil {
		return nil, moerr.NewInvalidInput(ctx, err.Error())
	}
	resolvedType := types.NewWithCharsetVersion(
		types.T(effectiveType.Id), effectiveType.Width, effectiveType.Scale,
		uint8(effectiveType.Charset), uint8(effectiveType.CollationVersion),
	)
	part, err := types.ResolveStringKeyPart(resolvedType, format)
	if err != nil {
		return nil, err
	}
	if !types.NeedsCollationKey(resolvedType, format) || !part.Transformed() {
		return input, nil
	}
	if f := input.GetF(); f != nil {
		fid, _ := function.DecodeOverloadID(f.Func.Obj)
		if fid == function.INTERNAL_COLLATION_KEY {
			if len(f.Args) == 2 && f.Args[1].GetLit() != nil && f.Args[1].GetLit().GetU64Val() == uint64(effectiveType.Charset) {
				return input, nil
			}
			return nil, moerr.NewInvalidInput(ctx, "cannot reinterpret an existing collation key")
		}
	}
	// A materialized/folded key no longer necessarily has its function node.
	// Binary metadata alone cannot establish that it is original text; decline
	// this rewrite instead of transforming an opaque column or literal twice.
	// A direct prepared marker is converted at runtime in the resolved domain.
	if input.Typ.Charset == uint32(types.CharsetBinary) && input.GetP() == nil {
		return nil, moerr.NewInvalidInput(ctx, "collation key operand requires original text provenance")
	}
	return BindFuncExprImplByPlanExpr(ctx, "internal_collation_key", []*pb.Expr{input, makePlan2Uint64ConstExprWithType(uint64(effectiveType.Charset))})
}

func makeNativeCollationKeyExpr(ctx context.Context, input *pb.Expr, effectiveType pb.Type) (*pb.Expr, error) {
	if input == nil || !types.T(effectiveType.Id).IsMySQLString() ||
		!types.NeedsCollationKey(types.NewWithCharsetVersion(
			types.T(effectiveType.Id), effectiveType.Width, effectiveType.Scale,
			uint8(effectiveType.Charset), uint8(effectiveType.CollationVersion),
		), types.PADSpaceKeyV1) {
		return input, nil
	}
	return MakeCollationKeyExpr(ctx, input, effectiveType, types.PADSpaceKeyV1)
}
