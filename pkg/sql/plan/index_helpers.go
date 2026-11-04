// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isSpatialIndexDef(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && catalog.IsRTreeIndexAlgo(idxDef.IndexAlgo)
}

func indexTableStoresSerializedKey(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && !isSpatialIndexDef(idxDef) && len(idxDef.Parts) > 1
}

// indexTableStoredKeySerialFunc identifies the serializer used to materialize
// an index-table key. Non-unique serialized keys retain NULL components so the
// hidden table can represent every base-table row.
func indexTableStoredKeySerialFunc(idxDef *planpb.IndexDef) string {
	if idxDef != nil && idxDef.KeyFormat == collation.KeyFormatV1 {
		return "physical_serial"
	}
	if idxDef != nil && !idxDef.Unique && indexTableStoresSerializedKey(idxDef) {
		return "serial_full"
	}
	return "serial"
}

// bindIndexKeyExpr builds the canonical value stored in (or compared with) a
// regular index table.  Versioned Unicode unique indexes store opaque physical
// collation keys; every DML producer and probe must use the same expression,
// even when the optimizer has pruned the source column's type metadata.
// Legacy indexes retain their historical raw/s serial representation.
func bindIndexKeyExpr(ctx context.Context, idxDef *planpb.IndexDef, parts []*planpb.Expr) (*planpb.Expr, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	if idxDef != nil && idxDef.KeyFormat == collation.KeyFormatV1 {
		if len(parts) == 1 {
			if !types.IsUnicodeCollation(uint8(parts[0].Typ.Charset)) || !types.T(parts[0].Typ.Id).IsMySQLString() {
				return parts[0], nil
			}
			return BindFuncExprImplByPlanExpr(ctx, "physical_collation_key", parts)
		}
		return BindFuncExprImplByPlanExpr(ctx, "physical_serial", parts)
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return BindFuncExprImplByPlanExpr(ctx, indexTableStoredKeySerialFunc(idxDef), parts)
}

// indexTableComparisonSerialFunc identifies the serializer for SQL comparison
// operands. serial and serial_full encode non-NULL components identically, but
// serial propagates NULL. That distinction lets the access predicate preserve
// SQL three-valued comparison semantics without a decoded row-by-row recheck.
func indexTableComparisonSerialFunc() string {
	return "serial"
}

func indexLookupColumnName(idxDef *planpb.IndexDef) string {
	if isSpatialIndexDef(idxDef) {
		return catalog.IndexTablePrimaryColName
	}
	return catalog.IndexTableIndexColName
}

func indexPrimaryPartName(idxDef *planpb.IndexDef) string {
	if idxDef == nil || len(idxDef.Parts) == 0 {
		return ""
	}
	return catalog.ResolveAlias(idxDef.Parts[0])
}

// makeIndexDedupKeyExprs preserves logical index parts for diagnostics without
// materializing a second key for every input row. Physical keys are opaque and
// cannot be decoded back to the source spelling.
func (builder *QueryBuilder) makeIndexDedupKeyExprs(
	tableDef *planpb.TableDef, idxDef *planpb.IndexDef, inputTag int32,
	columnPos func(string) (int32, bool),
) ([]*planpb.Expr, error) {
	if idxDef.KeyFormat != collation.KeyFormatV1 {
		return nil, nil
	}
	prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	parts := make([]*planpb.Expr, len(idxDef.Parts))
	for i, part := range idxDef.Parts {
		name := catalog.ResolveAlias(part)
		pos, ok := columnPos(name)
		if !ok {
			return nil, moerr.NewInternalErrorf(builder.GetContext(), "missing duplicate diagnostic column %s", name)
		}
		parts[i], err = builder.makeIndexPartExpr(inputTag, pos, name, tableDef.Cols[tableDef.Name2ColIndex[name]].Typ, prefixes)
		if err != nil {
			return nil, err
		}
	}
	return parts, nil
}
