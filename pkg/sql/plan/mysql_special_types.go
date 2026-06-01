// Copyright 2021 Matrix Origin
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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func isEnumPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_enum) && len(typ.GetEnumvalues()) > 0
}

// isSetPlanType identifies a SET column. SET is stored as T_uint64 with a non-empty
// Enumvalues field holding the comma-separated member list. This is the sole
// discriminator between a plain uint64 column and a SET column — no other code
// path should populate Enumvalues on a T_uint64 type.
func isSetPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_uint64) && len(typ.GetEnumvalues()) > 0
}

func isEnumOrSetPlanType(typ *plan.Type) bool {
	return isEnumPlanType(typ) || isSetPlanType(typ)
}

func isGeometryPlanType(typ *plan.Type) bool {
	return typ != nil && (typ.Id == int32(types.T_geometry) || typ.Id == int32(types.T_geometry32))
}

// Geometry subtype and SRID are stored in the column type's Scale and Width
// (per docs/design/gisimpl.md §1.4), not in Enumvalues:
//
//   - Scale holds the geo.Subtype enum (0 GENERIC .. 7 GEOMETRYCOLLECTION).
//   - Width holds srid+1 when the column declares an SRID, or 0 when it does
//     not. The +1 offset preserves the "SRID defined vs unspecified"
//     distinction while still keeping the SRID in Width. Only SRID 0 and 4326
//     are meaningful for computation, so the int32 range of Width is ample.

func geometrySubtypeName(typ *plan.Type) string {
	if !isGeometryPlanType(typ) {
		return ""
	}
	return geometrySubtypeNameFromEnum(geo.Subtype(typ.Scale))
}

func geometrySRIDValue(typ *plan.Type) (uint32, bool) {
	if !isGeometryPlanType(typ) {
		return 0, false
	}
	return decodeGeometrySRIDWidth(typ.Width)
}

// geometrySubtypeEnum maps a subtype name (as it appears in DDL) to the
// geo.Subtype enum stored in Scale. "" and "GEOMETRY" map to GENERIC.
func geometrySubtypeEnum(name string) geo.Subtype {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "POINT":
		return geo.POINT
	case "LINESTRING":
		return geo.LINESTRING
	case "POLYGON":
		return geo.POLYGON
	case "MULTIPOINT":
		return geo.MULTIPOINT
	case "MULTILINESTRING":
		return geo.MULTILINESTRING
	case "MULTIPOLYGON":
		return geo.MULTIPOLYGON
	case "GEOMETRYCOLLECTION":
		return geo.GEOMETRYCOLLECTION
	default:
		return geo.GENERIC
	}
}

// geometrySubtypeNameFromEnum is the inverse of geometrySubtypeEnum; GENERIC
// maps to "" (no subtype constraint), matching the convention callers expect.
func geometrySubtypeNameFromEnum(s geo.Subtype) string {
	if s == geo.GENERIC {
		return ""
	}
	return s.String()
}

func encodeGeometrySRIDWidth(srid uint32, defined bool) int32 {
	if !defined {
		return 0
	}
	return int32(srid + 1)
}

func decodeGeometrySRIDWidth(width int32) (uint32, bool) {
	if width <= 0 {
		return 0, false
	}
	return uint32(width - 1), true
}

func geometryMetadataString(subtype string, srid uint32, sridDefined bool) string {
	subtype = normalizeGeometrySubtype(subtype)
	if !sridDefined {
		return subtype
	}
	if subtype == "" {
		return fmt.Sprintf("SRID=%d", srid)
	}
	return fmt.Sprintf("%s;SRID=%d", subtype, srid)
}

func normalizeGeometrySubtype(subtype string) string {
	subtype = strings.ToUpper(strings.TrimSpace(subtype))
	switch subtype {
	case "", "GEOMETRY":
		return ""
	default:
		return subtype
	}
}

func geometrySubtypeCompatible(columnSubtype, valueSubtype string) bool {
	columnSubtype = strings.ToUpper(columnSubtype)
	valueSubtype = strings.ToUpper(valueSubtype)
	if columnSubtype == "" || columnSubtype == "GEOMETRY" {
		return true
	}
	if valueSubtype == "GEOMETRY" {
		return true
	}
	if valueSubtype == "" {
		return false
	}
	return columnSubtype == valueSubtype
}

func formatGeometrySRIDForError(srid uint32, defined bool) string {
	if !defined {
		return "UNSPECIFIED"
	}
	return strconv.FormatUint(uint64(srid), 10)
}

func mysqlSpecialTypeFuncNames(typ *plan.Type) (string, string, string, error) {
	switch {
	case isEnumPlanType(typ):
		return moEnumCastIndexToValueFun, moEnumCastValueToIndexFun, moEnumCastIndexValueToIndexFun, nil
	case isSetPlanType(typ):
		return moSetCastIndexToValueFun, moSetCastValueToIndexFun, moSetCastIndexValueToIndexFun, nil
	default:
		return "", "", "", moerr.NewInternalErrorNoCtx("not enum/set type")
	}
}

func wrapAstExprForMySQLSpecialType(ctx context.Context, targetType plan.Type, astExpr tree.Expr) (tree.Expr, error) {
	if !isEnumOrSetPlanType(&targetType) {
		return astExpr, nil
	}

	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	boundExpr, err := binder.BindExpr(astExpr, 0, false)
	if err != nil {
		return nil, err
	}

	_, valueToIndex, indexValueToIndex, err := mysqlSpecialTypeFuncNames(&targetType)
	if err != nil {
		return nil, err
	}

	funcName := valueToIndex
	if types.T(boundExpr.Typ.Id).IsInteger() {
		funcName = indexValueToIndex
	}

	return &tree.FuncExpr{
		Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(funcName)),
		Type: tree.FUNC_TYPE_DEFAULT,
		Exprs: []tree.Expr{
			tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char),
			astExpr,
		},
	}, nil
}

func funcCastForGeometryType(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	if !isGeometryPlanType(&targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	if types.T(expr.Typ.Id) == types.T_any || isGeometryNullLiteralExpr(expr) {
		expr.Typ = targetType
		return expr, nil
	}
	// The runtime cast_geometry_to_subtype function still validates against a
	// metadata string ("POINT;SRID=4326"); derive it from the column's
	// Scale/Width at this bind boundary.
	srid, sridDefined := geometrySRIDValue(&targetType)
	targetMetadata := geometryMetadataString(geometrySubtypeName(&targetType), srid, sridDefined)
	if isGeometryPlanType(&expr.Typ) && expr.Typ.Scale == targetType.Scale && expr.Typ.Width == targetType.Width {
		expr.Typ = targetType
		return expr, nil
	}

	args := make([]*Expr, 2)
	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	targetSubtypeExpr, err := binder.BindExpr(tree.NewNumVal(targetMetadata, targetMetadata, false, tree.P_char), 0, false)
	if err != nil {
		return nil, err
	}
	args[0] = targetSubtypeExpr
	args[1] = expr

	castedExpr, err := BindFuncExprImplByPlanExpr(ctx, moGeometryCastToSubtypeFun, args)
	if err != nil {
		return nil, err
	}
	castedExpr.Typ = targetType
	return castedExpr, nil
}

func isGeometryNullLiteralExpr(expr *Expr) bool {
	if expr == nil {
		return false
	}
	lit, ok := expr.Expr.(*plan.Expr_Lit)
	return ok && lit.Lit != nil && lit.Lit.Isnull
}
