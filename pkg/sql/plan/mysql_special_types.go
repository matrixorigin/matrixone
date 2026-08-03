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
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// validateGeometrySRID rejects SRIDs that cannot be stored in the type Width
// (see geo.MaxSRID).
func validateGeometrySRID(srid int64) error {
	if srid < 0 || srid > int64(geo.MaxSRID) {
		return moerr.NewInvalidInputNoCtxf("SRID should be between 0 and %d", geo.MaxSRID)
	}
	return nil
}

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

func isTypedArrayPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_json) && arrayPlanTypeString(typ) != ""
}

func arrayPlanTypeString(typ *plan.Type) string {
	if typ == nil {
		return ""
	}
	metadata := strings.TrimSpace(typ.GetEnumvalues())
	if strings.HasPrefix(strings.ToLower(metadata), "array(") {
		return metadata
	}
	return ""
}

func validateTypedArrayElementType(ctx context.Context, elem *tree.T) error {
	if elem == nil {
		return moerr.NewInternalError(ctx, "array type missing element type")
	}
	if elem.InternalType.Oid == uint32(defines.MYSQL_TYPE_TYPED_ARRAY) {
		if elem.InternalType.ArrayContents == nil {
			return moerr.NewInternalError(ctx, "array type missing element type")
		}
		return validateTypedArrayElementType(ctx, elem.InternalType.ArrayContents)
	}

	if isSupportedTypedArrayElementType(elem) {
		return nil
	}
	return moerr.NewInvalidInputf(ctx, "unsupported ARRAY element type %s", tree.String(&elem.InternalType, dialect.MYSQL))
}

func isSupportedTypedArrayElementType(elem *tree.T) bool {
	if elem == nil {
		return false
	}
	switch defines.MysqlType(elem.InternalType.Oid) {
	case defines.MYSQL_TYPE_BOOL,
		defines.MYSQL_TYPE_TINY,
		defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_INT24,
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_FLOAT,
		defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_DECIMAL,
		defines.MYSQL_TYPE_NEWDECIMAL,
		defines.MYSQL_TYPE_JSON,
		defines.MYSQL_TYPE_DATE,
		defines.MYSQL_TYPE_TIME,
		defines.MYSQL_TYPE_DATETIME,
		defines.MYSQL_TYPE_TIMESTAMP,
		defines.MYSQL_TYPE_YEAR,
		defines.MYSQL_TYPE_UUID:
		return true
	case defines.MYSQL_TYPE_STRING, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
		switch strings.ToLower(strings.TrimSpace(elem.InternalType.FamilyString)) {
		case "char", "varchar", "binary", "varbinary":
			return true
		default:
			return false
		}
	case defines.MYSQL_TYPE_BLOB:
		return strings.EqualFold(elem.InternalType.FamilyString, "blob")
	case defines.MYSQL_TYPE_TEXT:
		return strings.EqualFold(elem.InternalType.FamilyString, "text")
	default:
		return false
	}
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

// mysqlSpecialOrderTypeForExpr returns the storage type whose definition order
// belongs to a visible string expression. Provenance is deliberately narrow:
// an exact ENUM/SET display call originates it, and an exact ColRef may carry it
// through a query boundary. Any cast or other string expression clears it.
func (bc *BindContext) mysqlSpecialOrderTypeForExpr(expr *plan.Expr) *plan.Type {
	if expr == nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return nil
	}

	if isEnumOrSetDisplayValueExpr(expr) {
		fn := expr.GetF()
		if len(fn.Args) == 2 && isEnumOrSetPlanType(&fn.Args[1].Typ) {
			return DeepCopyType(&fn.Args[1].Typ)
		}
		return nil
	}

	col := expr.GetCol()
	if col == nil {
		return nil
	}
	if col.RelPos == bc.projectTag {
		if typ, recorded := bc.mysqlSpecialOrderTypes[col.ColPos]; recorded {
			return DeepCopyType(typ)
		}
	}
	if bc.groupTag > 0 && col.RelPos == bc.groupTag && col.ColPos >= 0 && int(col.ColPos) < len(bc.groups) {
		groupExpr := bc.groups[col.ColPos]
		if groupExpr == nil {
			return nil
		}
		if groupCol := groupExpr.GetCol(); groupCol != nil && groupCol.RelPos == bc.groupTag {
			return nil
		}
		return bc.mysqlSpecialOrderTypeForExpr(groupExpr)
	}
	binding := bc.bindingByTag[col.RelPos]
	if binding == nil || col.ColPos < 0 || int(col.ColPos) >= len(binding.mysqlSpecialOrderTypes) {
		return nil
	}
	return DeepCopyType(binding.mysqlSpecialOrderTypes[col.ColPos])
}

func (bc *BindContext) setMySQLSpecialOrderType(colPos int32, typ *plan.Type) {
	if bc.mysqlSpecialOrderTypes == nil {
		bc.mysqlSpecialOrderTypes = make(map[int32]*plan.Type)
	}
	bc.mysqlSpecialOrderTypes[colPos] = typ
}

func (bc *BindContext) mysqlSpecialOrderTypeForProject(colPos int32) *plan.Type {
	if typ, recorded := bc.mysqlSpecialOrderTypes[colPos]; recorded {
		return DeepCopyType(typ)
	}
	if colPos < 0 || int(colPos) >= len(bc.projects) {
		return nil
	}
	return bc.mysqlSpecialOrderTypeForExpr(bc.projects[colPos])
}

func mysqlSpecialOrderTypesCompatible(left, right *plan.Type) bool {
	return left != nil && right != nil && left.Id == right.Id && left.Enumvalues == right.Enumvalues
}

// enumFoldKey produces the same equivalence classes used by strings.EqualFold
// without an O(n^2) comparison across the maximum 65,535 ENUM members.
func enumFoldKey(value string) string {
	var folded strings.Builder
	for _, r := range value {
		min := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			if next < min {
				min = next
			}
		}
		folded.WriteRune(min)
	}
	return folded.String()
}

func mysqlSpecialOrderTypeReversible(typ *plan.Type) bool {
	switch {
	case isSetPlanType(typ):
		values, err := types.NormalizeSetValues(strings.Split(typ.Enumvalues, ","))
		if err != nil {
			return false
		}
		for _, value := range values {
			if value == "" {
				return false
			}
		}
		return true
	case isEnumPlanType(typ):
		seen := make(map[string]struct{})
		for _, value := range strings.Split(typ.Enumvalues, ",") {
			key := enumFoldKey(value)
			if _, exists := seen[key]; exists {
				return false
			}
			seen[key] = struct{}{}
		}
		return true
	default:
		return false
	}
}

func newNonReversibleMySQLSpecialOrderError(ctx context.Context) error {
	return moerr.NewNotSupported(ctx,
		"definition-order sorting of projected ENUM/SET values with non-unique display labels or ambiguous SET display values")
}

// makeMySQLSpecialOrderKey restores definition-order comparison after a query
// boundary. The display-to-index conversion is allowed only when it is a true
// inverse; ambiguous ENUM/SET definitions must never silently collapse storage
// values that have the same display value.
func makeMySQLSpecialOrderKey(ctx context.Context, displayExpr *plan.Expr, storageType *plan.Type) (*plan.Expr, error) {
	if !mysqlSpecialOrderTypeReversible(storageType) {
		return nil, newNonReversibleMySQLSpecialOrderError(ctx)
	}
	_, valueToIndex, _, err := mysqlSpecialTypeFuncNames(storageType)
	if err != nil {
		return nil, err
	}
	orderKey, err := BindFuncExprImplByPlanExpr(ctx, valueToIndex, []*plan.Expr{
		makePlan2StringConstExprWithType(storageType.Enumvalues),
		DeepCopyExpr(displayExpr),
	})
	if err != nil {
		return nil, err
	}
	orderKey.Typ.NotNullable = displayExpr.Typ.NotNullable
	orderKey.Typ.Enumvalues = storageType.Enumvalues
	return orderKey, nil
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
	if types.T(expr.Typ.Id) == types.T_any || isNullLiteralExpr(expr) {
		expr.Typ = targetType
		return expr, nil
	}

	// SRID is enforced here at bind time, from the types (the WKB payload does
	// not carry an SRID). A SRID-constrained column requires the value to carry
	// the same SRID; an unconstrained column (Width 0) accepts any SRID.
	if columnSRID, columnDefined := geometrySRIDValue(&targetType); columnDefined {
		valueSRID, _ := geometrySRIDValue(&expr.Typ)
		if valueSRID != columnSRID {
			return nil, moerr.NewInvalidInputf(ctx,
				"The SRID of the geometry does not match the SRID of the column. The SRID of the geometry is %d, but the SRID of the column is %d.",
				valueSRID, columnSRID)
		}
	}

	if isGeometryPlanType(&expr.Typ) && expr.Typ.Id == targetType.Id &&
		expr.Typ.Scale == targetType.Scale && expr.Typ.Width == targetType.Width {
		expr.Typ = targetType
		return expr, nil
	}

	// The runtime cast_geometry_to_subtype validates the value's subtype and
	// normalizes the stored bytes to WKB. It needs the column subtype name and,
	// for a GEOMETRY32 column, a "32:" prefix so the bytes are written as
	// float32-coordinate WKB.
	targetMetadata := geometrySubtypeName(&targetType)
	if types.T(targetType.Id) == types.T_geometry32 {
		targetMetadata = "32:" + targetMetadata
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

func funcCastForTypedArrayType(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	if !isTypedArrayPlanType(&targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	if types.T(expr.Typ.Id) == types.T_any || isNullLiteralExpr(expr) {
		expr.Typ = targetType
		return expr, nil
	}
	if isTypedArrayPlanType(&expr.Typ) && expr.Typ.GetEnumvalues() == targetType.GetEnumvalues() {
		expr.Typ = targetType
		return expr, nil
	}

	jsonType := plan.Type{Id: int32(types.T_json), NotNullable: expr.Typ.NotNullable}
	jsonExpr, err := forceCastExpr(ctx, expr, jsonType)
	if err != nil {
		return nil, err
	}

	args := make([]*Expr, 2)
	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	targetArrayTypeExpr, err := binder.BindExpr(tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char), 0, false)
	if err != nil {
		return nil, err
	}
	args[0] = targetArrayTypeExpr
	args[1] = jsonExpr

	castedExpr, err := BindFuncExprImplByPlanExpr(ctx, moJsonCastToArrayFun, args)
	if err != nil {
		return nil, err
	}
	castedExpr.Typ = targetType
	return castedExpr, nil
}

func isNullLiteralExpr(expr *Expr) bool {
	if expr == nil {
		return false
	}
	lit, ok := expr.Expr.(*plan.Expr_Lit)
	return ok && lit.Lit != nil && lit.Lit.Isnull
}
