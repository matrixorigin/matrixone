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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// validateGeometrySRID rejects SRIDs that cannot be stored in the type Width
// (see geo.MaxSRID).
func validateGeometrySRID(srid int64) error {
	if srid < 0 || srid > int64(geo.MaxSRID) {
		return moerr.NewInvalidInputNoCtxf("SRID should be between 0 and %d", geo.MaxSRID)
	}
	return nil
}

// geometrySRIDLiteralValue extracts the integer literal forms emitted by the
// parser and constant folder. Keep the conversion signed until validation so
// an oversized uint64 cannot wrap into an apparently valid SRID.
func geometrySRIDLiteralValue(lit *plan.Literal) (int64, bool, bool) {
	if lit == nil {
		return 0, false, false
	}
	if lit.Isnull {
		return 0, true, true
	}
	switch value := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return int64(value.I8Val), false, true
	case *plan.Literal_I16Val:
		return int64(value.I16Val), false, true
	case *plan.Literal_I32Val:
		return int64(value.I32Val), false, true
	case *plan.Literal_I64Val:
		return value.I64Val, false, true
	case *plan.Literal_U8Val:
		return int64(value.U8Val), false, true
	case *plan.Literal_U16Val:
		return int64(value.U16Val), false, true
	case *plan.Literal_U32Val:
		return int64(value.U32Val), false, true
	case *plan.Literal_U64Val:
		return int64(value.U64Val), false, true
	default:
		return 0, false, false
	}
}

// geometrySRIDRuntimeValue validates a prepared SRID before it is narrowed to
// the type Width. Runtime protocol values are commonly represented by native
// integers, while SQL EXECUTE values may arrive as strings or byte slices.
// Fractional, boolean, and arbitrary textual values are rejected rather than
// silently accepting a lossy cast.
func geometrySRIDRuntimeValue(value any) (uint32, bool, error) {
	if value == nil {
		return 0, true, nil
	}
	var srid uint64
	switch value := value.(type) {
	case int8:
		if value < 0 {
			return 0, false, validateGeometrySRID(-1)
		}
		srid = uint64(value)
	case int16:
		if value < 0 {
			return 0, false, validateGeometrySRID(-1)
		}
		srid = uint64(value)
	case int32:
		if value < 0 {
			return 0, false, validateGeometrySRID(-1)
		}
		srid = uint64(value)
	case int64:
		if value < 0 {
			return 0, false, validateGeometrySRID(value)
		}
		srid = uint64(value)
	case uint8:
		srid = uint64(value)
	case uint16:
		srid = uint64(value)
	case uint32:
		srid = uint64(value)
	case uint64:
		srid = value
	case string:
		return parseGeometrySRIDText(value)
	case []byte:
		return parseGeometrySRIDText(string(value))
	default:
		return 0, false, moerr.NewInvalidInputNoCtx("SRID should be an integer")
	}
	if srid > uint64(geo.MaxSRID) {
		return 0, false, moerr.NewInvalidInputNoCtxf("SRID should be between 0 and %d", geo.MaxSRID)
	}
	return uint32(srid), false, nil
}

func parseGeometrySRIDText(value string) (uint32, bool, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return 0, false, moerr.NewInvalidInputNoCtx("SRID should be an integer")
	}
	if strings.HasPrefix(text, "-") {
		srid, err := strconv.ParseInt(text, 10, 64)
		if err == nil && srid < 0 {
			return 0, false, validateGeometrySRID(srid)
		}
		return 0, false, moerr.NewInvalidInputNoCtx("SRID should be an integer")
	}
	srid, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, false, moerr.NewInvalidInputNoCtx("SRID should be an integer")
	}
	if srid > uint64(geo.MaxSRID) {
		return 0, false, moerr.NewInvalidInputNoCtxf("SRID should be between 0 and %d", geo.MaxSRID)
	}
	return uint32(srid), false, nil
}

func isDirectPreparedGeometrySRIDArg(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	return isImplicitPreparedParamCast(expr)
}

func isPreparedGeometrySRIDFunction(name string) bool {
	switch strings.ToLower(name) {
	case "st_srid", "st_geomfromwkb", "st_geomfrombinary", "st_geometryfromwkb":
		return true
	default:
		return false
	}
}

// isGeometrySRIDProducingFunction covers every geometry constructor whose
// explicit SRID is encoded in the result type.  The prepared marker support is
// intentionally narrower (see isPreparedGeometrySRIDFunction), but a static
// SRID constructor still has value-dependent metadata when its geometry source
// is a typed runtime NULL.
func isGeometrySRIDProducingFunction(name string) bool {
	switch strings.ToLower(name) {
	case "st_srid", "st_geomfromtext", "st_geomfromwkb", "st_geomfrombinary",
		"st_geometryfromtext", "st_geometryfromwkb", "st_pointfromtext",
		"st_linefromtext", "st_polygonfromtext", "st_mpointfromtext",
		"st_mlinefromtext", "st_mpolyfromtext", "st_geomcollfromtext",
		"st_pointfromgeohash", "st_geomfromgeojson":
		return true
	default:
		return false
	}
}

// geometryExprHasDeferredSRID reports whether a geometry expression contains
// a direct prepared SRID marker.  A Width of zero is also the representation
// of an ordinary, unconstrained geometry, so Width alone cannot tell the DML
// binder whether a mismatch check must be deferred until EXECUTE.
func geometryExprHasDeferredSRID(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && isPreparedGeometrySRIDFunction(fn.Func.GetObjName()) && len(fn.Args) >= 2 &&
			len(preparedGeometrySRIDParamPositionsInExpr(fn.Args[len(fn.Args)-1])) > 0 {
			return true
		}
		for _, arg := range fn.Args {
			if geometryExprHasDeferredSRID(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if geometryExprHasDeferredSRID(item) {
				return true
			}
		}
	}
	if sub := expr.GetSub(); sub != nil && sub.Child != nil {
		return geometryExprHasDeferredSRID(sub.Child)
	}
	return false
}

// geometrySRIDSourceIsStaticNull is deliberately narrower than a general
// constant-folding predicate. It recognizes NULL at the geometry input of an
// SRID-producing expression, including a cast around NULL, so an invalid SRID
// cannot mask the SQL NULL result. Row-varying NULLs are left to the runtime
// evaluator and do not bypass scalar SRID validation.
func geometrySRIDSourceIsStaticNull(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if isNullLiteralExpr(expr) {
		return true
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		name := strings.ToLower(fn.Func.GetObjName())
		if (name == "cast" || name == "cast_assign" || name == "cast_strict") && len(fn.Args) > 0 {
			// Generic CAST stores the source first and the TargetType second.
			return geometrySRIDSourceIsStaticNull(fn.Args[0])
		}
		if isGeometrySRIDProducingFunction(name) && len(fn.Args) >= 2 {
			// These functions are strict NULL propagators: a NULL geometry or a
			// NULL SRID produces a NULL geometry result. This matters at a DML
			// assignment boundary, where the result Width is otherwise the same
			// encoding as an unconstrained geometry and could be mistaken for a
			// mismatched SRID.
			return geometrySRIDSourceIsStaticNull(fn.Args[0]) ||
				geometrySRIDSourceIsStaticNull(fn.Args[len(fn.Args)-1])
		}
	}
	return false
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

// makeInsertIgnoreMySQLSpecialTypeConstExpr implements MySQL's INSERT IGNORE
// coercion for literal YEAR, ENUM, and SET values. BIT coercion stays in the
// shared literal parser. Regular INSERT keeps the existing strict conversion
// path; only IGNORE reaches this helper.
func makeInsertIgnoreMySQLSpecialTypeConstExpr(
	ctx context.Context,
	value *tree.NumVal,
	targetType plan.Type,
) (*plan.Expr, bool, error) {
	if value == nil || value.ValType == tree.P_null || value.ValType == tree.P_nulltext {
		return nil, false, nil
	}

	if isEnumPlanType(&targetType) {
		index, err := mysqlEnumLiteralIndex(targetType.Enumvalues, value)
		if err != nil {
			index = 0 // invalid ENUM values are stored as the empty-error member
		}
		return &plan.Expr{
			Typ: targetType,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_EnumVal{EnumVal: uint32(index)},
			}},
		}, true, nil
	}

	if isSetPlanType(&targetType) {
		bits := mysqlSetIgnoreLiteralBits(targetType.Enumvalues, value)
		return &plan.Expr{
			Typ: targetType,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: bits},
			}},
		}, true, nil
	}

	if types.T(targetType.Id) == types.T_year && !mysqlYearLiteralIsValid(value) {
		zero := makePlan2Int64ConstExprWithType(0)
		expr, err := appendCastBeforeExpr(ctx, zero, targetType)
		return expr, true, err
	}

	return nil, false, nil
}

func mysqlEnumLiteralIndex(enumValues string, value *tree.NumVal) (types.Enum, error) {
	switch value.ValType {
	case tree.P_int64:
		v, ok := value.Int64()
		if !ok || v < 0 || v > int64(^uint16(0)) {
			return 0, moerr.NewInvalidInputNoCtx("invalid ENUM index")
		}
		return types.ParseEnumValue(enumValues, uint16(v))
	case tree.P_uint64:
		v, ok := value.Uint64()
		if !ok || v > uint64(^uint16(0)) {
			return 0, moerr.NewInvalidInputNoCtx("invalid ENUM index")
		}
		return types.ParseEnumValue(enumValues, uint16(v))
	default:
		return types.ParseEnum(enumValues, value.String())
	}
}

func mysqlSetIgnoreLiteralBits(setValues string, value *tree.NumVal) uint64 {
	if value.ValType == tree.P_int64 {
		if v, ok := value.Int64(); ok && v >= 0 {
			return uint64(v) & mysqlSetValidBitmap(setValues)
		}
	}
	if value.ValType == tree.P_uint64 {
		if v, ok := value.Uint64(); ok {
			return v & mysqlSetValidBitmap(setValues)
		}
	}

	bits := uint64(0)
	for _, member := range strings.Split(value.String(), ",") {
		memberBits, err := types.ParseSet(setValues, member)
		if err == nil {
			bits |= memberBits
		}
	}
	return bits
}

func mysqlSetValidBitmap(setValues string) uint64 {
	memberCount := len(strings.Split(setValues, ","))
	if memberCount >= types.MaxSetMembers {
		return ^uint64(0)
	}
	return (uint64(1) << uint(memberCount)) - 1
}

func mysqlYearLiteralIsValid(value *tree.NumVal) bool {
	switch value.ValType {
	case tree.P_int64:
		v, ok := value.Int64()
		if !ok {
			return false
		}
		_, err := types.ParseMoYearFromInt(v)
		return err == nil
	case tree.P_uint64:
		v, ok := value.Uint64()
		if !ok || v > uint64(^uint64(0)>>1) {
			return false
		}
		_, err := types.ParseMoYearFromInt(int64(v))
		return err == nil
	case tree.P_char:
		_, err := types.ParseMoYear(value.String())
		return err == nil
	default:
		return true
	}
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

func (bc *BindContext) setMySQLSpecialCanonicalType(colPos int32, typ *plan.Type) {
	if bc.mysqlSpecialCanonicalTypes == nil {
		bc.mysqlSpecialCanonicalTypes = make(map[int32]*plan.Type)
	}
	bc.mysqlSpecialCanonicalTypes[colPos] = DeepCopyType(typ)
}

func (bc *BindContext) mysqlSpecialCanonicalTypeForExpr(expr *plan.Expr) *plan.Type {
	if expr == nil {
		return nil
	}
	col := expr.GetCol()
	if col == nil {
		return nil
	}
	if col.RelPos == bc.projectTag && col.ColPos >= 0 && int(col.ColPos) < len(bc.projects) {
		if typ, recorded := bc.mysqlSpecialCanonicalTypes[col.ColPos]; recorded {
			return DeepCopyType(typ)
		}
		project := bc.projects[col.ColPos]
		if project == nil {
			return nil
		}
		if projectCol := project.GetCol(); projectCol != nil &&
			projectCol.RelPos == col.RelPos && projectCol.ColPos == col.ColPos {
			return nil
		}
		return bc.mysqlSpecialCanonicalTypeForExpr(project)
	}
	binding := bc.bindingByTag[col.RelPos]
	if binding == nil || col.ColPos < 0 || int(col.ColPos) >= len(binding.mysqlSpecialCanonicalTypes) {
		return nil
	}
	return DeepCopyType(binding.mysqlSpecialCanonicalTypes[col.ColPos])
}

func (bc *BindContext) mysqlSpecialCanonicalTypeForProject(colPos int32) *plan.Type {
	if typ, recorded := bc.mysqlSpecialCanonicalTypes[colPos]; recorded {
		return DeepCopyType(typ)
	}
	if colPos < 0 || int(colPos) >= len(bc.projects) {
		return nil
	}
	return bc.mysqlSpecialCanonicalTypeForExpr(bc.projects[colPos])
}

func mysqlSpecialTypeFromProvenance(provenance OutputColumnProvenance) *plan.Type {
	if provenance.State != ProvenanceSingleSource || provenance.Source == nil ||
		!isEnumOrSetPlanType(&provenance.Source.Metadata.Typ) {
		return nil
	}
	return DeepCopyType(&provenance.Source.Metadata.Typ)
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

// mysqlSpecialNumericTypeReversible is stricter than the ORDER BY provenance
// guard because numeric conversion must also preserve ENUM's stored error
// member. An ENUM with an empty label displays both ordinal 0 and that label as
// the empty string; grouping can merge them, so the numeric value cannot be
// recovered from the group output alone.
func mysqlSpecialNumericTypeReversible(typ *plan.Type) bool {
	if !mysqlSpecialOrderTypeReversible(typ) {
		return false
	}
	if isEnumPlanType(typ) {
		for _, value := range strings.Split(typ.Enumvalues, ",") {
			if value == "" {
				return false
			}
		}
	}
	return true
}

// useStoredMySQLSpecialTypesForNumericContractWithProvenance extends the
// structural row-level rewrite with a narrow recovery for display values that
// crossed a GROUP BY or transparent query boundary. It only rewrites operands
// whose resolved function contract is numeric and whose source provenance is
// still the exact ENUM/SET display value.
func (b *baseBinder) useStoredMySQLSpecialTypesForNumericContractWithProvenance(
	ctx context.Context,
	name string,
	args []*plan.Expr,
) ([]*plan.Expr, error) {
	result := useStoredMySQLSpecialTypesForNumericContract(ctx, name, args)
	if b == nil || b.ctx == nil {
		return result, nil
	}

	hasProvenanceCandidate := false
	for _, arg := range args {
		if _, direct := storedMySQLSpecialTypeExpr(arg); direct {
			continue
		}
		if b.ctx.mysqlSpecialOrderTypeForExpr(arg) != nil {
			hasProvenanceCandidate = true
			break
		}
	}
	if !hasProvenanceCandidate {
		return result, nil
	}
	if mysqlSpecialNumericInList(name, args) {
		if _, direct := storedMySQLSpecialTypeExpr(args[0]); !direct {
			storageType := b.ctx.mysqlSpecialOrderTypeForExpr(args[0])
			if storageType != nil && mysqlSpecialNumericTypeReversible(storageType) {
				recovered, err := makeMySQLSpecialNumericValue(ctx, args[0], storageType)
				if err != nil {
					return nil, err
				}
				result = append([]*plan.Expr(nil), result...)
				result[0] = recovered
				return result, nil
			}
		}
	}

	displayTypes := make([]types.Type, len(args))
	for i, arg := range args {
		displayTypes[i] = makeTypeByPlan2Expr(arg)
	}
	resolved, err := function.GetFunctionByName(ctx, name, displayTypes)
	if err != nil {
		return result, nil
	}
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	if !shouldCast || len(targets) != len(args) {
		return result, nil
	}

	changed := false
	for i, arg := range args {
		if !targets[i].IsNumeric() {
			continue
		}
		if _, direct := storedMySQLSpecialTypeExpr(arg); direct {
			continue
		}
		storageType := b.ctx.mysqlSpecialOrderTypeForExpr(arg)
		if storageType == nil || !mysqlSpecialNumericTypeReversible(storageType) {
			continue
		}

		recovered, err := makeMySQLSpecialNumericValue(ctx, arg, storageType)
		if err != nil {
			return nil, err
		}
		if !changed {
			result = append([]*plan.Expr(nil), result...)
			changed = true
		}
		result[i] = recovered
	}
	return result, nil
}

func makeMySQLSpecialNumericValue(
	ctx context.Context,
	displayExpr *plan.Expr,
	storageType *plan.Type,
) (*plan.Expr, error) {
	if isEnumPlanType(storageType) {
		_, valueToIndex, _, err := mysqlSpecialTypeFuncNames(storageType)
		if err != nil {
			return nil, err
		}
		// A reversible ENUM definition has no empty label, so an empty grouped
		// display uniquely represents the error member at ordinal zero. The
		// legacy parser cannot convert that display, so feed it a valid label
		// first and restore ordinal zero after parsing. This keeps the serialized
		// conversion on the two-argument overload understood by old workers.
		emptyDisplay, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			DeepCopyExpr(displayExpr), makePlan2StringConstExprWithType(""),
		})
		if err != nil {
			return nil, err
		}
		firstLabel := strings.Split(storageType.Enumvalues, ",")[0]
		safeDisplay, err := BindFuncExprImplByPlanExpr(ctx, "if", []*plan.Expr{
			DeepCopyExpr(emptyDisplay),
			makePlan2StringConstExprWithType(firstLabel),
			DeepCopyExpr(displayExpr),
		})
		if err != nil {
			return nil, err
		}
		numericExpr, err := BindFuncExprImplByPlanExpr(ctx, valueToIndex, []*plan.Expr{
			makePlan2StringConstExprWithType(storageType.Enumvalues),
			safeDisplay,
		})
		if err != nil {
			return nil, err
		}
		numericExpr.Typ.NotNullable = displayExpr.Typ.NotNullable
		numericExpr.Typ.Enumvalues = storageType.Enumvalues

		numericZero := &plan.Expr{
			Typ: *DeepCopyType(storageType),
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_EnumVal{EnumVal: 0},
			}},
		}
		recovered, err := BindFuncExprImplByPlanExpr(ctx, "if", []*plan.Expr{
			DeepCopyExpr(emptyDisplay), numericZero, numericExpr,
		})
		if err != nil {
			return nil, err
		}
		recovered.Typ.NotNullable = displayExpr.Typ.NotNullable
		recovered.Typ.Enumvalues = storageType.Enumvalues
		return recovered, nil
	}

	if isSetPlanType(storageType) {
		numericExpr, err := makeMySQLSpecialOrderKey(ctx, displayExpr, storageType)
		if err != nil {
			return nil, err
		}
		// The SET value-to-index function returns an ordinary uint64 bitmap.
		// Do not let later SET-aware casts reinterpret it as a display value.
		numericExpr.Typ.Enumvalues = ""
		return numericExpr, nil
	}
	return nil, moerr.NewInternalError(ctx, "invalid ENUM/SET numeric provenance")
}

// setTypeHasEmptyMember identifies a SET definition that has at least one
// member whose display label is empty. Every bitmap containing only such a
// member renders as the empty string, so display text alone cannot recover
// the stored bitmap (the empty member may occur at any declaration position).
func setTypeHasEmptyMember(typ *plan.Type) bool {
	if !isSetPlanType(typ) {
		return false
	}
	values, err := types.NormalizeSetValues(strings.Split(typ.Enumvalues, ","))
	if err != nil {
		return false
	}
	for _, value := range values {
		if value == "" {
			return true
		}
	}
	return false
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
	if columnSRID, columnDefined := geometrySRIDValue(&targetType); columnDefined &&
		!geometryExprHasDeferredSRID(expr) && !geometrySRIDSourceIsStaticNull(expr) {
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

// validateGeometryAssignmentSRID rechecks a preserved DML assignment cast
// after execute-time specialization. The write root is intentionally kept
// stable for SQL-mode/physical-layout semantics, but its source geometry's
// SRID is value-dependent and must still agree with a constrained target.
func validateGeometryAssignmentSRID(ctx context.Context, expr *Expr, targetType Type) error {
	if !isGeometryPlanType(&targetType) {
		return nil
	}
	columnSRID, columnDefined := geometrySRIDValue(&targetType)
	if !columnDefined || expr == nil {
		return nil
	}
	source := expr
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.GetObjName(), moGeometryCastToSubtypeFun) && len(fn.Args) >= 2 {
		source = fn.Args[len(fn.Args)-1]
	}
	if source == nil || geometrySRIDSourceIsStaticNull(source) || !isGeometryPlanType(&source.Typ) {
		return nil
	}
	valueSRID, _ := geometrySRIDValue(&source.Typ)
	if valueSRID != columnSRID {
		return moerr.NewInvalidInputf(ctx,
			"The SRID of the geometry does not match the SRID of the column. The SRID of the geometry is %d, but the SRID of the column is %d.",
			valueSRID, columnSRID)
	}
	return nil
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

// A bare NULL is represented by the binder's legacy T_text placeholder.
// Explicitly typed NULL expressions must keep participating in type merging.
func isPureNullLiteralExpr(expr *Expr) bool {
	return isNullLiteralExpr(expr) &&
		types.T(expr.Typ.Id) == types.T_text && expr.Typ.Charset == uint32(types.CharsetLegacy)
}
