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

func isEnumPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_enum) && len(typ.GetEnumvalues()) > 0
}

// isSetPlanType identifies a SET column. SET is stored as T_uint64 with a non-empty
// Enumvalues field holding the comma-separated member list. This is the sole
// discriminator between a plain uint64 column and a SET column — no other code
// path should populate Enumvalues on a T_uint64 type.
func sameTypeMemberDomain(left, right *plan.Type) bool {
	if left.Id == int32(types.T_enum) && right.Id == int32(types.T_enum) {
		return types.EnumMembersEqual(left.Enumvalues, left.EnumEncoding, right.Enumvalues, right.EnumEncoding)
	}
	return left.Enumvalues == right.Enumvalues && left.EnumEncoding == right.EnumEncoding
}

// Slot 1 remains the value for both persisted overload shapes.
func mysqlSpecialTypeArgs(typ *plan.Type, value *plan.Expr) []*plan.Expr {
	args := []*plan.Expr{makePlan2StringConstExprWithType(typ.Enumvalues), value}
	if typ.Id == int32(types.T_enum) && typ.EnumEncoding != 0 {
		args = append(args, makePlan2Int64ConstExprWithType(int64(typ.EnumEncoding)))
	}
	return args
}

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
		members, err := types.DecodeEnumMembers(targetType.Enumvalues, targetType.EnumEncoding)
		if err != nil {
			return nil, true, err
		}
		index, err := mysqlEnumMembersLiteralIndex(members, value)
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

func mysqlEnumMembersLiteralIndex(members types.EnumMembers, value *tree.NumVal) (types.Enum, error) {
	switch value.ValType {
	case tree.P_int64:
		v, ok := value.Int64()
		if !ok || v < 0 || v > int64(^uint16(0)) {
			return 0, moerr.NewInvalidInputNoCtx("invalid ENUM index")
		}
		return members.ParseValue(uint16(v))
	case tree.P_uint64:
		v, ok := value.Uint64()
		if !ok || v > uint64(^uint16(0)) {
			return 0, moerr.NewInvalidInputNoCtx("invalid ENUM index")
		}
		return members.ParseValue(uint16(v))
	default:
		return members.Parse(value.String())
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
		if validMySQLSpecialDisplayCall(fn) && isEnumOrSetPlanType(&fn.Args[1].Typ) {
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

// setOmittedCharWidenProject records that the given select-list position's
// defining AST expression is the omitted-length CAST/CONVERT-to-CHAR
// widening (#32336, #32374, PR #32438 review). Planner-local provenance
// only: see BindContext.omittedCharWidenProjects.
func (bc *BindContext) setOmittedCharWidenProject(colPos int32) {
	if bc.omittedCharWidenProjects == nil {
		bc.omittedCharWidenProjects = make(map[int32]struct{})
	}
	bc.omittedCharWidenProjects[colPos] = struct{}{}
}

// isOmittedCharWidenProject reads back the omitted-length CAST/CONVERT-to-
// CHAR provenance recorded for the given select-list position.
func (bc *BindContext) isOmittedCharWidenProject(colPos int32) bool {
	_, recorded := bc.omittedCharWidenProjects[colPos]
	return recorded
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
	return left != nil && right != nil && left.Id == right.Id && sameTypeMemberDomain(left, right)
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
		values, err := types.DecodeEnumMembers(typ.Enumvalues, typ.EnumEncoding)
		if err != nil {
			return false
		}
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if value == "" {
				return false
			} // Ordinal zero has the same display.
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

// mysqlSpecialNumericTypeForExpr also resolves correlated references using the
// same binder-bearing ancestor traversal as baseBindColRef. The temporary
// ColRef is only for provenance lookup; recovery keeps the original CorrColRef.
func (bc *BindContext) mysqlSpecialNumericTypeForExpr(expr *plan.Expr) *plan.Type {
	if bc == nil || expr == nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return nil
	}
	corr := expr.GetCorr()
	if corr == nil {
		return bc.mysqlSpecialOrderTypeForExpr(expr)
	}
	if corr.Depth <= 0 {
		return nil
	}
	owner := bc
	for depth := int32(0); depth < corr.Depth; depth++ {
		owner = owner.parent
		for owner != nil && owner.binder == nil {
			owner = owner.parent
		}
		if owner == nil {
			return nil
		}
	}
	return owner.mysqlSpecialOrderTypeForExpr(&plan.Expr{
		Typ:  expr.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: corr.RelPos, ColPos: corr.ColPos}},
	})
}

func (bc *BindContext) mysqlSpecialNumericTypeForProject(colPos int32) *plan.Type {
	if typ, recorded := bc.mysqlSpecialOrderTypes[colPos]; recorded {
		return DeepCopyType(typ)
	}
	if colPos < 0 || int(colPos) >= len(bc.projects) {
		return nil
	}
	return bc.mysqlSpecialNumericTypeForExpr(bc.projects[colPos])
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
	if err := b.rejectUnprovenCorrelatedSpecialIn(ctx, name, args); err != nil {
		return nil, err
	}

	hasSpecial, hasProvenanceCandidate := false, false
	for _, arg := range args {
		if _, direct := storedMySQLSpecialTypeExpr(arg); direct {
			hasSpecial = true
		} else if b.ctx.mysqlSpecialNumericTypeForExpr(arg) != nil {
			hasSpecial, hasProvenanceCandidate = true, true
		}
	}
	if !hasSpecial {
		return result, nil
	}

	// An operator such as '+' may resolve two display strings as concatenation.
	// A proven special column still uses the numeric operator contract.
	if name == "+" && stringPlusConcatenates(args) {
		var numeric []*plan.Expr
		for i, arg := range args {
			raw, direct := storedMySQLSpecialTypeExpr(arg)
			typ := b.ctx.mysqlSpecialNumericTypeForExpr(arg)
			if !direct && typ == nil {
				continue
			}
			if !direct {
				if !mysqlSpecialOrderTypeReversible(typ) {
					return nil, moerr.NewNYI(ctx, "numeric expression with non-reversible ENUM/SET display")
				}
				var err error
				raw, err = makeMySQLSpecialNumericValue(ctx, arg, typ)
				if err != nil {
					return nil, err
				}
			}
			if numeric == nil {
				numeric = append([]*plan.Expr(nil), result...)
			}
			numeric[i] = raw
		}
		if numeric != nil {
			return numeric, nil
		}
	}

	if !hasProvenanceCandidate {
		return result, nil
	}
	if mysqlSpecialNumericInList(name, args) {
		if _, direct := storedMySQLSpecialTypeExpr(args[0]); !direct {
			storageType := b.ctx.mysqlSpecialNumericTypeForExpr(args[0])
			if storageType != nil && mysqlSpecialOrderTypeReversible(storageType) {
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

	targets, shouldCast := mysqlSpecialNumericFunctionTargets(ctx, name, args)
	if !shouldCast {
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
		if err := rejectNonReversibleCorrelatedNumeric(ctx, b.ctx, arg); err != nil {
			return nil, err
		}
		storageType := b.ctx.mysqlSpecialNumericTypeForExpr(arg)
		if storageType == nil || !mysqlSpecialOrderTypeReversible(storageType) {
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

// A mixed or dynamic IN list may compare a display value numerically.
// Only NULL and literal string members are known to need no numeric recovery.
func mysqlSpecialInListMayNeedNumeric(name string, args []*plan.Expr) bool {
	if (name != "in" && name != "not_in" && name != "partition_in") || len(args) != 2 {
		return false
	}
	list := args[1].GetList()
	if list == nil {
		return false
	}
	for _, member := range list.List {
		if !mysqlSpecialInOperandIsLexical(member) {
			return true
		}
	}
	return false
}

func mysqlSpecialInOperandIsLexical(expr *plan.Expr) bool {
	lit := expr.GetLit()
	return lit != nil && (lit.Isnull || (lit.Src == nil && types.T(expr.Typ.Id).IsMySQLString()))
}

func correlatedMySQLSpecialDisplay(bindCtx *BindContext, expr *plan.Expr) bool {
	if bindCtx == nil || expr == nil || !hasCorrCol(expr) {
		return false
	}
	if _, raw := storedMySQLSpecialTypeExpr(expr); raw {
		return true
	}
	return bindCtx.mysqlSpecialNumericTypeForExpr(expr) != nil
}

// IN expands some lists into separate comparisons. Do not let that expansion
// lose correlated special-type provenance on either side of a numeric test.
func (b *baseBinder) rejectUnprovenCorrelatedSpecialIn(ctx context.Context, name string, args []*plan.Expr) error {
	if (name != "in" && name != "not_in" && name != "partition_in") || len(args) != 2 || args[1].GetList() == nil {
		return nil
	}
	if correlatedMySQLSpecialDisplay(b.ctx, args[0]) && mysqlSpecialInListMayNeedNumeric(name, args) {
		// A direct display wrapper still has the original stored value. The
		// existing IN owner uses it for an entirely numeric, static list.
		_, raw := storedMySQLSpecialTypeExpr(args[0])
		proven := raw && mysqlSpecialNumericInList(name, args)
		for _, member := range args[1].GetList().List {
			lit := member.GetLit()
			if containsDynamicParam(member) || (lit != nil && lit.Src != nil) {
				proven = false
			}
		}
		if !proven {
			return moerr.NewNYI(ctx, "correlated special IN comparison requires a proven numeric contract")
		}
	}
	if !mysqlSpecialInOperandIsLexical(args[0]) {
		for _, member := range args[1].GetList().List {
			if correlatedMySQLSpecialDisplay(b.ctx, member) {
				return moerr.NewNYI(ctx, "correlated special IN comparison requires a proven numeric contract")
			}
		}
	}
	return nil
}

// A correlated grouped display with an ambiguous domain cannot be converted
// back to its stored ordinal or bitmap without guessing.
func rejectNonReversibleCorrelatedNumeric(ctx context.Context, bindCtx *BindContext, expr *plan.Expr) error {
	if bindCtx != nil && hasCorrCol(expr) {
		if _, raw := storedMySQLSpecialTypeExpr(expr); raw {
			return nil
		}
		if typ := bindCtx.mysqlSpecialNumericTypeForExpr(expr); typ != nil && !mysqlSpecialOrderTypeReversible(typ) {
			return moerr.NewNYI(ctx, "numeric correlated value with non-reversible ENUM/SET display")
		}
	}
	return nil
}

// noFromSpecialScalarProjection exposes the same singleton projection that
// flattenSubquery would expose, but only for a direct special-type column.
// Keeping this proof narrow avoids moving subquery execution into an operand.
func (b *baseBinder) noFromSpecialScalarProjection(ast tree.Expr, expr *plan.Expr) *plan.Expr {
	if b == nil || b.builder == nil || b.ctx == nil || expr == nil || expr.GetSub() == nil ||
		expr.GetSub().Typ != plan.SubqueryRef_SCALAR || expr.GetSub().RowSize != 1 {
		return nil
	}
	subquery, ok := scalarSubqueryExpr(ast)
	if !ok || subquery.Exists {
		return nil
	}
	var selectStmt *tree.Select
	switch stmt := subquery.Select.(type) {
	case *tree.Select:
		selectStmt = stmt
	case *tree.ParenSelect:
		selectStmt = stmt.Select
	}
	if selectStmt == nil || selectStmt.With != nil || len(selectStmt.OrderBy) != 0 ||
		selectStmt.Limit != nil || selectStmt.RankOption != nil || selectStmt.TimeWindow != nil {
		return nil
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil ||
		len(clause.Exprs) != 1 || clause.From == nil || len(clause.From.Tables) != 1 {
		return nil
	}
	if _, ok := unwrapParenExpr(clause.Exprs[0].Expr).(*tree.UnresolvedName); !ok {
		return nil
	}
	from, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
	if !ok {
		return nil
	}
	table, ok := from.Expr.(*tree.TableName)
	if !ok || table.ObjectName != "" {
		return nil
	}
	node := b.builder.noFromSubqueryProject(expr.GetSub().NodeId)
	if node == nil || node.Limit != nil || node.Offset != nil || node.RankOption != nil ||
		len(node.ProjectList) != 1 || len(node.FilterList) != 0 || len(node.OrderBy) != 0 {
		return nil
	}
	project := node.ProjectList[0]
	if project == nil {
		return nil
	}
	corr := project.GetCorr()
	if corr == nil {
		fn := project.GetF()
		raw, valid := storedMySQLSpecialTypeExpr(project)
		if !valid || fn == nil || fn.Func == nil || fn.Args[0].GetLit().Src != nil {
			return nil
		}
		id, overload := function.DecodeOverloadID(fn.Func.Obj)
		if _, exists := function.GetFunctionByIdWithoutError(fn.Func.Obj); !exists {
			return nil
		}
		if !((id == function.CAST_INDEX_TO_VALUE && ((overload == 0 && len(fn.Args) == 2) ||
			(overload == 1 && len(fn.Args) == 3 && fn.Args[2].GetLit().Src == nil))) ||
			(id == function.CAST_INDEX_TO_SET_VALUE && overload == 0 && len(fn.Args) == 2)) {
			return nil
		}
		corr = raw.GetCorr()
	}
	if corr == nil || corr.Depth != 1 {
		return nil
	}
	exposed, remainsCorrelated := decreaseDepth(DeepCopyExpr(project))
	if remainsCorrelated {
		return nil
	}
	if _, raw := storedMySQLSpecialTypeExpr(exposed); !raw && b.ctx.mysqlSpecialNumericTypeForExpr(exposed) == nil {
		return nil
	}
	return exposed
}

// Apply the existing numeric-contract decision to the exposed projections,
// publishing only operands that it actually recognizes as numeric.
func (b *baseBinder) useStoredNoFromSpecialScalarArgs(name string, astArgs []tree.Expr, args []*plan.Expr) ([]*plan.Expr, error) {
	if len(astArgs) != len(args) {
		return args, nil
	}
	if (name == "in" || name == "not_in" || name == "partition_in") && len(args) == 2 &&
		args[1].GetList() != nil && !mysqlSpecialInOperandIsLexical(args[0]) {
		if tuple, ok := unwrapParenExpr(astArgs[1]).(*tree.Tuple); ok && len(tuple.Exprs) == len(args[1].GetList().List) {
			for i, member := range args[1].GetList().List {
				if b.noFromSpecialScalarProjection(tuple.Exprs[i], member) != nil {
					return nil, moerr.NewNYI(b.GetContext(), "numeric scalar IN list needs a proven lossless shared type")
				}
			}
		}
	}
	var staged []*plan.Expr
	var candidates []bool
	for i := range args {
		if exposed := b.noFromSpecialScalarProjection(astArgs[i], args[i]); exposed != nil {
			if staged == nil {
				staged = append([]*plan.Expr(nil), args...)
				candidates = make([]bool, len(args))
			}
			staged[i], candidates[i] = exposed, true
		}
	}
	if staged == nil {
		return args, nil
	}
	if candidates[0] && mysqlSpecialInListMayNeedNumeric(name, staged) && !mysqlSpecialNumericInList(name, staged) {
		return nil, moerr.NewNYI(b.GetContext(), "numeric scalar IN list needs a proven lossless shared type")
	}
	rewritten, err := b.useStoredMySQLSpecialTypesForNumericContractWithProvenance(b.GetContext(), name, staged)
	if err != nil {
		return nil, err
	}
	var result []*plan.Expr
	for i, candidate := range candidates {
		if !candidate {
			continue
		}
		value := rewritten[i]
		if value == staged[i] {
			if typ := b.ctx.mysqlSpecialNumericTypeForExpr(staged[i]); typ != nil && !mysqlSpecialOrderTypeReversible(typ) {
				targets, hasTargets := mysqlSpecialNumericFunctionTargets(b.GetContext(), name, staged)
				if (hasTargets && targets[i].IsNumeric()) || (i == 0 && mysqlSpecialNumericInList(name, staged)) {
					return nil, moerr.NewNYI(b.GetContext(), "numeric scalar with non-reversible ENUM/SET display")
				}
			}
			continue
		}
		if result == nil {
			result = append([]*plan.Expr(nil), args...)
		}
		result[i] = value
		if i == 0 && mysqlSpecialNumericInList(name, staged) {
			// Reuse the IN owner's typed vector so grouped recovery executes
			// once. Do not round fractional or otherwise unsafe members.
			if types.T(result[i].Typ.Id) == types.T_enum {
				intType := types.T_int64.ToType()
				result[i], err = appendCastBeforeExpr(b.GetContext(), result[i], makePlan2Type(&intType))
				if err != nil {
					return nil, err
				}
			}
			list := DeepCopyExpr(args[1])
			for _, member := range list.GetList().List {
				if lit := member.GetLit(); lit != nil && lit.Isnull {
					if lit.Src != nil {
						return nil, moerr.NewNYI(b.GetContext(), "numeric scalar IN list needs a proven lossless shared type")
					}
					member.Typ = result[i].Typ
					member.Typ.NotNullable = false
				} else {
					memberType, targetType := makeTypeByPlan2Expr(member), makeTypeByPlan2Expr(result[i])
					if (member.GetP() != nil && !memberType.Eq(targetType)) || !checkNoNeedCast(memberType, targetType, member) {
						return nil, moerr.NewNYI(b.GetContext(), "numeric scalar IN list needs a proven lossless shared type")
					}
				}
			}
			result[1] = list
		}
	}
	if result != nil {
		return result, nil
	}
	return args, nil
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
		// legacy domains on the two-argument overload understood by old workers.
		// Framed domains use the existing encoding-aware overload.
		emptyDisplay, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			DeepCopyExpr(displayExpr), makePlan2StringConstExprWithType(""),
		})
		if err != nil {
			return nil, err
		}
		members, err := types.DecodeEnumMembers(storageType.Enumvalues, storageType.EnumEncoding)
		if err != nil {
			return nil, err
		}
		firstLabel := members[0]
		safeDisplay, err := BindFuncExprImplByPlanExpr(ctx, "if", []*plan.Expr{
			DeepCopyExpr(emptyDisplay),
			makePlan2StringConstExprWithType(firstLabel),
			DeepCopyExpr(displayExpr),
		})
		if err != nil {
			return nil, err
		}
		numericExpr, err := BindFuncExprImplByPlanExpr(ctx, valueToIndex, mysqlSpecialTypeArgs(storageType, safeDisplay))
		if err != nil {
			return nil, err
		}
		numericExpr.Typ.NotNullable = displayExpr.Typ.NotNullable
		numericExpr.Typ.Enumvalues = storageType.Enumvalues
		numericExpr.Typ.EnumEncoding = storageType.EnumEncoding

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
		recovered.Typ.EnumEncoding = storageType.EnumEncoding
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
		numericExpr.Typ.EnumEncoding = 0
		return numericExpr, nil
	}
	return nil, moerr.NewInternalError(ctx, "invalid ENUM/SET numeric provenance")
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
	orderKey, err := BindFuncExprImplByPlanExpr(ctx, valueToIndex, mysqlSpecialTypeArgs(storageType, DeepCopyExpr(displayExpr)))
	if err != nil {
		return nil, err
	}
	orderKey.Typ.NotNullable = displayExpr.Typ.NotNullable
	orderKey.Typ.Enumvalues = storageType.Enumvalues
	orderKey.Typ.EnumEncoding = storageType.EnumEncoding
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

	args := []tree.Expr{tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char), astExpr}
	if targetType.Id == int32(types.T_enum) && targetType.EnumEncoding != 0 {
		args = append(args, tree.NewNumVal(int64(targetType.EnumEncoding), strconv.FormatUint(uint64(targetType.EnumEncoding), 10), false, tree.P_int64))
	}
	return &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(funcName)),
		Type:  tree.FUNC_TYPE_DEFAULT,
		Exprs: args,
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

// validMySQLSpecialDisplayCall recognizes only a well-formed presentation
// wrapper for its stored operand. Dynamic or mismatched definitions must still
// execute their conversion and cannot be unwrapped to bypass an error.
func validMySQLSpecialDisplayCall(fn *plan.Function) bool {
	if fn == nil || fn.Func == nil || (len(fn.Args) != 2 && len(fn.Args) != 3) ||
		fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	literal := fn.Args[0].GetLit()
	if literal == nil || literal.Isnull {
		return false
	}
	value, ok := literal.Value.(*plan.Literal_Sval)
	if !ok {
		return false
	}
	typ := &fn.Args[1].Typ
	if value.Sval != typ.Enumvalues {
		return false
	}
	if fn.Func.ObjName == moSetCastIndexToValueFun {
		return len(fn.Args) == 2 && isSetPlanType(typ) && typ.EnumEncoding == 0
	}
	if fn.Func.ObjName != moEnumCastIndexToValueFun || !isEnumPlanType(typ) {
		return false
	}
	var encoding uint32
	if len(fn.Args) == 3 {
		if fn.Args[2] == nil {
			return false
		}
		tag := fn.Args[2].GetLit()
		if tag == nil || tag.Isnull {
			return false
		}
		v, ok := tag.Value.(*plan.Literal_I64Val)
		if !ok || v.I64Val < 0 || v.I64Val > int64(types.EnumEncodingFramed) {
			return false
		}
		encoding = uint32(v.I64Val)
	}
	if encoding != typ.EnumEncoding {
		return false
	}
	_, err := types.DecodeEnumMembers(value.Sval, encoding)
	return err == nil
}
