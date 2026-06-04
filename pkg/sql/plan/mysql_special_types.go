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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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
	return typ != nil && typ.Id == int32(types.T_geometry)
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

func geometrySubtypeName(typ *plan.Type) string {
	if !isGeometryPlanType(typ) {
		return ""
	}
	subtype, _, _ := decodeGeometryMetadata(typ.GetEnumvalues())
	return subtype
}

func geometrySRIDValue(typ *plan.Type) (uint32, bool) {
	if !isGeometryPlanType(typ) {
		return 0, false
	}
	_, srid, ok := decodeGeometryMetadata(typ.GetEnumvalues())
	return srid, ok
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

func decodeGeometryMetadata(metadata string) (subtype string, srid uint32, sridDefined bool) {
	metadata = strings.TrimSpace(metadata)
	if metadata == "" {
		return "", 0, false
	}
	parts := strings.Split(metadata, ";")
	start := 0
	head := strings.TrimSpace(parts[0])
	if !strings.HasPrefix(strings.ToUpper(head), "SRID=") {
		subtype = normalizeGeometrySubtype(head)
		start = 1
	}
	for _, part := range parts[start:] {
		part = strings.TrimSpace(part)
		if len(part) < len("SRID=") || !strings.EqualFold(part[:5], "SRID=") {
			continue
		}
		value := strings.TrimSpace(part[5:])
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			continue
		}
		return subtype, uint32(parsed), true
	}
	return subtype, 0, false
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
	if types.T(expr.Typ.Id) == types.T_any || isNullLiteralExpr(expr) {
		expr.Typ = targetType
		return expr, nil
	}
	targetMetadata := targetType.Enumvalues
	if isGeometryPlanType(&expr.Typ) && expr.Typ.GetEnumvalues() == targetMetadata {
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
