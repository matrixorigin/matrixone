// Copyright 2021 - 2022 Matrix Origin
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
	"bytes"
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// func appendQueryNode(query *Query, node *Node) int32 {
// 	nodeID := int32(len(query.Nodes))
// 	node.NodeId = nodeID
// 	query.Nodes = append(query.Nodes, node)

// 	return nodeID
// }

// applyLockTableFallback upgrades cardinality-known lock targets before their
// first row lock is acquired. Shared targets need this planner fallback because
// converting already-shared rows is not always possible without changing
// Shared compatibility. Exclusive targets normally coarsen on the owner side;
// the only planner exception is an admitted unrestricted single-target UPDATE:
// its target universe is the whole table, every written keyspace has a total
// range, and no earlier lock target would have its order reversed. Bounded
// predicates retain row/range locks even when their estimate is large.
func applyLockTableFallback(builder *QueryBuilder) {
	proc := builder.compCtx.GetProcess()
	if proc == nil || proc.Base.LockService == nil {
		return
	}
	maxRows := float64(proc.Base.LockService.GetConfig().MaxLockRowCount)
	if maxRows <= 0 {
		return
	}
	fullUpdateRows := estimateFullTableUpdateRows(builder)

	for _, node := range builder.qry.Nodes {
		if node.NodeType != plan.Node_LOCK_OP ||
			node.Stats == nil {
			continue
		}
		for _, target := range node.LockTargets {
			_, fullUpdateTarget := builder.fullTableUpdateLockTargets[target]
			estimatedRows := node.Stats.Outcnt
			if fullUpdateTarget && fullUpdateRows > estimatedRows {
				estimatedRows = fullUpdateRows
			}
			if estimatedRows <= maxRows {
				continue
			}
			if target.Mode == lockpb.LockMode_Shared {
				target.LockTable = true
				continue
			}
			if fullUpdateTarget {
				target.LockTable = true
			}
		}
	}
}

// estimateFullTableUpdateRows returns the unfiltered cardinality of the target
// scan for an UPDATE already proven to cover its complete keyspace. Join and
// projection estimates can undercount the final LOCK_OP, especially with stale
// hidden-index statistics. TableCnt is safe only under that semantic proof: a
// bounded UPDATE must never acquire a table lock just because its source table
// is large.
func estimateFullTableUpdateRows(builder *QueryBuilder) float64 {
	if !builder.hasFullTableUpdateSourceTableID {
		return 0
	}

	var rows float64
	for _, node := range builder.qry.Nodes {
		if node.NodeType != plan.Node_TABLE_SCAN ||
			node.TableDef == nil ||
			node.TableDef.TblId != builder.fullTableUpdateSourceTableID ||
			node.Stats == nil {
			continue
		}
		candidate := node.Stats.TableCnt
		if candidate <= 0 || math.IsNaN(candidate) || math.IsInf(candidate, 0) {
			continue
		}
		if candidate > rows {
			rows = candidate
		}
	}
	return rows
}

// GetFunctionArgTypeStrFromAst function arg type do not have scale and width, it depends on the data that it process
func GetFunctionArgTypeStrFromAst(arg tree.FunctionArg) (string, error) {
	argDecl := arg.(*tree.FunctionArgDecl)
	return GetFunctionTypeStrFromAst(argDecl.Type)
}

func GetFunctionTypeStrFromAst(typRef tree.ResolvableTypeReference) (string, error) {
	typ, err := getTypeFromAst(moerr.Context(), typRef)
	if err != nil {
		return "", err
	}
	ret := strings.ToLower(types.T(typ.Id).String())
	// do not display precision, because the choice of decimal64 or decimal128 is not exposed to user
	if strings.HasPrefix(ret, "decimal") {
		return "decimal", nil
	}
	return ret, nil
}

func getTypeFromAst(ctx context.Context, typ tree.ResolvableTypeReference) (plan.Type, error) {
	ret, err := getTypeFromAstWithoutCharset(ctx, typ)
	if err != nil {
		return plan.Type{}, err
	}
	// AST-authored types are new metadata. Assign their explicit charset here so
	// CharsetLegacy remains reserved for types decoded from pre-collation catalogs.
	ret.Charset = uint32(types.CharsetType(types.T(ret.Id)))
	return ret, nil
}

func getTypeFromAstWithoutCharset(ctx context.Context, typ tree.ResolvableTypeReference) (plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch defines.MysqlType(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_BIT:
			return plan.Type{Id: int32(types.T_bit), Width: n.InternalType.DisplayWith, Scale: -1}, nil
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return plan.Type{Id: int32(types.T_uint8), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return plan.Type{Id: int32(types.T_int8), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return plan.Type{Id: int32(types.T_uint16), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return plan.Type{Id: int32(types.T_int16), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_INT24:
			if n.InternalType.Unsigned {
				return plan.Type{Id: int32(types.T_uint32), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return plan.Type{Id: int32(types.T_int32), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return plan.Type{Id: int32(types.T_uint64), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return plan.Type{Id: int32(types.T_int64), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return plan.Type{Id: int32(types.T_float32), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return plan.Type{Id: int32(types.T_float64), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_STRING:
			width := n.InternalType.DisplayWith
			// for char type,if we didn't specify the length,
			// the default width should be 1, and for varchar,it's
			// the defaultMaxLength
			fstr := strings.ToLower(n.InternalType.FamilyString)
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				if fstr == "char" {
					width = 1
				} else {
					width = types.MaxVarcharLen
				}
			}
			if fstr == "char" && width > types.MaxCharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, "char", " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if fstr == "varchar" && width > types.MaxVarcharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, "varchar", " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			}
			if fstr == "char" { // type char
				return plan.Type{Id: int32(types.T_char), Width: width}, nil
			}
			return plan.Type{Id: int32(types.T_varchar), Width: width}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			width := n.InternalType.DisplayWith
			// for char type,if we didn't specify the length,
			// the default width should be 1, and for varchar,it's
			// the defaultMaxLength
			// Should always specify length to varbinary.
			fstr := strings.ToLower(n.InternalType.FamilyString)
			// Check explicit casting.
			if fstr == "binary" && n.InternalType.Scale == -1 {
				r := plan.Type{Id: int32(types.T_binary), Width: width}
				r.Scale = -1
				return r, nil
			}
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				if fstr == "char" || fstr == "binary" {
					width = 1
				} else if fstr == types.ArrayFloat32SQLName || fstr == types.ArrayFloat64SQLName || fstr == types.ArrayBF16SQLName || fstr == types.ArrayFloat16SQLName || fstr == types.ArrayInt8SQLName || fstr == types.ArrayUint8SQLName {
					width = types.MaxArrayDimension
				} else {
					width = types.MaxVarcharLen
				}
			}

			if (fstr == "char" || fstr == "binary") && width > types.MaxCharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, fstr, " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if (fstr == "varchar" || fstr == "varbinary") && width > types.MaxVarcharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, fstr, " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			} else if fstr == types.ArrayFloat32SQLName || fstr == types.ArrayFloat64SQLName || fstr == types.ArrayBF16SQLName || fstr == types.ArrayFloat16SQLName || fstr == types.ArrayInt8SQLName || fstr == types.ArrayUint8SQLName {
				if width > types.MaxArrayDimension {
					return plan.Type{}, moerr.NewOutOfRangef(ctx, fstr, " typeLen is over the MaxVectorLen : %v", types.MaxArrayDimension)
				}
				if width < 1 {
					return plan.Type{}, moerr.NewOutOfRange(ctx, fstr, " typeLen cannot be less than 1")
				}
			}
			switch fstr {
			case "char":
				return plan.Type{Id: int32(types.T_char), Width: width}, nil
			case "binary":
				return plan.Type{Id: int32(types.T_binary), Width: width}, nil
			case "varchar":
				return plan.Type{Id: int32(types.T_varchar), Width: width}, nil
			case types.ArrayFloat32SQLName:
				return plan.Type{Id: int32(types.T_array_float32), Width: width}, nil
			case types.ArrayFloat64SQLName:
				return plan.Type{Id: int32(types.T_array_float64), Width: width}, nil
			case types.ArrayBF16SQLName:
				return plan.Type{Id: int32(types.T_array_bf16), Width: width}, nil
			case types.ArrayFloat16SQLName:
				return plan.Type{Id: int32(types.T_array_float16), Width: width}, nil
			case types.ArrayInt8SQLName:
				return plan.Type{Id: int32(types.T_array_int8), Width: width}, nil
			case types.ArrayUint8SQLName:
				return plan.Type{Id: int32(types.T_array_uint8), Width: width}, nil
			}
			// varbinary
			return plan.Type{Id: int32(types.T_varbinary), Width: width}, nil
		case defines.MYSQL_TYPE_DATE:
			return plan.Type{Id: int32(types.T_date)}, nil
		case defines.MYSQL_TYPE_TIME:
			return plan.Type{Id: int32(types.T_time), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_DATETIME:
			// currently the ast's width for datetime's is 26, this is not accurate and may need revise, not important though, as we don't need it anywhere else except to differentiate empty vector.Typ.
			return plan.Type{Id: int32(types.T_datetime), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return plan.Type{Id: int32(types.T_timestamp), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_YEAR:
			return plan.Type{Id: int32(types.T_year), Width: 4}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 38 {
				return plan.Type{Id: int32(types.T_decimal256), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
			}
			if n.InternalType.DisplayWith > 16 {
				return plan.Type{Id: int32(types.T_decimal128), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
			}
			return plan.Type{Id: int32(types.T_decimal64), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_BOOL:
			return plan.Type{Id: int32(types.T_bool)}, nil
		case defines.MYSQL_TYPE_BLOB:
			return plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_TEXT:
			//NOTE: This is an important part where datatype is assigned to the column
			fstr := strings.ToLower(n.InternalType.FamilyString)
			if fstr == "datalink" {
				return plan.Type{Id: int32(types.T_datalink)}, nil
			}
			switch fstr {
			case "tinytext":
				// TEXT-family limits are byte limits in MySQL. Preserve TINYTEXT's
				// 255-byte bound in the plan so DML assignment casts can enforce it
				// without changing the externally visible TEXT type family.
				return plan.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen}, nil
			case "mediumtext":
				return plan.Type{Id: int32(types.T_text), Width: types.MaxMediumTextLen}, nil
			case "longtext":
				// The protocol column-length field is a uint32, but Connector/J
				// exposes LONGTEXT's effective signed maximum as its precision.
				return plan.Type{Id: int32(types.T_text), Width: types.MaxLongTextLen}, nil
			}

			return plan.Type{Id: int32(types.T_text)}, nil
		case defines.MYSQL_TYPE_JSON:
			return plan.Type{Id: int32(types.T_json)}, nil
		case defines.MYSQL_TYPE_TYPED_ARRAY:
			if n.InternalType.ArrayContents == nil {
				return plan.Type{}, moerr.NewInternalError(ctx, "array type missing element type")
			}
			if _, err := getTypeFromAst(ctx, n.InternalType.ArrayContents); err != nil {
				return plan.Type{}, err
			}
			if err := validateTypedArrayElementType(ctx, n.InternalType.ArrayContents); err != nil {
				return plan.Type{}, err
			}
			arrayType := tree.String(&n.InternalType, dialect.MYSQL)
			return plan.Type{Id: int32(types.T_json), Enumvalues: arrayType}, nil
		case defines.MYSQL_TYPE_GEOMETRY:
			fstr := strings.ToUpper(n.InternalType.FamilyString)
			oid := types.T_geometry
			srid := uint32(0)
			sridDefined := false
			if n.InternalType.GeoMetadata != nil {
				srid = n.InternalType.GeoMetadata.SRID
				sridDefined = n.InternalType.GeoMetadata.SRIDDefined
				if n.InternalType.GeoMetadata.Float32 {
					oid = types.T_geometry32
				}
			}
			if sridDefined {
				if err := validateGeometrySRID(int64(srid)); err != nil {
					return plan.Type{}, err
				}
			}
			typ := plan.Type{Id: int32(oid)}
			typ.Scale = int32(geometrySubtypeEnum(fstr))
			typ.Width = encodeGeometrySRIDWidth(srid, sridDefined)
			return typ, nil
		case defines.MYSQL_TYPE_UUID:
			return plan.Type{Id: int32(types.T_uuid)}, nil
		case defines.MYSQL_TYPE_TINY_BLOB:
			return plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_MEDIUM_BLOB:
			return plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_LONG_BLOB:
			return plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_ENUM:
			if len(n.InternalType.EnumValues) > types.MaxEnumLen {
				return plan.Type{}, moerr.NewNYI(ctx, "enum type out of max length")
			}
			if len(n.InternalType.EnumValues) == 0 {
				return plan.Type{}, moerr.NewNYI(ctx, "enum type length err")
			}

			return plan.Type{Id: int32(types.T_enum), Enumvalues: strings.Join(n.InternalType.EnumValues, ",")}, nil
		case defines.MYSQL_TYPE_SET:
			setValues, err := types.NormalizeSetValues(n.InternalType.EnumValues)
			if err != nil {
				return plan.Type{}, err
			}

			return plan.Type{Id: int32(types.T_uint64), Enumvalues: strings.Join(setValues, ",")}, nil
		default:
			return plan.Type{}, moerr.NewNYIf(ctx, "data type: '%s'", tree.String(&n.InternalType, dialect.MYSQL))
		}
	}
	return plan.Type{}, moerr.NewInternalError(ctx, "unknown data type")
}

// GetTypeFromAst resolves a parser SQL type into its plan representation.
// Stored procedure variables use this to retain their declared type, including
// metadata such as DECIMAL width and scale, throughout interpretation.
func GetTypeFromAst(ctx context.Context, typ tree.ResolvableTypeReference) (plan.Type, error) {
	return getTypeFromAst(ctx, typ)
}

func applyColumnAttributesToType(ctx context.Context, colType *plan.Type, attrs []tree.ColumnAttribute) error {
	return applyDefaultAndColumnAttributesToType(ctx, colType, colType.Charset, attrs)
}

func applyDefaultAndColumnAttributesToType(
	ctx context.Context,
	colType *plan.Type,
	tableCharset uint32,
	attrs []tree.ColumnAttribute,
) error {
	isGeometry := isGeometryPlanType(colType)
	srid, sridDefined := geometrySRIDValue(colType)
	var columnCharset string
	var columnCollation string
	for _, attr := range attrs {
		switch attribute := attr.(type) {
		case *tree.AttributeVisable:
			if !attribute.Is {
				return moerr.NewNotSupported(ctx, "invisible columns")
			}
		case *tree.AttributeCharset:
			columnCharset = attribute.Charset
		case *tree.AttributeCollate:
			columnCollation = attribute.Collate
		case *tree.AttributeSRID:
			if !isGeometry {
				return moerr.NewInvalidInputf(ctx, "SRID is only supported for GEOMETRY columns")
			}
			sridAttr := attribute
			srid = sridAttr.Value
			sridDefined = true
		}
	}
	// Charset and collation clauses are independent attributes. Resolve both
	// after scanning the list so a compatible COLLATE wins regardless of
	// textual order.
	var charset uint32
	if columnCharset != "" {
		var ok bool
		charset, ok = charsetForName(columnCharset)
		if !ok {
			return moerr.NewInvalidInputf(ctx, "unsupported character set '%s'", columnCharset)
		}
	}
	var collation uint32
	if columnCollation != "" {
		var ok bool
		collation, ok = collationForName(columnCollation)
		if !ok {
			return unsupportedCollationError(ctx, columnCollation)
		}
	}
	if columnCharset != "" && columnCollation != "" &&
		!charsetAndCollationCompatible(columnCharset, columnCollation) {
		return moerr.NewInvalidInputf(ctx,
			"COLLATION '%s' is not valid for CHARACTER SET '%s'",
			columnCollation, columnCharset)
	}
	// Resolve the effective identity before applying it. In particular, a binary
	// table default converts VARCHAR to VARBINARY; applying that conversion before
	// an explicit nonbinary column override would make the override irreversible.
	switch {
	case columnCharset != "":
		applyCharsetToPlanType(colType, charset)
		if columnCollation != "" {
			applyTextCharsetToPlanType(colType, collation)
		}
	case columnCollation != "":
		applyTextCharsetToPlanType(colType, collation)
	default:
		applyTableDefaultCharsetToPlanType(colType, tableCharset)
	}
	if isGeometry {
		// Scale (subtype) is already set by getTypeFromAst; an SRID column
		// attribute only overrides the SRID, which lives in Width.
		colType.Width = encodeGeometrySRIDWidth(srid, sridDefined)
	}
	return nil
}

func applyTextCharsetToPlanType(typ *plan.Type, charset uint32) {
	switch types.T(typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
		typ.Charset = charset
	}
}

func applyCharsetToPlanType(typ *plan.Type, charset uint32) {
	if charset != uint32(types.CharsetBinary) {
		applyTextCharsetToPlanType(typ, charset)
		return
	}

	// MySQL defines CHARACTER SET binary on a nonbinary string column as the
	// corresponding binary string type, not as a _bin collation on text.
	switch types.T(typ.Id) {
	case types.T_char:
		typ.Id = int32(types.T_binary)
	case types.T_varchar:
		typ.Id = int32(types.T_varbinary)
	case types.T_text:
		typ.Id = int32(types.T_blob)
	default:
		return
	}
	typ.Charset = uint32(types.CharsetBinary)
}

func charsetForName(name string) (uint32, bool) {
	switch strings.ToLower(name) {
	case "binary":
		return uint32(types.CharsetBinary), true
	case "utf8", "utf8mb3", "utf8mb4", "latin1", "ascii":
		// MatrixOne stores text as UTF-8. Accept MySQL's single-byte charset
		// spellings for DDL compatibility and normalize them to the supported
		// general-ci text identity rather than pretending to preserve encoding.
		return uint32(types.CharsetUTF8), true
	default:
		return 0, false
	}
}

func collationForName(name string) (uint32, bool) {
	switch strings.ToLower(name) {
	case "binary":
		return uint32(types.CharsetBinary), true
	case "utf8_bin", "utf8mb3_bin", "utf8mb4_bin":
		return uint32(types.CharsetUTF8MB4Bin), true
	case "utf8_general_ci", "utf8mb3_general_ci", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci",
		"latin1_swedish_ci", "ascii_general_ci":
		// MySQL 8 uses utf8mb4_0900_ai_ci by default. Accept that exact spelling
		// as a DDL compatibility alias, but normalize it to MatrixOne's existing
		// general-ci identity instead of claiming native UCA 9.0 semantics.
		return uint32(types.CharsetUTF8), true
	default:
		// Do not silently alias other advertised UCA/0900 collations to either
		// legacy general_ci or byte ordering. Their weight and padding contracts differ.
		return 0, false
	}
}

func unsupportedCollationError(ctx context.Context, name string) error {
	// Older MatrixOne releases accepted these spellings but collapsed them to
	// general_ci or byte ordering. A dump can preserve that historical MO
	// behavior by replacing the name with the explicit supported identity below;
	// keeping the rejected spelling would falsely promise MySQL UCA semantics.
	var replacement string
	switch strings.ToLower(name) {
	case "utf8_unicode_ci", "utf8mb3_unicode_ci":
		replacement = "utf8_general_ci"
	case "utf8mb4_unicode_ci",
		"utf8mb4_de_pb_0900_ai_ci", "utf8mb4_is_0900_ai_ci", "utf8mb4_lv_0900_ai_ci":
		replacement = "utf8mb4_general_ci"
	case "utf8mb4_0900_bin":
		replacement = "utf8mb4_bin"
	}
	if replacement != "" {
		return moerr.NewInvalidInputf(ctx,
			"unsupported collation '%s'; replace it with '%s' when restoring legacy MatrixOne DDL",
			name, replacement)
	}
	return moerr.NewInvalidInputf(ctx, "unsupported collation '%s'", name)
}

func applyTableDefaultCharsetToPlanType(typ *plan.Type, charset uint32) {
	applyCharsetToPlanType(typ, charset)
}

func charsetAndCollationCompatible(charset, collation string) bool {
	charset = canonicalCharsetName(charset)
	collation = strings.ToLower(collation)
	if charset == "binary" || collation == "binary" {
		return charset == collation
	}
	if separator := strings.IndexByte(collation, '_'); separator > 0 {
		return canonicalCharsetName(collation[:separator]) == charset
	}
	return true
}

func canonicalCharsetName(name string) string {
	switch strings.ToLower(name) {
	case "utf8", "utf8mb3", "utf8mb4":
		// MatrixOne implements the accepted utf8/utf8mb3/utf8mb4 general_ci
		// and _bin spellings with the same internal collation identities.
		return "utf8mb4"
	default:
		return strings.ToLower(name)
	}
}

func tableDefaultCharset(ctx CompilerContext, options []tree.TableOption) (uint32, error) {
	tableCharset := uint32(types.CharsetUTF8)
	tableCollation := uint32(types.CharsetUTF8)
	var tableCharsetName string
	var tableCollationName string
	hasTableCollation := false
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.TableOptionCharset:
			tableCharsetName = opt.Charset
			var ok bool
			tableCharset, ok = charsetForName(opt.Charset)
			if !ok {
				return 0, moerr.NewInvalidInputf(ctx.GetContext(), "unsupported character set '%s'", opt.Charset)
			}
		case *tree.TableOptionCollate:
			tableCollationName = opt.Collate
			var ok bool
			tableCollation, ok = collationForName(opt.Collate)
			if !ok {
				return 0, unsupportedCollationError(ctx.GetContext(), opt.Collate)
			}
			hasTableCollation = true
		}
	}
	if tableCharsetName != "" && tableCollationName != "" &&
		!charsetAndCollationCompatible(tableCharsetName, tableCollationName) {
		return 0, moerr.NewInvalidInputf(ctx.GetContext(),
			"COLLATION '%s' is not valid for CHARACTER SET '%s'",
			tableCollationName, tableCharsetName)
	}
	if hasTableCollation {
		return tableCollation, nil
	}
	if tableCharsetName == "" {
		// An unqualified table inherits the effective server collation. Older
		// compiler contexts and internal callers may not expose session variables,
		// in which case the compiled-in general_ci default remains the fallback.
		value, err := ctx.ResolveVariable("collation_server", true, false)
		if err != nil {
			return tableCharset, nil
		}
		if value != nil {
			name, ok := value.(string)
			if !ok {
				return 0, moerr.NewInternalError(ctx.GetContext(), "collation_server is not a string")
			}
			if name != "" {
				if serverCollation, supported := collationForName(name); supported {
					return serverCollation, nil
				}
				return 0, unsupportedCollationError(ctx.GetContext(), name)
			}
		}
	}
	return tableCharset, nil
}

func buildDefaultExpr(col *tree.ColumnTableDef, typ plan.Type, proc *process.Process) (*plan.Default, error) {
	return buildDefaultExprWithColumns(col, typ, proc, nil)
}

// buildDefaultExprWithColumns is the scoped form of buildDefaultExpr.  The
// unscoped form remains for call sites that bind an expression which is not a
// table-row default (for example internal compatibility expressions).
func buildDefaultExprWithColumns(
	col *tree.ColumnTableDef,
	typ plan.Type,
	proc *process.Process,
	columns []*ColDef,
) (*plan.Default, error) {
	nullAbility := true
	var expr tree.Expr = nil
	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeNull); ok {
			nullAbility = s.Is
			break
		}
	}

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeDefault); ok {
			expr = s.Expr
			break
		}
	}

	originExpr := expr
	semanticExpr := unwrapParenExpr(expr)
	_, isExpressionDefault := originExpr.(*tree.ParenExpr)

	colNameOrigin := col.Name.ColNameOrigin()
	if typ.Id == int32(types.T_json) {
		if semanticExpr != nil && !isNullAstExpr(semanticExpr) && !isExpressionDefault {
			return nil, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("JSON column '%s' cannot have default value", colNameOrigin))
		}
	}
	if isGeometryPlanType(&typ) {
		if semanticExpr != nil && !isNullAstExpr(semanticExpr) {
			return nil, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("GEOMETRY column '%s' cannot have default value", colNameOrigin))
		}
	}
	if !nullAbility && isNullAstExpr(semanticExpr) {
		return nil, moerr.NewInvalidInputf(proc.Ctx, "invalid default value for column '%s'", colNameOrigin)
	}

	if expr == nil {
		return &plan.Default{
			NullAbility:  nullAbility,
			Expr:         nil,
			OriginString: "",
		}, nil
	}
	var binder *DefaultBinder
	if columns != nil {
		binder = NewDefaultBinderWithColumns(proc.Ctx, typ, columns)
	} else {
		binder = NewDefaultBinder(proc.Ctx, nil, nil, typ, nil)
	}
	planExpr, err := binder.BindExpr(semanticExpr, 0, false)
	if err != nil {
		return nil, err
	}
	if err = preservePersistedFormatCompatibility(proc.Ctx, planExpr); err != nil {
		return nil, err
	}

	if defaultFunc := planExpr.GetF(); defaultFunc != nil {
		if int(typ.Id) != int(types.T_uuid) && defaultFunc.Func.ObjName == "uuid" && !isExpressionDefault {
			return nil, moerr.NewInvalidInputf(proc.Ctx, "invalid default value for column '%s'", colNameOrigin)
		}
	}

	defaultExpr, err := makePlan2AssignmentCastExpr(proc.Ctx, planExpr, typ)
	if err != nil {
		return nil, err
	}

	// try to calculate default value, return err if fails
	newExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(defaultExpr), proc, false, true)
	if err != nil {
		return nil, mapDDLAssignmentCastError(proc.Ctx, typ, colNameOrigin, err)
	}

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(originExpr, originExpr, false)
	return &plan.Default{
		NullAbility:  nullAbility,
		Expr:         newExpr,
		OriginString: fmtCtx.String(),
	}, nil
}

func buildOnUpdate(col *tree.ColumnTableDef, typ plan.Type, proc *process.Process) (*plan.OnUpdate, error) {
	var expr tree.Expr = nil

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeOnUpdate); ok {
			expr = s.Expr
			break
		}
	}

	if expr == nil {
		return nil, nil
	}

	binder := NewDefaultBinder(proc.Ctx, nil, nil, typ, nil)
	planExpr, err := binder.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	if err = preservePersistedFormatCompatibility(proc.Ctx, planExpr); err != nil {
		return nil, err
	}

	onUpdateExpr, err := makePlan2AssignmentCastExpr(proc.Ctx, planExpr, typ)
	if err != nil {
		return nil, err
	}

	// try to calculate on update value, return err if fails
	executor, err := colexec.NewExpressionExecutor(proc, onUpdateExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	_, err = executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, mapDDLAssignmentCastError(proc.Ctx, typ, col.Name.ColNameOrigin(), err)
	}

	ret := &plan.OnUpdate{
		Expr:         onUpdateExpr,
		OriginString: tree.String(expr, dialect.MYSQL),
	}
	return ret, nil
}

// buildGeneratedExpr builds the expression for a GENERATED ALWAYS AS column.
// existingCols contains the columns defined before this generated column, used
// to resolve column references in the expression.
// getColumnNullAbility returns the nullability of a column based on its attributes.
// Returns true if the column allows NULL (default), false if NOT NULL is specified.
func getColumnNullAbility(col *tree.ColumnTableDef) bool {
	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeNull); ok {
			return s.Is
		}
	}
	return true
}

func buildGeneratedExpr(col *tree.ColumnTableDef, typ plan.Type, existingCols []*ColDef, proc *process.Process) (*plan.GeneratedCol, error) {
	var genAttr *tree.AttributeGeneratedAlways
	for _, attr := range col.Attributes {
		if ga, ok := attr.(*tree.AttributeGeneratedAlways); ok {
			genAttr = ga
			break
		}
	}
	if genAttr == nil {
		return nil, nil
	}

	colNameOrigin := col.Name.ColNameOrigin()

	// Validate: generated column cannot have DEFAULT
	for _, attr := range col.Attributes {
		if _, ok := attr.(*tree.AttributeDefault); ok {
			return nil, moerr.NewInvalidInputf(proc.Ctx, "generated column '%s' cannot have a default value", colNameOrigin)
		}
	}
	// Validate: generated column cannot have ON UPDATE
	for _, attr := range col.Attributes {
		if _, ok := attr.(*tree.AttributeOnUpdate); ok {
			return nil, moerr.NewInvalidInputf(proc.Ctx, "generated column '%s' cannot have ON UPDATE", colNameOrigin)
		}
	}
	// Validate: generated column cannot have AUTO_INCREMENT
	for _, attr := range col.Attributes {
		if _, ok := attr.(*tree.AttributeAutoIncrement); ok {
			return nil, moerr.NewInvalidInputf(proc.Ctx, "generated column '%s' cannot have AUTO_INCREMENT", colNameOrigin)
		}
	}

	// Collect column names and types from existing (non-generated or already-defined generated) columns
	colNames := make([]string, len(existingCols))
	colTypes := make([]plan.Type, len(existingCols))
	for i, c := range existingCols {
		colNames[i] = c.Name
		colTypes[i] = c.Typ
	}

	binder := NewGeneratedColBinder(proc.Ctx, colNames, colTypes)
	planExpr, err := binder.BindExpr(genAttr.Expr, 0, false)
	if err != nil {
		return nil, err
	}
	if err = preservePersistedFormatCompatibility(proc.Ctx, planExpr); err != nil {
		return nil, err
	}

	// Validate: generated column expression cannot contain non-deterministic functions
	if err := checkExprForVolatileFunc(proc.Ctx, planExpr); err != nil {
		return nil, err
	}
	if err := checkGeneratedExprReferences(proc.Ctx, planExpr, colNameOrigin, existingCols, make(map[int32]bool)); err != nil {
		return nil, err
	}

	// Persist only stable function IDs in generated-column catalog metadata.
	// DML plan construction rewrites this wrapper to cast_assign/cast_ignore
	// when the active protocol supports those functions.
	genExpr, err := makePlan2AssignmentCastExpr(proc.Ctx, planExpr, typ)
	if err != nil {
		return nil, err
	}

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(genAttr.Expr, genAttr.Expr, false)
	return &plan.GeneratedCol{
		Expr:         genExpr,
		OriginString: fmtCtx.String(),
		IsStored:     genAttr.Stored,
	}, nil
}

func mapDDLAssignmentCastError(ctx context.Context, typ plan.Type, colName string, err error) error {
	if useSqlModeStringAssignmentCast(typ) &&
		moerr.IsMoErrCode(err, moerr.ErrInternal) {
		return moerr.NewErrInvalidDefault(ctx, colName)
	}
	return err
}

// checkGeneratedExprReferences rejects variable references and auto-increment
// dependencies in generated-column expressions, including indirect references
// through earlier generated columns.
func checkGeneratedExprReferences(ctx context.Context, expr *plan.Expr, currentColName string, cols []*ColDef, visited map[int32]bool) error {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if int(e.Col.ColPos) >= len(cols) {
			return nil
		}
		refCol := cols[e.Col.ColPos]
		if refCol.Typ.AutoIncr {
			return moerr.NewInvalidInputf(ctx, "generated column '%s' cannot refer to auto-increment column", currentColName)
		}
		if refCol.GeneratedCol != nil && refCol.GeneratedCol.Expr != nil && !visited[e.Col.ColPos] {
			visited[e.Col.ColPos] = true
			return checkGeneratedExprReferences(ctx, refCol.GeneratedCol.Expr, currentColName, cols, visited)
		}
	case *plan.Expr_V:
		return moerr.NewInvalidInputf(ctx, "expression of generated column cannot refer to a variable")
	case *plan.Expr_P:
		return moerr.NewInvalidInputf(ctx, "expression of generated column cannot contain parameter marker")
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if err := checkGeneratedExprReferences(ctx, arg, currentColName, cols, visited); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if err := checkGeneratedExprReferences(ctx, item, currentColName, cols, visited); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkExprForVolatileFunc walks a plan expression tree and reports an error
// if any function call is volatile or real-time related, which is not allowed
// in generated column expressions.
func checkExprForVolatileFunc(ctx context.Context, expr *plan.Expr) error {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		ov, exists := function.GetFunctionByIdWithoutError(e.F.Func.Obj)
		if exists && (ov.CannotFold() || ov.IsRealTimeRelated()) {
			return moerr.NewInvalidInputf(ctx,
				"expression of generated column cannot refer to a non-deterministic function '%s'", e.F.Func.ObjName)
		}
		for _, arg := range e.F.Args {
			if err := checkExprForVolatileFunc(ctx, arg); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if err := checkExprForVolatileFunc(ctx, item); err != nil {
				return err
			}
		}
	case *plan.Expr_Lit, *plan.Expr_Max, *plan.Expr_Vec:
		// Leaf nodes – nothing to recurse into.
	}
	return nil
}

// validateNoForwardGenRef checks that a generated column expression does not
// reference another generated column that is defined after it in the CREATE TABLE
// statement. Forward references to base (non-generated) columns are allowed.
func validateNoForwardGenRef(ctx context.Context, expr *plan.Expr, currentIdx int, allCols []*ColDef, isGenerated []bool) error {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		refIdx := int(e.Col.ColPos)
		if refIdx > currentIdx && refIdx < len(isGenerated) && isGenerated[refIdx] {
			return moerr.NewInvalidInputf(ctx,
				"generated column '%s' cannot refer to generated column '%s' defined later",
				allCols[currentIdx].Name, allCols[refIdx].Name)
		}
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if err := validateNoForwardGenRef(ctx, arg, currentIdx, allCols, isGenerated); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if err := validateNoForwardGenRef(ctx, item, currentIdx, allCols, isGenerated); err != nil {
				return err
			}
		}
	case *plan.Expr_Lit, *plan.Expr_Max, *plan.Expr_Vec:
		// Leaf nodes – nothing to recurse into.
	}
	return nil
}

// validateDefaultColumnDependencies validates the references persisted in
// expression defaults.  Defaults are evaluated per row, so a reference to a
// generated or auto-increment column is not a stable input.  The dependency
// graph also has to be acyclic; otherwise DML expansion would recurse forever
// (and, more importantly, the schema would have no defined row value).
func validateDefaultColumnDependencies(ctx context.Context, cols []*ColDef) error {
	state := make([]uint8, len(cols)) // 0=unvisited, 1=visiting, 2=done
	var visit func(int) error
	visit = func(colIdx int) error {
		if colIdx < 0 || colIdx >= len(cols) || cols[colIdx] == nil {
			return moerr.NewInvalidInput(ctx, "default expression references an invalid column")
		}
		switch state[colIdx] {
		case 1:
			return moerr.NewInvalidInputf(ctx,
				"default expression for column '%s' has a circular dependency",
				cols[colIdx].Name)
		case 2:
			return nil
		}
		state[colIdx] = 1
		col := cols[colIdx]
		if col.Default != nil && col.Default.Expr != nil {
			for _, refIdx := range collectRefColPos(col.Default.Expr) {
				ref := int(refIdx)
				if ref < 0 || ref >= len(cols) || cols[ref] == nil {
					return moerr.NewInvalidInputf(ctx,
						"default expression for column '%s' references an invalid column position %d",
						col.Name, ref)
				}
				refCol := cols[ref]
				if ref == colIdx {
					return moerr.NewInvalidInputf(ctx,
						"default expression for column '%s' cannot refer to itself",
						col.Name)
				}
				if refCol.GeneratedCol != nil {
					return moerr.NewInvalidInputf(ctx,
						"default expression for column '%s' cannot refer to generated column '%s'",
						col.Name, refCol.Name)
				}
				if refCol.Typ.AutoIncr {
					return moerr.NewInvalidInputf(ctx,
						"default expression for column '%s' cannot refer to auto-increment column '%s'",
						col.Name, refCol.Name)
				}
				if refCol.Default != nil && refCol.Default.Expr != nil {
					if err := visit(ref); err != nil {
						return err
					}
					if ref > colIdx && isExpressionDefault(refCol.Default) {
						return moerr.NewInvalidInputf(ctx,
							"default expression for column '%s' cannot refer to column '%s' defined after it when that column has an expression default",
							col.Name, refCol.Name)
					}
				}
			}
		}
		state[colIdx] = 2
		return nil
	}

	for i, col := range cols {
		if col == nil || col.Default == nil || col.Default.Expr == nil {
			continue
		}
		if err := visit(i); err != nil {
			return err
		}
	}
	return nil
}

// isExpressionDefault distinguishes an expression default from a plain
// literal default in the persisted catalog metadata. Parentheses survive in
// OriginString when constant folding turns an expression into a literal;
// column references and non-literal roots cover expressions that remain as
// plan nodes. This lets us retain MySQL's forward-reference compatibility for
// ordinary base columns while rejecting a forward reference to another
// expression default.
func isExpressionDefault(def *plan.Default) bool {
	if def == nil {
		return false
	}
	if isGeneratedExpressionDefault(def) || len(collectRefColPos(def.Expr)) > 0 {
		return true
	}
	if def.Expr == nil {
		return false
	}
	switch def.Expr.Expr.(type) {
	case *plan.Expr_Lit:
		return false
	default:
		// Non-literal roots (for example CURRENT_TIMESTAMP) remain expressions
		// even when the source syntax did not use an outer pair of parentheses.
		return true
	}
}

// defaultExprExpander replaces row-local ColRef(0, colIdx) references with
// the expression that supplies that column for the current DML row.  It is
// deliberately memoized: a chain such as c -> b -> a is expanded once per
// column, avoiding repeated tree growth when several defaults share a base
// column.  RelPos values other than zero belong to the surrounding plan and
// must remain untouched.
type defaultExprExpander struct {
	ctx      context.Context
	resolve  func(int32) (*plan.Expr, bool)
	state    map[int32]uint8
	memo     map[int32]*plan.Expr
	nodes    int
	maxNodes int
}

// A few planner-only callers still need expression inlining (for example,
// checks that are not attached to a writable projection). Keep those callers
// fail-fast when a malformed or adversarial dependency graph would otherwise
// produce an exponential protobuf tree. DML write paths use
// appendMaterializedExprProjections and do not depend on this limit.
const maxDefaultExpansionNodes = 1 << 20

// appendMaterializedExprProjections evaluates row-local expressions in dependency
// order. A PROJECT cannot safely read a sibling expression from the same
// projection: each sibling is evaluated against the child batch. Inlining a
// dependency therefore both duplicates work and re-evaluates volatile defaults
// (for example, b DEFAULT (a) where a DEFAULT (rand())). Keep one fixed-width
// row image and add a projection boundary for each dependency level. Independent
// expressions share a boundary, so a wide schema does not pay one full-width
// projection per materialized column. Every dependent expression then reads the
// already materialized value from the preceding boundary.
//
// projection contains one expression per output position. expressions maps a
// table-column position to the raw expression supplying that column, and order
// is the deterministic table-column order in which those expressions were
// collected. materialize marks the expressions that must be evaluated after the
// initial projection (normally defaults/generated columns containing local
// references). The helper preserves projection width and positions throughout,
// so downstream PRE_INSERT and index projections need only retag their column
// references to the returned tag.
func (builder *QueryBuilder) appendMaterializedExprProjections(
	nodeCtx *BindContext,
	childID int32,
	initialTag int32,
	projection []*plan.Expr,
	colIdxToProjPos map[int32]int32,
	expressions map[int32]*plan.Expr,
	materialize map[int32]bool,
	order []int32,
) (int32, int32, error) {
	// Keep the caller's map immutable. Some callers reuse the expression map to
	// build index/update projections after this function returns.
	materialized := make(map[int32]bool, len(materialize))
	for colIdx, needsStage := range materialize {
		if needsStage {
			materialized[colIdx] = true
		}
	}

	// A generated column can depend on a volatile default without having a
	// local reference itself. For example, a DEFAULT (rand()) and g AS (a+1)
	// would otherwise inline rand() into g and evaluate it twice. Detect that
	// dependency closure here, after all raw expressions (including generated
	// expressions) are known, so every DML entry point gets the same rule.
	// Only sort and inspect the full projection when at least one raw
	// expression contains a row-local reference. Ordinary INSERTs with literal
	// values/defaults still use this helper, but do not need the dependency
	// closure or its O(width log width) work.
	needsVolatileClosure := false
	for colIdx := range colIdxToProjPos {
		if raw, ok := expressions[colIdx]; ok && raw != nil && exprHasLocalColumnRef(raw) {
			needsVolatileClosure = true
			break
		}
	}
	var projectionColumns []int32
	if needsVolatileClosure {
		projectionColumns = make([]int32, 0, len(colIdxToProjPos))
		for colIdx := range colIdxToProjPos {
			projectionColumns = append(projectionColumns, colIdx)
		}
		sort.Slice(projectionColumns, func(i, j int) bool {
			left, right := colIdxToProjPos[projectionColumns[i]], colIdxToProjPos[projectionColumns[j]]
			if left == right {
				return projectionColumns[i] < projectionColumns[j]
			}
			return left < right
		})
		for _, colIdx := range projectionColumns {
			if materialized[colIdx] {
				continue
			}
			raw, ok := expressions[colIdx]
			if !ok || raw == nil {
				continue
			}
			if !exprHasLocalColumnRef(raw) {
				continue
			}
			needsStage, err := hasVolatileLocalDependency(
				builder.GetContext(), colIdx, expressions, materialized,
			)
			if err != nil {
				return 0, 0, err
			}
			if needsStage {
				materialized[colIdx] = true
			}
		}
	}

	materializationOrder := make([]int32, 0, len(materialized))
	ordered := make(map[int32]struct{}, len(materialized))
	appendOrder := func(colIdx int32) {
		if !materialized[colIdx] {
			return
		}
		if _, exists := ordered[colIdx]; exists {
			return
		}
		ordered[colIdx] = struct{}{}
		materializationOrder = append(materializationOrder, colIdx)
	}
	for _, colIdx := range order {
		appendOrder(colIdx)
	}
	// The volatile-closure pass can add columns that the caller did not know
	// needed a stage. Include all such columns in stable projection order.
	for _, colIdx := range projectionColumns {
		appendOrder(colIdx)
	}

	initial := DeepCopyExprList(projection)
	for colIdx := range materialized {
		projPos, ok := colIdxToProjPos[colIdx]
		raw, rawOK := expressions[colIdx]
		if !ok || projPos < 0 || int(projPos) >= len(initial) || !rawOK || raw == nil {
			return 0, 0, moerr.NewInvalidInputf(builder.GetContext(),
				"expression for column position %d cannot be materialized", colIdx)
		}
		// The value is filled by a later stage. A typed NULL keeps the initial
		// projection executable while preserving the target column metadata.
		nullExpr := makePlan2NullConstExprWithType()
		nullExpr.Typ = raw.Typ
		nullExpr.Typ.NotNullable = false
		initial[projPos] = nullExpr
	}
	for pos, expr := range initial {
		if expr == nil {
			return 0, 0, moerr.NewInternalErrorf(builder.GetContext(),
				"nil expression at projection position %d", pos)
		}
	}

	currentID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: initial,
		Children:    []int32{childID},
		BindingTags: []int32{initialTag},
	}, nodeCtx)
	currentTag := initialTag
	currentProjection := initial
	// Compute the longest materialized dependency distance for each column.
	// Columns with the same distance can be evaluated against the same input
	// image: none of them may depend on another column in that group. Besides
	// reducing plan depth, this preserves the rule that every local reference
	// reads a value from an earlier projection boundary.
	state := make(map[int32]uint8, len(materialized))
	level := make(map[int32]int, len(materialized))
	var levelOf func(int32) (int, error)
	levelOf = func(colIdx int32) (int, error) {
		if !materialized[colIdx] {
			return -1, nil
		}
		switch state[colIdx] {
		case 1:
			return 0, moerr.NewInvalidInputf(builder.GetContext(),
				"expression has a circular dependency at column position %d", colIdx)
		case 2:
			return level[colIdx], nil
		}
		raw, ok := expressions[colIdx]
		if !ok || raw == nil {
			return 0, moerr.NewInvalidInputf(builder.GetContext(),
				"expression for column position %d is missing", colIdx)
		}
		state[colIdx] = 1
		columnLevel := 0
		for _, refIdx := range collectRefColPos(raw) {
			if _, mapped := colIdxToProjPos[refIdx]; !mapped {
				return 0, moerr.NewInvalidInputf(builder.GetContext(),
					"expression for column position %d references unavailable column position %d",
					colIdx, refIdx)
			}
			if !materialized[refIdx] {
				continue
			}
			refLevel, err := levelOf(refIdx)
			if err != nil {
				return 0, err
			}
			if refLevel+1 > columnLevel {
				columnLevel = refLevel + 1
			}
		}
		state[colIdx] = 2
		level[colIdx] = columnLevel
		return columnLevel, nil
	}

	maxLevel := -1
	for _, colIdx := range materializationOrder {
		columnLevel, err := levelOf(colIdx)
		if err != nil {
			return 0, 0, err
		}
		if columnLevel > maxLevel {
			maxLevel = columnLevel
		}
	}
	levels := make([][]int32, maxLevel+1)
	for _, colIdx := range materializationOrder {
		levels[level[colIdx]] = append(levels[level[colIdx]], colIdx)
	}

	for _, columns := range levels {
		stage := make([]*plan.Expr, len(currentProjection))
		for pos, expr := range currentProjection {
			stage[pos] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: currentTag,
					ColPos: int32(pos),
				}},
			}
		}
		for _, colIdx := range columns {
			raw := expressions[colIdx]
			rewritten, err := rewriteLocalRefsToProjection(builder.GetContext(), raw, currentTag, colIdxToProjPos)
			if err != nil {
				return 0, 0, err
			}
			stage[colIdxToProjPos[colIdx]] = rewritten
		}
		nextTag := builder.genNewBindTag()
		currentID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: stage,
			Children:    []int32{currentID},
			BindingTags: []int32{nextTag},
		}, nodeCtx)
		currentTag = nextTag
		currentProjection = stage
	}
	return currentID, currentTag, nil
}

// hasVolatileLocalDependency reports whether expr depends on a non-foldable
// expression through the local table-column namespace. It follows raw
// expressions rather than expanded copies, so the check is linear in the
// dependency graph and cannot recreate the exponential planner tree that the
// materialization boundary is intended to avoid.
func hasVolatileLocalDependency(
	ctx context.Context,
	colIdx int32,
	expressions map[int32]*plan.Expr,
	materialized map[int32]bool,
) (bool, error) {
	state := make(map[int32]uint8)
	memo := make(map[int32]bool)
	var visit func(int32) (bool, error)
	visit = func(current int32) (bool, error) {
		if value, ok := memo[current]; ok {
			return value, nil
		}
		if state[current] == 1 {
			return false, moerr.NewInvalidInputf(ctx,
				"expression has a circular dependency at column position %d", current)
		}
		expr, ok := expressions[current]
		if !ok || expr == nil {
			return false, moerr.NewInvalidInputf(ctx,
				"expression for column position %d references unavailable expression", current)
		}
		state[current] = 1
		if containsVolatileFunction(expr) {
			state[current] = 2
			memo[current] = true
			return true, nil
		}
		for _, refIdx := range collectRefColPos(expr) {
			if materialized[refIdx] {
				state[current] = 2
				memo[current] = true
				return true, nil
			}
			if _, exists := expressions[refIdx]; !exists {
				return false, moerr.NewInvalidInputf(ctx,
					"expression for column position %d references unavailable column position %d",
					current, refIdx)
			}
			needsStage, err := visit(refIdx)
			if err != nil {
				return false, err
			}
			if needsStage {
				state[current] = 2
				memo[current] = true
				return true, nil
			}
		}
		state[current] = 2
		memo[current] = false
		return false, nil
	}
	return visit(colIdx)
}

// rewriteLocalRefsToProjection makes a single expression read the current
// materialized row image. Outer references (RelPos != 0) are intentionally
// untouched; only the default/generated-column local namespace is remapped.
func rewriteLocalRefsToProjection(
	ctx context.Context,
	expr *plan.Expr,
	tag int32,
	colIdxToProjPos map[int32]int32,
) (*plan.Expr, error) {
	ret := DeepCopyExpr(expr)
	var rewrite func(*plan.Expr) error
	rewrite = func(node *plan.Expr) error {
		if node == nil {
			return nil
		}
		switch impl := node.Expr.(type) {
		case *plan.Expr_Col:
			if impl.Col == nil || impl.Col.RelPos != 0 {
				return nil
			}
			projPos, ok := colIdxToProjPos[impl.Col.ColPos]
			if !ok {
				return moerr.NewInvalidInputf(ctx,
					"local expression references unavailable column position %d", impl.Col.ColPos)
			}
			name := impl.Col.Name
			node.Expr = &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: tag,
				ColPos: projPos,
				Name:   name,
			}}
		case *plan.Expr_F:
			for _, arg := range impl.F.Args {
				if err := rewrite(arg); err != nil {
					return err
				}
			}
		case *plan.Expr_List:
			for _, item := range impl.List.List {
				if err := rewrite(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := rewrite(ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func newDefaultExprExpander(ctx context.Context, resolve func(int32) (*plan.Expr, bool)) *defaultExprExpander {
	return &defaultExprExpander{
		ctx:      ctx,
		resolve:  resolve,
		state:    make(map[int32]uint8),
		memo:     make(map[int32]*plan.Expr),
		maxNodes: maxDefaultExpansionNodes,
	}
}

func (e *defaultExprExpander) expandColumn(colIdx int32) (*plan.Expr, error) {
	switch e.state[colIdx] {
	case 1:
		return nil, moerr.NewInvalidInputf(e.ctx,
			"default expression has a circular dependency at column position %d", colIdx)
	case 2:
		return e.copyExpandedExpr(e.memo[colIdx])
	}
	raw, ok := e.resolve(colIdx)
	if !ok || raw == nil {
		return nil, nil
	}
	e.state[colIdx] = 1
	expanded, err := e.expandExpr(raw)
	if err != nil {
		return nil, err
	}
	e.state[colIdx] = 2
	e.memo[colIdx] = expanded
	return e.copyExpandedExpr(expanded)
}

func (e *defaultExprExpander) expandExpr(expr *plan.Expr) (*plan.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if err := e.consumeNode(); err != nil {
			return nil, err
		}
		if impl.Col == nil || impl.Col.RelPos != 0 {
			return DeepCopyExpr(expr), nil
		}
		raw, ok := e.resolve(impl.Col.ColPos)
		if !ok || raw == nil {
			return DeepCopyExpr(expr), nil
		}
		return e.expandColumn(impl.Col.ColPos)
	case *plan.Expr_F:
		if err := e.consumeNode(); err != nil {
			return nil, err
		}
		ret := &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_F{F: &plan.Function{}}}
		*ret.GetF() = *impl.F
		ret.GetF().Args = make([]*plan.Expr, len(impl.F.Args))
		ret.GetF().AggConfig = bytes.Clone(impl.F.AggConfig)
		retFunc := ret.GetF()
		for i, arg := range impl.F.Args {
			child, err := e.expandExpr(arg)
			if err != nil {
				return nil, err
			}
			retFunc.Args[i] = child
		}
		return ret, nil
	case *plan.Expr_List:
		if err := e.consumeNode(); err != nil {
			return nil, err
		}
		ret := &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{}}}
		retList := ret.GetList()
		retList.List = make([]*plan.Expr, len(impl.List.List))
		for i, item := range impl.List.List {
			child, err := e.expandExpr(item)
			if err != nil {
				return nil, err
			}
			retList.List[i] = child
		}
		return ret, nil
	default:
		if err := e.consumeTree(expr); err != nil {
			return nil, err
		}
		return DeepCopyExpr(expr), nil
	}
}

func (e *defaultExprExpander) consumeNode() error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if e.nodes >= e.maxNodes {
		return moerr.NewInvalidInput(e.ctx,
			"default expression expansion exceeds the planner limit")
	}
	e.nodes++
	return nil
}

func (e *defaultExprExpander) consumeTree(expr *plan.Expr) error {
	if expr == nil {
		return nil
	}
	if err := e.consumeNode(); err != nil {
		return err
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if err := e.consumeTree(arg); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			if err := e.consumeTree(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *defaultExprExpander) copyExpandedExpr(expr *plan.Expr) (*plan.Expr, error) {
	if err := e.consumeTree(expr); err != nil {
		return nil, err
	}
	return DeepCopyExpr(expr), nil
}

// expandDefaultExprsInProjection expands only the projection entries listed
// in defaultPositions.  The resolver maps physical table-column positions to
// the expressions currently supplying those columns (explicit values or
// other defaults).
func expandDefaultExprsInProjection(
	ctx context.Context,
	projection []*plan.Expr,
	defaultPositions []int32,
	columnExprs map[int32]*plan.Expr,
) error {
	expander := newDefaultExprExpander(ctx, func(colIdx int32) (*plan.Expr, bool) {
		expr, ok := columnExprs[colIdx]
		return expr, ok
	})
	for _, pos := range defaultPositions {
		if pos < 0 || int(pos) >= len(projection) || projection[pos] == nil {
			continue
		}
		expr, err := expander.expandExpr(projection[pos])
		if err != nil {
			return err
		}
		projection[pos] = expr
	}
	return nil
}

func expandDefaultExprWithColumnExprs(
	ctx context.Context,
	expr *plan.Expr,
	columnExprs map[int32]*plan.Expr,
) (*plan.Expr, error) {
	expander := newDefaultExprExpander(ctx, func(colIdx int32) (*plan.Expr, bool) {
		expr, ok := columnExprs[colIdx]
		return expr, ok
	})
	return expander.expandExpr(expr)
}

// expandDefaultExprsInValueScan resolves row-local default references in a
// VALUES rowset.  The rowset is column-major, so defaults are expanded one row
// at a time without constructing a second full-width row matrix.  That keeps
// the extra memory bounded by the expression dependency chain rather than by
// (number of rows * number of table columns).
func expandDefaultExprsInValueScan(
	ctx context.Context,
	tableDef *TableDef,
	inputColumns []string,
	rowsetData *plan.RowsetData,
) error {
	if tableDef == nil || rowsetData == nil || len(inputColumns) == 0 {
		return nil
	}
	inputToTable := make(map[int]int, len(inputColumns))
	tableToInput := make(map[int32]int32, len(inputColumns))
	for inputPos, name := range inputColumns {
		tablePos, ok := tableDef.Name2ColIndex[name]
		if !ok {
			for i, col := range tableDef.Cols {
				if col != nil && strings.EqualFold(col.Name, name) {
					tablePos = int32(i)
					ok = true
					break
				}
			}
		}
		if !ok || tablePos < 0 || int(tablePos) >= len(tableDef.Cols) {
			return moerr.NewInvalidInputf(ctx, "insert column '%s' does not exist", name)
		}
		inputToTable[inputPos] = int(tablePos)
		if _, exists := tableToInput[tablePos]; exists {
			return moerr.NewInvalidInputf(ctx, "insert column '%s' is specified more than once", name)
		}
		tableToInput[tablePos] = int32(inputPos)
	}
	rowCount := int(rowsetData.RowCount)
	for row := 0; row < rowCount; row++ {
		// Most VALUES statements contain only constants or parameters.  Check
		// the current row before allocating the dependency map; local
		// references are only produced by DEFAULT (and by REPLACE's
		// DEFAULT-like binder).
		needsExpansion := false
		for inputPos := range inputToTable {
			if inputPos >= len(rowsetData.Cols) || rowsetData.Cols[inputPos] == nil ||
				row >= len(rowsetData.Cols[inputPos].Data) || rowsetData.Cols[inputPos].Data[row] == nil ||
				rowsetData.Cols[inputPos].Data[row].Expr == nil {
				return moerr.NewInvalidInputf(ctx, "invalid VALUES rowset at row %d", row+1)
			}
			if exprHasLocalColumnRef(rowsetData.Cols[inputPos].Data[row].Expr) {
				needsExpansion = true
			}
		}
		if !needsExpansion {
			continue
		}

		raw := make(map[int32]*plan.Expr, len(inputToTable))
		for inputPos, tablePos := range inputToTable {
			raw[int32(tablePos)] = rowsetData.Cols[inputPos].Data[row].Expr
		}

		defaultExprs := make(map[int32]*plan.Expr, len(tableDef.Cols))
		var resolveErr error
		expander := newDefaultExprExpander(ctx, func(colIdx int32) (*plan.Expr, bool) {
			// Values supplied by this row already have an executable vector
			// position. Keep their local reference intact so the VALUE_SCAN
			// operator can read the materialized source value instead of replaying
			// a volatile expression (a DEFAULT (a) chain is the canonical case).
			if _, ok := raw[colIdx]; ok {
				return nil, false
			}
			if colIdx < 0 || int(colIdx) >= len(tableDef.Cols) || tableDef.Cols[colIdx] == nil {
				return nil, false
			}
			if expr, ok := defaultExprs[colIdx]; ok {
				return expr, true
			}
			expr, err := getDefaultExpr(ctx, tableDef.Cols[colIdx])
			if err != nil {
				resolveErr = err
				return nil, false
			}
			defaultExprs[colIdx] = expr
			return expr, true
		})

		for inputPos, tablePos := range inputToTable {
			rowExpr := raw[int32(tablePos)]
			if !exprHasLocalColumnRef(rowExpr) {
				continue
			}
			expr, err := expander.expandExpr(rowExpr)
			if err != nil {
				return err
			}
			if resolveErr != nil {
				return resolveErr
			}
			if err := remapValueScanLocalRefs(ctx, expr, tableToInput); err != nil {
				return err
			}
			rowsetData.Cols[inputPos].Data[row].Expr = expr
		}
	}
	return nil
}

// valueScanColumnsWithDefaultDependencies returns the input columns plus any
// target-table columns required by row-local expressions in the VALUES rowset.
// VALUE_SCAN owns the row image used to evaluate those expressions.  Keeping
// an omitted dependency in that image is important for volatile defaults:
//
//	a DEFAULT (rand()), b DEFAULT (a), INSERT INTO t(b) VALUES (DEFAULT)
//
// must evaluate rand() once and let both stored columns observe that value.
// The closure follows persisted default expressions transitively and emits
// newly discovered columns in deterministic first-reference order.
func valueScanColumnsWithDefaultDependencies(
	ctx context.Context,
	tableDef *TableDef,
	inputColumns []string,
	rowsetData *plan.RowsetData,
) ([]string, error) {
	if tableDef == nil {
		return append([]string(nil), inputColumns...), nil
	}
	columns := append([]string(nil), inputColumns...)
	tableToInput := make(map[int32]struct{}, len(columns))
	for _, name := range columns {
		pos, ok := tableDef.Name2ColIndex[name]
		if !ok {
			for i, col := range tableDef.Cols {
				if col != nil && strings.EqualFold(col.Name, name) {
					pos = int32(i)
					ok = true
					break
				}
			}
		}
		if !ok || pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil {
			return nil, moerr.NewInvalidInputf(ctx,
				"insert column '%s' does not exist", name)
		}
		if _, duplicate := tableToInput[pos]; duplicate {
			return nil, moerr.NewInvalidInputf(ctx,
				"insert column '%s' is specified more than once", name)
		}
		tableToInput[pos] = struct{}{}
	}

	refs := make([]int32, 0)
	if rowsetData != nil {
		for _, col := range rowsetData.Cols {
			if col == nil {
				continue
			}
			for _, row := range col.Data {
				if row != nil {
					refs = append(refs, collectRefColPos(row.Expr)...)
				}
			}
		}
	}
	for next := 0; next < len(refs); next++ {
		ref := refs[next]
		if ref < 0 || int(ref) >= len(tableDef.Cols) || tableDef.Cols[ref] == nil {
			return nil, moerr.NewInvalidInputf(ctx,
				"VALUES expression references invalid column position %d", ref)
		}
		if _, present := tableToInput[ref]; !present {
			col := tableDef.Cols[ref]
			if col.GeneratedCol != nil {
				return nil, moerr.NewInvalidInputf(ctx,
					"VALUES expression cannot depend on generated column '%s'", col.Name)
			}
			if col.Typ.AutoIncr {
				return nil, moerr.NewInvalidInputf(ctx,
					"VALUES expression cannot depend on auto-increment column '%s'", col.Name)
			}
			tableToInput[ref] = struct{}{}
			columns = append(columns, col.Name)
			// A nullable ordinary column may have no persisted Default metadata
			// (older catalogs and synthetic plans are both allowed to omit it).
			// Its implicit default is NULL; do not dereference the absent metadata
			// while discovering the dependency closure.
			if col.Default != nil {
				refs = append(refs, collectRefColPos(col.Default.GetExpr())...)
			}
		}
	}
	return columns, nil
}

func exprHasLocalColumnRef(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return impl.Col != nil && impl.Col.RelPos == 0
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if exprHasLocalColumnRef(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			if exprHasLocalColumnRef(item) {
				return true
			}
		}
	}
	return false
}

// remapValueScanLocalRefs changes table-column positions in a VALUES expression
// to positions in the synthetic VALUE_SCAN batch. INSERT permits an explicit
// column list in any order (for example, INSERT INTO t(b, a) ...), while the
// rowset vectors follow that input order. References retained for supplied
// values therefore must use the input position, not the physical table
// position. References to omitted columns are expanded away before this pass.
func remapValueScanLocalRefs(
	ctx context.Context,
	expr *plan.Expr,
	tableToInput map[int32]int32,
) error {
	if expr == nil {
		return nil
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col == nil || impl.Col.RelPos != 0 {
			return nil
		}
		inputPos, ok := tableToInput[impl.Col.ColPos]
		if !ok {
			return moerr.NewInvalidInputf(ctx,
				"VALUES expression references unavailable column position %d", impl.Col.ColPos)
		}
		impl.Col.ColPos = inputPos
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if err := remapValueScanLocalRefs(ctx, arg, tableToInput); err != nil {
				return err
			}
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			if err := remapValueScanLocalRefs(ctx, item, tableToInput); err != nil {
				return err
			}
		}
	}
	return nil
}

// remapGeneratedColExpr rewrites ColRef positions in a generated column expression
// for use in INSERT/UPDATE projections. The stored expression has ColRef(0, colIdx)
// inlineGeneratedColExpr replaces ColRef(0, colIdx) in a generated column expression
// with a deep copy of the corresponding expression from projList1.
// colIdxToProjPos maps tableDef column index → projList1 position.
// This is used in INSERT to compute the generated value in projList1 directly.
func inlineGeneratedColExpr(expr *plan.Expr, colIdxToProjPos map[int32]int32, projList1 []*plan.Expr) {
	if expr == nil {
		return
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col.RelPos == 0 {
			if projPos, ok := colIdxToProjPos[e.Col.ColPos]; ok {
				if int(projPos) < len(projList1) {
					src := DeepCopyExpr(projList1[projPos])
					expr.Expr = src.Expr
					expr.Typ = src.Typ
				}
			}
		}
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			inlineGeneratedColExpr(arg, colIdxToProjPos, projList1)
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			inlineGeneratedColExpr(item, colIdxToProjPos, projList1)
		}
	}
}

// remapCTASColumnExprsToTableOrder normalizes expressions from the two
// independent CTAS input schemas. Explicit target definitions are bound
// against declaration order; inherited defaults on SELECT-only columns retain
// the SELECT output order. Treating every final column as if it came from the
// explicit declaration list can silently corrupt an inherited source default
// when target-only columns are prepended.
func remapCTASColumnExprsToTableOrder(
	tableCols, declarationCols, sourceCols []*ColDef,
) {
	if len(tableCols) == 0 {
		return
	}

	explicitNames := make(map[string]struct{}, len(declarationCols))
	for _, col := range declarationCols {
		if col != nil {
			explicitNames[strings.ToLower(col.Name)] = struct{}{}
		}
	}

	explicitOwners := make([]*ColDef, 0, len(declarationCols))
	sourceOwners := make([]*ColDef, 0, len(sourceCols))
	for _, col := range tableCols {
		if col == nil {
			continue
		}
		if _, ok := explicitNames[strings.ToLower(col.Name)]; ok {
			explicitOwners = append(explicitOwners, col)
		} else {
			sourceOwners = append(sourceOwners, col)
		}
	}

	remapColumnExprsToTableOrder(explicitOwners, declarationCols, tableCols)
	remapColumnExprsToTableOrder(sourceOwners, sourceCols, tableCols)
}

// remapColumnExprsToTableOrder remaps expressions owned by tableCols. The
// origin list supplies the coordinates used when those expressions were
// bound, while finalTableCols supplies the coordinates persisted in the
// resulting table. DEFAULT and generated expressions share the same row-local
// ColRef representation and therefore must be remapped together.
func remapColumnExprsToTableOrder(
	tableCols, originCols, finalTableCols []*ColDef,
) {
	if len(tableCols) == 0 || len(originCols) == 0 || len(finalTableCols) == 0 {
		return
	}

	finalPosByName := make(map[string]int, len(finalTableCols))
	for pos, col := range finalTableCols {
		if col != nil {
			finalPosByName[strings.ToLower(col.Name)] = pos
		}
	}
	originToFinal := make(map[int32]int32, len(originCols))
	for originPos, col := range originCols {
		if col == nil {
			continue
		}
		if finalPos, ok := finalPosByName[strings.ToLower(col.Name)]; ok {
			originToFinal[int32(originPos)] = int32(finalPos)
		}
	}
	remapColumnExprsByPosition(tableCols, originToFinal)
}

// remapColumnExprsByPosition applies a complete coordinate map to every
// row-local expression attached to the supplied columns.
func remapColumnExprsByPosition(cols []*ColDef, positions map[int32]int32) {
	if len(cols) == 0 || len(positions) == 0 {
		return
	}
	for _, col := range cols {
		if col == nil {
			continue
		}
		if col.Default != nil && col.Default.Expr != nil {
			remapGeneratedColExprPositions(col.Default.Expr, positions)
		}
		if col.GeneratedCol != nil && col.GeneratedCol.Expr != nil {
			remapGeneratedColExprPositions(col.GeneratedCol.Expr, positions)
		}
	}
}

func remapGeneratedColExprPositions(expr *plan.Expr, positions map[int32]int32) {
	if expr == nil {
		return
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col != nil && e.Col.RelPos == 0 {
			if pos, ok := positions[e.Col.ColPos]; ok {
				e.Col.ColPos = pos
			}
		}
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			remapGeneratedColExprPositions(arg, positions)
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			remapGeneratedColExprPositions(item, positions)
		}
	}
}

// applyGeneratedColumnAssignmentCast upgrades persisted legacy cast_strict
// wrappers to cast_assign and uses cast_ignore for INSERT/UPDATE IGNORE. This
// keeps generated-column assignment semantics compatible across catalog
// versions without rewriting catalog rows.
func (builder *QueryBuilder) applyGeneratedColumnAssignmentCast(expr *plan.Expr, isIgnore bool) *plan.Expr {
	if expr == nil {
		return expr
	}
	f := expr.GetF()
	if f == nil || f.Func == nil ||
		(f.Func.ObjName != "cast_assign" && f.Func.ObjName != "cast_strict") ||
		len(f.Args) == 0 {
		return expr
	}
	funcName := assignmentCastFunctionName(expr.Typ, isIgnore, builder.compCtx.GetProcess())
	assignmentCast, err := forceAssignmentCastExprWithName(builder.GetContext(), f.Args[0], expr.Typ, funcName)
	if err != nil {
		return expr
	}
	return assignmentCast
}

// substituteColRefsInExpr replaces ColRef(0, colIdx) in a generated column expression
// with the actual expressions from projList at offset+colIdx. This is used in UPDATE
// to inline referenced column values into the generated expression.
func substituteColRefsInExpr(expr *plan.Expr, projList []*plan.Expr, offset int32) *plan.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col.RelPos == 0 {
			pos := offset + e.Col.ColPos
			if int(pos) < len(projList) {
				return DeepCopyExpr(projList[pos])
			}
		}
		return expr
	case *plan.Expr_F:
		newArgs := make([]*plan.Expr, len(e.F.Args))
		for i, arg := range e.F.Args {
			newArgs[i] = substituteColRefsInExpr(arg, projList, offset)
		}
		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func:          e.F.Func,
					Args:          newArgs,
					AggConfig:     bytes.Clone(e.F.AggConfig),
					AggConfigType: e.F.AggConfigType,
				},
			},
		}
	case *plan.Expr_List:
		newItems := make([]*plan.Expr, len(e.List.List))
		for i, item := range e.List.List {
			newItems[i] = substituteColRefsInExpr(item, projList, offset)
		}
		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_List{
				List: &plan.ExprList{List: newItems},
			},
		}
	default:
		return expr
	}
}

// collectRefColPos returns the ColPos of every base-table column reference
// (RelPos == 0) inside expr, e.g. the source columns of a generated column's
// definition expression.
func collectRefColPos(expr *plan.Expr) []int32 {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col != nil && e.Col.RelPos == 0 {
			return []int32{e.Col.ColPos}
		}
		return nil
	case *plan.Expr_F:
		var res []int32
		for _, arg := range e.F.Args {
			res = append(res, collectRefColPos(arg)...)
		}
		return res
	case *plan.Expr_List:
		var res []int32
		for _, item := range e.List.List {
			res = append(res, collectRefColPos(item)...)
		}
		return res
	default:
		return nil
	}
}

func isNullExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch ef := expr.Expr.(type) {
	case *plan.Expr_Lit:
		return expr.Typ.Id == int32(types.T_any) && ef.Lit.Isnull
	default:
		return false
	}
}

func isNullAstExpr(expr tree.Expr) bool {
	if expr == nil {
		return false
	}
	v, ok := expr.(*tree.NumVal)
	return ok && v.ValType == tree.P_null
}

func convertValueIntoBool(name string, args []*Expr, isLogic bool) error {
	if !isLogic && (len(args) != 2 || (args[0].Typ.Id != int32(types.T_bool) && args[1].Typ.Id != int32(types.T_bool))) {
		return nil
	}
	for _, arg := range args {
		if arg.Typ.Id == int32(types.T_bool) {
			continue
		}
		switch ex := arg.Expr.(type) {
		case *plan.Expr_Lit:
			switch value := ex.Lit.Value.(type) {
			case *plan.Literal_I64Val:
				if value.I64Val == 0 {
					ex.Lit.Value = &plan.Literal_Bval{Bval: false}
				} else {
					ex.Lit.Value = &plan.Literal_Bval{Bval: true}
				}
				arg.Typ.Id = int32(types.T_bool)
			}
		}
	}
	return nil
}

func getFunctionObjRef(funcID int64, name string) *ObjectRef {
	return &ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}

// getAccountIds transforms the account names into account ids.
// if accounts is nil, return the id of the sys account.
// func getAccountIds(ctx CompilerContext, accounts tree.IdentifierList) ([]uint32, error) {
// 	var accountIds []uint32
// 	var err error
// 	if len(accounts) != 0 {
// 		accountNames := make([]string, len(accounts))
// 		for i, account := range accounts {
// 			accountNames[i] = string(account)
// 		}
// 		accountIds, err = ctx.ResolveAccountIds(accountNames)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		accountIds = []uint32{catalog.System_Account}
// 	}
// 	if len(accountIds) == 0 {
// 		return nil, moerr.NewInternalError(ctx.GetContext(), "need specify account for the cluster tables")
// 	}
// 	return accountIds, err
// }

// func getAccountInfoOfClusterTable(ctx CompilerContext, accounts tree.IdentifierList, tableDef *TableDef, isClusterTable bool) (*plan.ClusterTable, error) {
// 	var accountIds []uint32
// 	var columnIndexOfAccountId int32 = -1
// 	var err error
// 	if isClusterTable {
// 		accountIds, err = getAccountIds(ctx, accounts)
// 		if err != nil {
// 			return nil, err
// 		}
// 		for i, col := range tableDef.GetCols() {
// 			if util.IsClusterTableAttribute(col.Name) {
// 				if columnIndexOfAccountId >= 0 {
// 					return nil, moerr.NewInternalError(ctx.GetContext(), "there are two account_ids in the cluster table")
// 				} else {
// 					columnIndexOfAccountId = int32(i)
// 				}
// 			}
// 		}

// 		if columnIndexOfAccountId == -1 {
// 			return nil, moerr.NewInternalError(ctx.GetContext(), "there is no account_id in the cluster table")
// 		} else if columnIndexOfAccountId >= int32(len(tableDef.GetCols())) {
// 			return nil, moerr.NewInternalError(ctx.GetContext(), "the index of the account_id in the cluster table is invalid")
// 		}
// 	} else {
// 		if len(accounts) != 0 {
// 			return nil, moerr.NewInvalidInput(ctx.GetContext(), "can not specify the accounts for the non cluster table")
// 		}
// 	}
// 	return &plan.ClusterTable{
// 		IsClusterTable:         isClusterTable,
// 		AccountIDs:             accountIds,
// 		ColumnIndexOfAccountId: columnIndexOfAccountId,
// 	}, nil
// }

func getDefaultExpr(ctx context.Context, d *plan.ColDef) (*Expr, error) {
	if d == nil {
		return nil, moerr.NewInvalidInput(ctx, "cannot resolve default value for a missing column definition")
	}
	if d.Default == nil {
		if d.Typ.NotNullable && !d.Typ.AutoIncr {
			return nil, moerr.NewInvalidInputf(ctx, "invalid default value for column '%s'", d.Name)
		}
		typ := d.Typ
		typ.NotNullable = false
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{Isnull: true},
			},
			Typ: typ,
		}, nil
	}
	if !d.Default.NullAbility && d.Default.Expr == nil && !d.Typ.AutoIncr {
		return nil, moerr.NewInvalidInputf(ctx, "invalid default value for column '%s'", d.Name)
	}
	if d.Default.Expr == nil {
		typ := d.Typ
		typ.NotNullable = false
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: true,
				},
			},
			Typ: typ,
		}, nil
	}
	newDefExpr := DeepCopyExpr(d.Default.Expr)
	err := replaceFuncId(ctx, newDefExpr)
	return newDefExpr, err
}

func replaceFuncId(ctx context.Context, expr *Expr) error {
	switch fun := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range fun.F.Args {
			err := replaceFuncId(ctx, arg)
			if err != nil {
				return err
			}
		}

		fnName := fun.F.Func.ObjName
		newFID, err := function.GetFunctionIdByName(ctx, fnName)
		if err != nil {
			return err
		}
		oldFID, oldIdx := function.DecodeOverloadID(fun.F.Func.Obj)
		if oldFID != newFID {
			fun.F.Func.Obj = function.EncodeOverloadID(newFID, oldIdx)
		}
	default:
	}
	return nil
}

func judgeUnixTimestampReturnType(timestr string) types.T {
	retDecimal := 0
	if dotIdx := strings.LastIndex(timestr, "."); dotIdx >= 0 {
		retDecimal = len(timestr) - dotIdx - 1
	}

	if retDecimal > 6 || retDecimal == -1 {
		retDecimal = 6
	}

	if retDecimal == 0 {
		return types.T_int64
	} else {
		return types.T_decimal128
	}
}

// Get the primary key name of the table
func getTablePriKeyName(priKeyDef *plan.PrimaryKeyDef) string {
	if priKeyDef == nil {
		return ""
	} else {
		return priKeyDef.PkeyColName
	}
}

// Check whether the table column name is an internal key
func checkTableColumnNameValid(name string) bool {
	if name == catalog.Row_ID || name == catalog.CPrimaryKeyColName ||
		name == catalog.TableTailAttrDeleteRowID || name == catalog.TableTailAttrAborted ||
		name == catalog.TableTailAttrPKVal || name == catalog.TableTailAttrCommitTs ||
		catalog.IsAlias(name) {
		return false
	}
	return true
}

// Check the expr has paramExpr
func checkExprHasParamExpr(exprs []tree.Expr) bool {
	for _, expr := range exprs {
		if _, ok := expr.(*tree.ParamExpr); ok {
			return true
		} else if e, ok := expr.(*tree.FuncExpr); ok {
			return checkExprHasParamExpr(e.Exprs)
		}
	}
	return false
}

// makeSelectList forms SELECT Clause "Select t.a,t.b,... "
func makeSelectList(table string, strs []string) string {
	bb := strings.Builder{}
	for i, str := range strs {
		if i > 0 {
			bb.WriteByte(',')
		}
		//table
		bb.WriteByte('`')
		bb.WriteString(table)
		bb.WriteByte('`')
		bb.WriteByte('.')
		//column
		bb.WriteByte('`')
		bb.WriteString(str)
		bb.WriteByte('`')
	}
	return bb.String()
}

// makeWhere forms WHERE Clause "Where t.a is not null and ..."
func makeWhere(table string, strs []string) string {
	bb := strings.Builder{}
	for i, str := range strs {
		if i > 0 {
			bb.WriteString(" and ")
		}
		//table
		bb.WriteByte('`')
		bb.WriteString(table)
		bb.WriteByte('`')
		bb.WriteByte('.')
		//column
		bb.WriteByte('`')
		bb.WriteString(str)
		bb.WriteByte('`')
		//is not null
		bb.WriteString(" is not null")
	}
	bb.WriteByte(' ')
	return bb.String()
}

// colIdsToNames convert the colId to the col name
func colIdsToNames(ctx context.Context, colIds []uint64, colDefs []*plan.ColDef) ([]string, error) {
	colId2Name := make(map[uint64]string)
	for _, def := range colDefs {
		colId2Name[def.ColId] = def.Name
	}
	names := make([]string, 0)
	for _, colId := range colIds {
		if name, has := colId2Name[colId]; !has {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("colId %d does exist", colId))
		} else {
			names = append(names, name)
		}
	}
	return names, nil
}

/*
genSqlForCheckFKConstraints generates the fk constraint checking sql.

basic logic of fk constraint check.

	parent table:
		T(a)
	child table:
		S(b)
		foreign key (b) references T(a)


	generated sql :
		select count(*) == 0 from (
			select distinct S.b from S where S.b is not null
			except
			select distinct T.a from T
		) as __mo_fk_check_source
	if the result is true, then the fk constraint confirmed.
*/
func genSqlForCheckFKConstraints(ctx context.Context,
	fkey *plan.ForeignKeyDef,
	childDbName, childTblName string, colsOfChild []*plan.ColDef,
	parentDbName, parentTblName string, colsOfParent []*plan.ColDef) (string, error) {

	//fk column names
	fkCols, err := colIdsToNames(ctx, fkey.Cols, colsOfChild)
	if err != nil {
		return "", err
	}
	//referred column names
	referCols, err := colIdsToNames(ctx, fkey.ForeignCols, colsOfParent)
	if err != nil {
		return "", err
	}

	childTableClause := fmt.Sprintf("`%s`.`%s`", childDbName, childTblName)
	parentTableClause := fmt.Sprintf("`%s`.`%s`", parentDbName, parentTblName)
	where := fmt.Sprintf("where %s", makeWhere(childTblName, fkCols))
	except := fmt.Sprintf("select distinct %s from %s %s except select distinct %s from %s",
		makeSelectList(childTblName, fkCols),
		childTableClause,
		where,
		makeSelectList(parentTblName, referCols),
		parentTableClause,
	)

	//make detect sql
	sql := strings.Join([]string{
		"select count(*) = 0 from (",
		except,
		") as __mo_fk_check_source",
	}, " ")
	return sql, nil
}

// genSqlsForCheckFKSelfRefer generates the fk constraint checking sql.
// the only difference between genSqlsForCheckFKSelfRefer and genSqlForCheckFKConstraints
// is the parent table and child table are same in the fk self refer.
func genSqlsForCheckFKSelfRefer(ctx context.Context,
	dbName, tblName string,
	cols []*plan.ColDef, fkeys []*plan.ForeignKeyDef) ([]string, error) {
	ret := make([]string, 0)
	for _, fkey := range fkeys {
		if fkey.ForeignTbl != 0 {
			continue
		}
		sql, err := genSqlForCheckFKConstraints(ctx, fkey, dbName, tblName, cols, dbName, tblName, cols)
		if err != nil {
			return nil, err
		}
		ret = append(ret, sql)
	}
	return ret, nil
}

// genPreCheckSqlsForReplaceFKSelfRefer generates pre-check SQLs that verify
// no other row references the PK values being replaced (parent→child safety).
// These run BEFORE the REPLACE execution to enforce RESTRICT semantics.
func genPreCheckSqlsForReplaceFKSelfRefer(
	ctx context.Context,
	dbName, tblName string,
	cols []*plan.ColDef,
	fkeys []*plan.ForeignKeyDef,
	stmt *tree.Replace,
) ([]string, error) {
	if stmt.Rows == nil {
		return nil, nil
	}
	valuesClause, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return nil, nil
	}

	ret := make([]string, 0, len(fkeys))
	for _, fkey := range fkeys {
		if fkey.ForeignTbl != 0 {
			continue
		}
		// Only RESTRICT / NO_ACTION need a parent→child pre-check.
		// CASCADE / SET_NULL / SET_DEFAULT semantics allow the operation to
		// proceed and let the cascading action handle the children, so a
		// pre-check would incorrectly block valid REPLACEs.
		if fkey.OnDelete != plan.ForeignKeyDef_RESTRICT &&
			fkey.OnDelete != plan.ForeignKeyDef_NO_ACTION {
			continue
		}
		fkCols, err := colIdsToNames(ctx, fkey.Cols, cols)
		if err != nil {
			return nil, err
		}
		referCols, err := colIdsToNames(ctx, fkey.ForeignCols, cols)
		if err != nil {
			return nil, err
		}
		if len(referCols) != 1 || len(fkCols) != 1 {
			continue
		}

		// Build column name → position in the Replace column list.
		// Names are stored lower-cased in ColDef.Name; the user-supplied
		// AST identifiers may use any casing, so normalize both sides.
		colNameToPos := make(map[string]int)
		if len(stmt.Columns) > 0 {
			for i, col := range stmt.Columns {
				colNameToPos[strings.ToLower(string(col))] = i
			}
		} else {
			// Implicit column list: same visible-column rule as
			// getInsertColsFromStmt — skip hidden cols (e.g. composite PK
			// helper, fake PK, cluster-by composite, Row_ID), since the
			// user VALUES list never supplies them.
			pos := 0
			for _, col := range cols {
				if col.Hidden {
					continue
				}
				colNameToPos[col.Name] = pos
				pos++
			}
		}

		refPos, ok := colNameToPos[referCols[0]]
		if !ok {
			continue
		}

		// The pre-check SQL embeds referenced PK values directly into a
		// background statement. That is only semantics-preserving for
		// static literals (NumVal/StrVal, including NULL via NumVal
		// P_null). Non-literals such as prepared-statement parameters
		// (ParamExpr "?"), function calls (rand(), uuid(), now()),
		// subqueries, arithmetic, etc. would be re-evaluated when the
		// pre-check runs and may not match the value actually written
		// by REPLACE — skip pre-check generation in those cases.
		//
		// Trade-off: prepared REPLACE on RESTRICT self-ref FK tables and
		// REPLACE with non-literal PK expressions lose the parent-row
		// safety check. The full fix needs to defer pre-check generation
		// to compile time after parameters/expressions are evaluated,
		// which is a larger change left for follow-up work.
		isSimpleLiteralExpr := func(expr tree.Expr) bool {
			switch expr.(type) {
			case *tree.NumVal, *tree.StrVal:
				return true
			default:
				return false
			}
		}

		hasUnsafeRefExpr := false
		var valStrs []string
		for _, row := range valuesClause.Rows {
			if refPos >= len(row) {
				continue
			}
			if !isSimpleLiteralExpr(row[refPos]) {
				hasUnsafeRefExpr = true
				break
			}
			valStrs = append(valStrs, tree.String(row[refPos], dialect.MYSQL))
		}
		if hasUnsafeRefExpr || len(valStrs) == 0 {
			continue
		}

		inList := strings.Join(valStrs, ",")
		tableClause := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
		sql := fmt.Sprintf(
			"select count(*) = 0 from %s where `%s` in (%s) and `%s` is not null and `%s` not in (%s)",
			tableClause, fkCols[0], inList, fkCols[0], referCols[0], inList,
		)
		ret = append(ret, sql)
	}
	return ret, nil
}

func cleanHint(originSql string) string {
	re := regexp.MustCompile(`/\*[^!].*?\*/`)
	cleanSQL := re.ReplaceAllString(originSql, "")
	return cleanSQL
}

// RewriteCountNotNullColToStarcount rewrites count(not_null_col) to starcount (ObjName + Obj) on node.AggList
// so that compile uses countStarExec instead of countColumnExec. tableDef must be the child's (e.g. TABLE_SCAN).
func RewriteCountNotNullColToStarcount(node *plan.Node, tableDef *plan.TableDef) {
	if node == nil || tableDef == nil || len(node.AggList) == 0 {
		return
	}
	for i := range node.AggList {
		agg := node.AggList[i].GetF()
		if agg == nil || agg.Func == nil || agg.Func.ObjName != "count" {
			continue
		}
		if uint64(agg.Func.Obj)&function.Distinct != 0 {
			continue
		}
		if len(agg.Args) == 0 {
			continue
		}
		arg := agg.Args[0]
		col := arg.GetCol()
		if col == nil {
			continue
		}
		colPos := int(col.ColPos)
		if colPos < 0 || colPos >= len(tableDef.Cols) {
			continue
		}
		if !tableDef.Cols[colPos].Typ.NotNullable {
			continue
		}
		agg.Func.ObjName = "starcount"
		agg.Func.Obj = function.EncodeOverloadID(int32(function.STARCOUNT), 0)
	}
}
