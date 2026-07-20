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
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

// reCheckifNeedLockWholeTable checks if the whole table needs to be locked based on the last node's statistics.
// It returns true if the out count of the last node is greater than the maximum lock count, otherwise it returns false.
func reCheckifNeedLockWholeTable(builder *QueryBuilder) {
	lockService := builder.compCtx.GetProcess().Base.LockService
	if lockService == nil {
		// MockCompilerContext
		return
	}
	lockconfig := lockService.GetConfig()

	for _, n := range builder.qry.Nodes {
		if n.NodeType != plan.Node_LOCK_OP {
			continue
		}
		if !n.LockTargets[0].LockTable {
			reCheckIfNeed := n.Stats.Outcnt > float64(lockconfig.MaxLockRowCount)
			if reCheckIfNeed {
				logutil.Infof("Row lock upgraded to table lock for SQL : %s", builder.compCtx.GetRootSql())
				logutil.Infof("the outcnt stats is %f", n.Stats.Outcnt)
				for _, target := range n.LockTargets {
					target.LockTable = reCheckIfNeed
				}
			}
		}
	}
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
				} else if fstr == "vecf32" || fstr == "vecf64" {
					width = types.MaxArrayDimension
				} else {
					width = types.MaxVarcharLen
				}
			}

			if (fstr == "char" || fstr == "binary") && width > types.MaxCharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, fstr, " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if (fstr == "varchar" || fstr == "varbinary") && width > types.MaxVarcharLen {
				return plan.Type{}, moerr.NewOutOfRangef(ctx, fstr, " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			} else if fstr == "vecf32" || fstr == "vecf64" {
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
			case "vecf32":
				return plan.Type{Id: int32(types.T_array_float32), Width: width}, nil
			case "vecf64":
				return plan.Type{Id: int32(types.T_array_float64), Width: width}, nil
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

func applyColumnAttributesToType(ctx context.Context, colType *plan.Type, attrs []tree.ColumnAttribute) error {
	if !isGeometryPlanType(colType) {
		for _, attr := range attrs {
			if _, ok := attr.(*tree.AttributeSRID); ok {
				return moerr.NewInvalidInputf(ctx, "SRID is only supported for GEOMETRY columns")
			}
		}
		return nil
	}
	// Scale (subtype) is already set by getTypeFromAst; an SRID column attribute
	// only overrides the SRID, which lives in Width.
	srid, sridDefined := geometrySRIDValue(colType)
	for _, attr := range attrs {
		if sridAttr, ok := attr.(*tree.AttributeSRID); ok {
			srid = sridAttr.Value
			sridDefined = true
		}
	}
	colType.Width = encodeGeometrySRIDWidth(srid, sridDefined)
	return nil
}

func buildDefaultExpr(col *tree.ColumnTableDef, typ plan.Type, proc *process.Process) (*plan.Default, error) {
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

	colNameOrigin := col.Name.ColNameOrigin()
	if typ.Id == int32(types.T_json) {
		if semanticExpr != nil && !isNullAstExpr(semanticExpr) {
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
	_, isExpressionDefault := originExpr.(*tree.ParenExpr)

	binder := NewDefaultBinder(proc.Ctx, nil, nil, typ, nil)
	planExpr, err := binder.BindExpr(semanticExpr, 0, false)
	if err != nil {
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
		return nil, err
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
		return nil, err
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

	// Validate: generated column expression cannot contain non-deterministic functions
	if err := checkExprForVolatileFunc(proc.Ctx, planExpr); err != nil {
		return nil, err
	}
	if err := checkGeneratedExprReferences(proc.Ctx, planExpr, colNameOrigin, existingCols, make(map[int32]bool)); err != nil {
		return nil, err
	}

	// A generated CHAR/VARCHAR column is materialized as a real column write, so
	// use the strict assignment cast: an over-length value is rejected instead of
	// being silently truncated, matching column DEFAULT / ON UPDATE and the DML
	// assignment paths.
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
					Func: e.F.Func,
					Args: newArgs,
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
		if e.Col.RelPos == 0 {
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
	if !d.Default.NullAbility && d.Default.Expr == nil && !d.Typ.AutoIncr {
		return nil, moerr.NewInvalidInputf(ctx, "invalid default value for column '%s'", d.Name)
	}
	if d.Default.Expr == nil {
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: true,
				},
			},
			Typ: plan.Type{
				Id:          d.Typ.Id,
				NotNullable: false,
			},
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
		name == catalog.TableTailAttrPKVal || name == catalog.TableTailAttrCommitTs {
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
		)
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
		")",
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

func genParentSideReplaceFKSqls(
	ctx CompilerContext,
	parentRef *plan.ObjectRef,
	parent *plan.TableDef,
	stmt *tree.Replace,
) (string, []string, []string, error) {
	if stmt.Rows == nil || len(parent.RefChildTbls) == 0 {
		return "", nil, nil, nil
	}
	hasNonSelfReference := false
	for _, childID := range parent.RefChildTbls {
		if childID != 0 {
			hasNonSelfReference = true
			break
		}
	}
	if !hasNonSelfReference {
		return "", nil, nil, nil
	}
	values, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return "", nil, nil, moerr.NewNotSupported(ctx.GetContext(), "REPLACE SELECT/TABLE on a referenced parent table")
	}
	positions := make(map[string]int)
	if len(stmt.Columns) > 0 {
		pos := 0
		for _, col := range stmt.Columns {
			name := strings.ToLower(string(col))
			if colIdx, found := parent.Name2ColIndex[name]; found &&
				int(colIdx) < len(parent.Cols) && parent.Cols[colIdx].GeneratedCol != nil {
				continue
			}
			positions[name] = pos
			pos++
		}
	} else {
		pos := 0
		for _, col := range parent.Cols {
			if !col.Hidden && col.GeneratedCol == nil {
				positions[strings.ToLower(col.Name)] = pos
				pos++
			}
		}
	}
	quoteIdentifier := func(name string) string {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	qualifiedCol := func(alias, name string) string {
		return quoteIdentifier(alias) + "." + quoteIdentifier(name)
	}
	findParentCol := func(name string) (*plan.ColDef, bool) {
		if pos, found := parent.Name2ColIndex[name]; found && int(pos) < len(parent.Cols) {
			return parent.Cols[pos], true
		}
		for _, col := range parent.Cols {
			if col.Name == name {
				return col, true
			}
		}
		return nil, false
	}
	treatAutoIncrementZeroAsGenerated := true
	if sqlMode, err := ctx.ResolveVariable("sql_mode", true, false); err != nil {
		return "", nil, nil, err
	} else if mode, ok := sqlMode.(string); ok {
		treatAutoIncrementZeroAsGenerated = !strings.Contains(strings.ToUpper(mode), "NO_AUTO_VALUE_ON_ZERO")
	}
	isAssignmentConvertedZero := func(expr tree.Expr, col *plan.ColDef) (bool, error) {
		var value *tree.NumVal
		switch input := expr.(type) {
		case *tree.NumVal:
			value = input
		case *tree.StrVal:
			value = tree.NewNumVal(input.String(), input.String(), false, tree.P_char)
		default:
			return false, nil
		}
		if value.ValType == tree.P_null || value.ValType == tree.P_bool {
			return false, nil
		}
		proc := ctx.GetProcess()
		if proc == nil {
			return false, moerr.NewInternalError(ctx.GetContext(),
				"cannot materialize auto-increment value without a process")
		}
		converted, err := MakeInsertValueConstExpr(proc, value, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
		if err != nil {
			return false, err
		}
		if converted == nil {
			binder := NewDefaultBinder(ctx.GetContext(), nil, nil, plan.Type{}, nil)
			converted, err = binder.BindExpr(expr, 0, true)
			if err != nil {
				return false, err
			}
			converted, err = forceAssignmentCastExpr(ctx.GetContext(), converted, col.Typ)
			if err != nil {
				return false, err
			}
			converted, err = ConstantFold(batch.EmptyForConstFoldBatch, converted, proc, false, true)
			if err != nil {
				return false, err
			}
		}
		lit := converted.GetLit()
		if lit == nil {
			return false, nil
		}
		switch val := lit.Value.(type) {
		case *plan.Literal_I8Val:
			return val.I8Val == 0, nil
		case *plan.Literal_I16Val:
			return val.I16Val == 0, nil
		case *plan.Literal_I32Val:
			return val.I32Val == 0, nil
		case *plan.Literal_I64Val:
			return val.I64Val == 0, nil
		case *plan.Literal_U8Val:
			return val.U8Val == 0, nil
		case *plan.Literal_U16Val:
			return val.U16Val == 0, nil
		case *plan.Literal_U32Val:
			return val.U32Val == 0, nil
		case *plan.Literal_U64Val:
			return val.U64Val == 0, nil
		default:
			return false, nil
		}
	}

	type uniqueKey struct {
		parts         []string
		prefixLengths map[string]int
	}
	uniqueKeys := make([]uniqueKey, 0, 1+len(parent.Indexes))
	if parent.Pkey != nil && len(parent.Pkey.Names) > 0 {
		uniqueKeys = append(uniqueKeys, uniqueKey{parts: parent.Pkey.Names})
	}
	for _, idx := range parent.Indexes {
		if !idx.Unique {
			continue
		}
		parts := make([]string, len(idx.Parts))
		for i, part := range idx.Parts {
			parts[i] = catalog.ResolveAlias(part)
		}
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
		if err != nil {
			return "", nil, nil, err
		}
		uniqueKeys = append(uniqueKeys, uniqueKey{parts: parts, prefixLengths: prefixLengths})
	}

	const parentAlias = "__mo_replace_parent"
	literalFmt := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	formatStringLiteral := func(value string) string {
		tree.NewNumVal(value, value, false, tree.P_char).Format(literalFmt)
		formatted := literalFmt.String()
		literalFmt.Reset()
		return formatted
	}
	formatLiteral := func(expr *plan.Expr) (string, bool, bool) {
		lit := expr.GetLit()
		if lit == nil {
			return "", false, false
		}
		if lit.Isnull {
			return "", true, true
		}
		switch val := lit.Value.(type) {
		case *plan.Literal_I8Val:
			return strconv.FormatInt(int64(val.I8Val), 10), false, true
		case *plan.Literal_I16Val:
			return strconv.FormatInt(int64(val.I16Val), 10), false, true
		case *plan.Literal_I32Val:
			return strconv.FormatInt(int64(val.I32Val), 10), false, true
		case *plan.Literal_I64Val:
			return strconv.FormatInt(val.I64Val, 10), false, true
		case *plan.Literal_U8Val:
			return strconv.FormatUint(uint64(val.U8Val), 10), false, true
		case *plan.Literal_U16Val:
			return strconv.FormatUint(uint64(val.U16Val), 10), false, true
		case *plan.Literal_U32Val:
			return strconv.FormatUint(uint64(val.U32Val), 10), false, true
		case *plan.Literal_U64Val:
			return strconv.FormatUint(val.U64Val, 10), false, true
		case *plan.Literal_Fval:
			return strconv.FormatFloat(float64(val.Fval), 'g', -1, 32), false, true
		case *plan.Literal_Dval:
			return strconv.FormatFloat(val.Dval, 'g', -1, 64), false, true
		case *plan.Literal_Bval:
			return strconv.FormatBool(val.Bval), false, true
		case *plan.Literal_EnumVal:
			return strconv.FormatUint(uint64(val.EnumVal), 10), false, true
		case *plan.Literal_Decimal64Val:
			return types.Decimal64(val.Decimal64Val.A).Format(expr.Typ.Scale), false, true
		case *plan.Literal_Decimal128Val:
			decimal := types.Decimal128{
				B0_63:   uint64(val.Decimal128Val.A),
				B64_127: uint64(val.Decimal128Val.B),
			}
			return decimal.Format(expr.Typ.Scale), false, true
		case *plan.Literal_Dateval:
			return formatStringLiteral(types.Date(val.Dateval).String()), false, true
		case *plan.Literal_Timeval:
			return formatStringLiteral(types.Time(val.Timeval).String2(expr.Typ.Scale)), false, true
		case *plan.Literal_Datetimeval:
			return formatStringLiteral(types.Datetime(val.Datetimeval).String2(expr.Typ.Scale)), false, true
		case *plan.Literal_Timestampval:
			location := time.UTC
			if proc := ctx.GetProcess(); proc != nil && proc.GetSessionInfo().TimeZone != nil {
				location = proc.GetSessionInfo().TimeZone
			}
			value := types.Timestamp(val.Timestampval).String2(location, expr.Typ.Scale)
			return formatStringLiteral(value), false, true
		case *plan.Literal_Sval:
			return formatStringLiteral(val.Sval), false, true
		default:
			return "", false, false
		}
	}
	materializeDefault := func(colName string) (string, bool, error) {
		col, found := findParentCol(colName)
		if !found {
			return "", false, moerr.NewInternalErrorf(ctx.GetContext(),
				"REPLACE conflict column %s not found", colName)
		}
		if col.GeneratedCol != nil {
			return "", false, moerr.NewNotSupported(ctx.GetContext(),
				"REPLACE with an omitted generated conflict key")
		}
		defaultExpr, err := getDefaultExpr(ctx.GetContext(), col)
		if err != nil {
			return "", false, err
		}
		formatted, isNull, ok := formatLiteral(defaultExpr)
		if !ok {
			return "", false, moerr.NewNotSupported(ctx.GetContext(),
				"REPLACE with a non-literal default conflict key")
		}
		return formatted, isNull, nil
	}
	sqlTypeForColumn := func(col *plan.ColDef) string {
		typ := types.T(col.Typ.Id)
		switch typ {
		case types.T_time, types.T_datetime, types.T_timestamp:
			if col.Typ.Scale > 0 {
				return fmt.Sprintf("%s(%d)", makeTypeByPlan2Type(col.Typ).String(), col.Typ.Scale)
			}
		case types.T_enum:
			values := strings.Split(col.Typ.Enumvalues, ",")
			for i := range values {
				values[i] = formatStringLiteral(values[i])
			}
			return "ENUM(" + strings.Join(values, ",") + ")"
		}
		if isSetPlanType(&col.Typ) {
			values := strings.Split(col.Typ.Enumvalues, ",")
			for i := range values {
				values[i] = formatStringLiteral(values[i])
			}
			return "SET(" + strings.Join(values, ",") + ")"
		}
		return makeTypeByPlan2Type(col.Typ).DescString()
	}
	castForColumn := func(value string, col *plan.ColDef) string {
		return fmt.Sprintf("cast(%s as %s)", value, sqlTypeForColumn(col))
	}
	formatInputLiteral := func(expr tree.Expr, col *plan.ColDef) (string, error) {
		switch value := expr.(type) {
		case *tree.NumVal:
			if value.ValType == tree.P_char &&
				(types.T(col.Typ.Id) == types.T_char || types.T(col.Typ.Id) == types.T_varchar) &&
				col.Typ.Width > 0 && int32(utf8.RuneCountInString(value.String())) > col.Typ.Width {
				return "", moerr.NewInvalidInputf(ctx.GetContext(),
					"Src length %d is larger than Dest length %d",
					utf8.RuneCountInString(value.String()), col.Typ.Width)
			}
			expr.Format(literalFmt)
		case *tree.StrVal:
			if (types.T(col.Typ.Id) == types.T_char || types.T(col.Typ.Id) == types.T_varchar) &&
				col.Typ.Width > 0 && int32(utf8.RuneCountInString(value.String())) > col.Typ.Width {
				return "", moerr.NewInvalidInputf(ctx.GetContext(),
					"Src length %d is larger than Dest length %d",
					utf8.RuneCountInString(value.String()), col.Typ.Width)
			}
			expr.Format(literalFmt)
		default:
			return "", moerr.NewNotSupported(ctx.GetContext(), "REPLACE with a non-literal conflict key")
		}
		formatted := literalFmt.String()
		literalFmt.Reset()
		return formatted, nil
	}
	var materializeInputColumn func(tree.Exprs, int, map[int]bool) (string, error)
	materializeInputColumn = func(row tree.Exprs, colIdx int, visiting map[int]bool) (string, error) {
		if colIdx < 0 || colIdx >= len(parent.Cols) {
			return "", moerr.NewInternalErrorf(ctx.GetContext(), "REPLACE conflict column position %d not found", colIdx)
		}
		col := parent.Cols[colIdx]
		if col.GeneratedCol != nil {
			if visiting[colIdx] {
				return "", moerr.NewInternalErrorf(ctx.GetContext(),
					"cyclic generated column dependency at %s", col.Name)
			}
			visiting[colIdx] = true
			defer delete(visiting, colIdx)
			if strings.TrimSpace(col.GeneratedCol.OriginString) == "" {
				return "", moerr.NewNotSupportedf(ctx.GetContext(),
					"REPLACE with generated conflict key %s lacking its source expression", col.Name)
			}
			refs := collectRefColPos(col.GeneratedCol.Expr)
			slices.Sort(refs)
			refs = slices.Compact(refs)
			selectParts := make([]string, 0, len(refs))
			for _, refPos := range refs {
				refExpr, err := materializeInputColumn(row, int(refPos), visiting)
				if err != nil {
					return "", err
				}
				selectParts = append(selectParts, fmt.Sprintf("%s as %s", refExpr,
					quoteIdentifier(parent.Cols[refPos].Name)))
			}
			generatedExpr := castForColumn(col.GeneratedCol.OriginString, col)
			if len(selectParts) == 0 {
				return "(select " + generatedExpr + ")", nil
			}
			return fmt.Sprintf("(select %s from (select %s) as %s)", generatedExpr,
				strings.Join(selectParts, ", "), quoteIdentifier("__mo_replace_input")), nil
		}

		pos, supplied := positions[strings.ToLower(col.Name)]
		if !supplied || pos >= len(row) {
			if col.Typ.AutoIncr {
				return "null", nil
			}
			value, isNull, err := materializeDefault(col.Name)
			if err != nil {
				return "", err
			}
			if isNull {
				return "null", nil
			}
			return castForColumn(value, col), nil
		}
		if _, ok := row[pos].(*tree.DefaultVal); ok {
			value, isNull, err := materializeDefault(col.Name)
			if err != nil {
				return "", err
			}
			if isNull {
				return "null", nil
			}
			return castForColumn(value, col), nil
		}
		if col.Typ.AutoIncr && treatAutoIncrementZeroAsGenerated {
			zero, err := isAssignmentConvertedZero(row[pos], col)
			if err != nil {
				return "", err
			}
			if zero {
				return "null", nil
			}
		}
		value, err := formatInputLiteral(row[pos], col)
		if err != nil {
			return "", err
		}
		return castForColumn(value, col), nil
	}
	conflictPredicates := make([]string, 0, len(values.Rows)*len(uniqueKeys))
	for _, row := range values.Rows {
		for _, key := range uniqueKeys {
			parts := make([]string, 0, len(key.parts))
			keyCannotConflict := false
			for _, colName := range key.parts {
				col, found := findParentCol(colName)
				if !found {
					return "", nil, nil, moerr.NewInternalErrorf(ctx.GetContext(),
						"REPLACE conflict column %s not found", colName)
				}
				pos, supplied := positions[strings.ToLower(colName)]
				var incomingExpr string
				if !supplied || pos >= len(row) {
					if col.Typ.AutoIncr {
						keyCannotConflict = true
						break
					}
					var isNull bool
					var err error
					if col.GeneratedCol != nil {
						colIdx := -1
						for i, candidate := range parent.Cols {
							if strings.EqualFold(candidate.Name, colName) {
								colIdx = i
								break
							}
						}
						incomingExpr, err = materializeInputColumn(row, colIdx, make(map[int]bool))
					} else {
						incomingExpr, isNull, err = materializeDefault(colName)
					}
					if err != nil {
						return "", nil, nil, err
					}
					if isNull {
						keyCannotConflict = true
						break
					}
				} else {
					zero := false
					if col.Typ.AutoIncr && treatAutoIncrementZeroAsGenerated {
						var zeroErr error
						zero, zeroErr = isAssignmentConvertedZero(row[pos], col)
						if zeroErr != nil {
							return "", nil, nil, zeroErr
						}
					}
					if zero {
						// PRE_INSERT turns an explicit numeric zero into NULL and
						// allocates a fresh auto-increment value unless
						// NO_AUTO_VALUE_ON_ZERO is enabled. Such a value cannot
						// identify an old parent row during this pre-phase.
						keyCannotConflict = true
						break
					}
					switch value := row[pos].(type) {
					case *tree.NumVal:
						if value.ValType == tree.P_char &&
							(types.T(col.Typ.Id) == types.T_char || types.T(col.Typ.Id) == types.T_varchar) &&
							col.Typ.Width > 0 && int32(utf8.RuneCountInString(value.String())) > col.Typ.Width {
							return "", nil, nil, moerr.NewInvalidInputf(ctx.GetContext(),
								"Src length %d is larger than Dest length %d",
								utf8.RuneCountInString(value.String()), col.Typ.Width)
						}
						row[pos].Format(literalFmt)
						incomingExpr = literalFmt.String()
						literalFmt.Reset()
					case *tree.StrVal:
						if (types.T(col.Typ.Id) == types.T_char || types.T(col.Typ.Id) == types.T_varchar) &&
							col.Typ.Width > 0 && int32(utf8.RuneCountInString(value.String())) > col.Typ.Width {
							return "", nil, nil, moerr.NewInvalidInputf(ctx.GetContext(),
								"Src length %d is larger than Dest length %d",
								utf8.RuneCountInString(value.String()), col.Typ.Width)
						}
						row[pos].Format(literalFmt)
						incomingExpr = literalFmt.String()
						literalFmt.Reset()
					case *tree.DefaultVal:
						var isNull bool
						var err error
						incomingExpr, isNull, err = materializeDefault(colName)
						if err != nil {
							return "", nil, nil, err
						}
						if isNull {
							keyCannotConflict = true
							break
						}
					default:
						return "", nil, nil, moerr.NewNotSupported(ctx.GetContext(), "REPLACE with a non-literal conflict key")
					}
				}
				if keyCannotConflict {
					break
				}
				incomingExpr = castForColumn(incomingExpr, col)
				parentExpr := qualifiedCol(parentAlias, colName)
				if length := key.prefixLengths[colName]; length > 0 {
					parentExpr = fmt.Sprintf("substring(%s, 1, %d)", parentExpr, length)
					incomingExpr = fmt.Sprintf("substring(%s, 1, %d)", incomingExpr, length)
				}
				parts = append(parts, fmt.Sprintf("%s = %s", parentExpr, incomingExpr))
			}
			if keyCannotConflict {
				continue
			}
			conflictPredicates = append(conflictPredicates, "("+strings.Join(parts, " and ")+")")
		}
	}
	if len(conflictPredicates) == 0 {
		return "", nil, nil, nil
	}
	conflictPredicate := "(" + strings.Join(conflictPredicates, " or ") + ")"
	parentTable := quoteIdentifier(parentRef.SchemaName) + "." + quoteIdentifier(parent.Name)

	var checks, actions []string
	referencedIndexes := make(map[string]*plan.IndexDef)
	partsEqual := func(parts, names []string) bool {
		if len(parts) != len(names) {
			return false
		}
		for i := range parts {
			if catalog.ResolveAlias(parts[i]) != names[i] {
				return false
			}
		}
		return true
	}
	pkeyNames := []string(nil)
	if parent.Pkey != nil {
		pkeyNames = parent.Pkey.Names
		if len(pkeyNames) == 0 && parent.Pkey.PkeyColName != "" {
			pkeyNames = []string{parent.Pkey.PkeyColName}
		}
	}
	seen := make(map[uint64]bool)
	for _, childID := range parent.RefChildTbls {
		if childID == 0 || seen[childID] {
			continue
		}
		seen[childID] = true
		childRef, child, err := ctx.ResolveById(childID, nil)
		if err != nil {
			return "", nil, nil, err
		}
		if child == nil {
			return "", nil, nil, moerr.NewInternalError(ctx.GetContext(), fmt.Sprintf("referencing table %d not found", childID))
		}
		for _, fk := range child.Fkeys {
			if fk.ForeignTbl != parent.TblId {
				continue
			}
			if len(fk.Cols) == 0 || len(fk.Cols) != len(fk.ForeignCols) {
				return "", nil, nil, moerr.NewInternalError(ctx.GetContext(), "invalid parent foreign key definition")
			}
			parentCols, err := colIdsToNames(ctx.GetContext(), fk.ForeignCols, parent.Cols)
			if err != nil {
				return "", nil, nil, err
			}
			if !partsEqual(pkeyNames, parentCols) {
				for _, idx := range parent.Indexes {
					if idx.Unique && partsEqual(idx.Parts, parentCols) {
						if idx.IndexTableName == "" {
							return "", nil, nil, moerr.NewInternalErrorf(ctx.GetContext(),
								"unique index %s has no index table", idx.IndexName)
						}
						referencedIndexes[idx.IndexTableName] = idx
						break
					}
				}
			}
			childCols, err := colIdsToNames(ctx.GetContext(), fk.Cols, child.Cols)
			if err != nil {
				return "", nil, nil, err
			}
			childTable := quoteIdentifier(childRef.SchemaName) + "." + quoteIdentifier(child.Name)
			joinParts := make([]string, len(childCols))
			for i := range childCols {
				joinParts[i] = fmt.Sprintf("%s.%s = %s",
					childTable, quoteIdentifier(childCols[i]), qualifiedCol(parentAlias, parentCols[i]))
			}
			exists := fmt.Sprintf("exists (select 1 from %s as %s where %s and %s)",
				parentTable, quoteIdentifier(parentAlias), conflictPredicate, strings.Join(joinParts, " and "))
			switch fk.OnDelete {
			case plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_SET_DEFAULT:
				checks = append(checks, fmt.Sprintf("select count(*) = 0 from %s where %s", childTable, exists))
			case plan.ForeignKeyDef_CASCADE:
				actions = append(actions, fmt.Sprintf("delete from %s where %s", childTable, exists))
			case plan.ForeignKeyDef_SET_NULL:
				setParts := make([]string, len(childCols))
				for i, col := range childCols {
					setParts[i] = quoteIdentifier(col) + " = null"
				}
				actions = append(actions, fmt.Sprintf("update %s set %s where %s",
					childTable, strings.Join(setParts, ", "), exists))
			}
		}
	}

	indexNames := make([]string, 0, len(referencedIndexes))
	for name := range referencedIndexes {
		indexNames = append(indexNames, name)
	}
	slices.Sort(indexNames)
	lockSelects := make([]string, 0, len(indexNames)+1)
	if parent.Pkey != nil && parent.Pkey.PkeyColName != "" {
		lockSelects = append(lockSelects, qualifiedCol(parentAlias, parent.Pkey.PkeyColName))
	} else {
		lockSelects = append(lockSelects, "1")
	}
	for i, indexName := range indexNames {
		idx := referencedIndexes[indexName]
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
		if err != nil {
			return "", nil, nil, err
		}
		keyParts := make([]string, len(idx.Parts))
		for partPos, part := range idx.Parts {
			colName := catalog.ResolveAlias(part)
			col, found := findParentCol(colName)
			if !found {
				return "", nil, nil, moerr.NewInternalErrorf(ctx.GetContext(),
					"REPLACE referenced index column %s not found", colName)
			}
			partExpr := qualifiedCol(parentAlias, colName)
			if length := prefixLengths[colName]; length > 0 {
				partExpr = fmt.Sprintf("substring(%s, 1, %d)", partExpr, length)
				if prefixType, ok := indexTableKeyTypeForPrefix(col.Typ); ok {
					partExpr = fmt.Sprintf("cast(%s as %s)", partExpr, makeTypeByPlan2Type(prefixType).DescString())
				}
			}
			keyParts[partPos] = partExpr
		}
		keyExpr := keyParts[0]
		if indexTableStoresSerializedKey(idx) {
			keyExpr = "serial(" + strings.Join(keyParts, ", ") + ")"
		}
		indexAlias := fmt.Sprintf("__mo_replace_fk_idx_%d", i)
		indexTable := quoteIdentifier(parentRef.SchemaName) + "." + quoteIdentifier(indexName)
		indexKey := qualifiedCol(indexAlias, catalog.IndexTableIndexColName)
		lockSelects = append(lockSelects, fmt.Sprintf(
			"(select %s from %s as %s where %s = %s for update)",
			indexKey, indexTable, quoteIdentifier(indexAlias), indexKey, keyExpr))
	}
	parentLock := fmt.Sprintf("select %s from %s as %s where %s for update",
		strings.Join(lockSelects, ", "), parentTable, quoteIdentifier(parentAlias), conflictPredicate)
	return parentLock, checks, actions, nil
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
