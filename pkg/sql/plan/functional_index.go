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
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const functionalIndexProtocolError = "functional indexes require all CNs to support protocol version 57"

type functionalIndexExprKind uint8

const (
	functionalIndexExprInvalid functionalIndexExprKind = iota
	functionalIndexExprInteger
	functionalIndexExprString
	functionalIndexExprJSON
)

type functionalIndexExprVisit uint8

const (
	functionalIndexExprUnvisited functionalIndexExprVisit = iota
	functionalIndexExprVisiting
	functionalIndexExprDone
)

type functionalIndexExprValue struct {
	kind functionalIndexExprKind
	typ  Type
}

func hasFunctionalIndexKeyPart(parts []*tree.KeyPart) bool {
	for _, part := range parts {
		if part != nil && part.Expr != nil {
			return true
		}
	}
	return false
}

// requireFunctionalIndexProtocol protects the persisted hidden-column/index
// contract during a rolling upgrade. A nil process is used by parser/planner
// unit tests and has no deployment to gate.
func requireFunctionalIndexProtocol(ctx context.Context, proc *process.Process) error {
	if proc == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(ctx, functionalIndexProtocolError)
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version := int64(0)
	switch v := value.(type) {
	case int64:
		version = v
	case int:
		version = int64(v)
	case uint64:
		version = int64(v)
	default:
		ok = false
	}
	if !ok || version < defines.MORPCVersion57 {
		return moerr.NewNotSupported(ctx, functionalIndexProtocolError)
	}
	return nil
}

// validateFunctionalIndexExpression is the single semantic admission check for
// persisted functional-index expressions. It intentionally reasons over the
// resolved plan expression rather than the SQL spelling: implicit casts,
// overload selection, assignment wrappers, and generated-column dependencies
// are all part of the value that writers and readers will execute.
func validateFunctionalIndexExpression(ctx context.Context, expr *plan.Expr, tableDef *TableDef) error {
	if expr == nil || tableDef == nil {
		return moerr.NewNotSupported(ctx, "functional index expression is not resolvable")
	}
	_, err := validateFunctionalIndexExpressionValue(ctx, expr, tableDef)
	return err
}

func validateFunctionalIndexExpressionValue(
	ctx context.Context,
	expr *plan.Expr,
	tableDef *TableDef,
) (functionalIndexExprValue, error) {
	if expr == nil || tableDef == nil {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression is not resolvable")
	}
	states := make(map[int32]functionalIndexExprVisit)
	values := make(map[int32]functionalIndexExprValue)
	return validateFunctionalIndexExprNode(ctx, expr, tableDef, states, values)
}

func validateFunctionalIndexExprNode(
	ctx context.Context,
	expr *plan.Expr,
	tableDef *TableDef,
	states map[int32]functionalIndexExprVisit,
	values map[int32]functionalIndexExprValue,
) (functionalIndexExprValue, error) {
	if expr == nil {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression contains a nil node")
	}

	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(tableDef.Cols) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression references an invalid column")
		}
		pos := e.Col.ColPos
		col := tableDef.Cols[pos]
		if col == nil {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression references a missing column")
		}
		if col.GeneratedCol != nil {
			return validateFunctionalIndexGeneratedColumn(ctx, pos, col, tableDef, states, values)
		}
		if col.Typ.AutoIncr {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression cannot depend on an auto-increment column")
		}
		value := functionalIndexValueForType(col.Typ)
		if value.kind == functionalIndexExprInvalid {
			return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx, "functional index expression references unsupported column type %s", types.T(col.Typ.Id).String())
		}
		return value, nil

	case *plan.Expr_Lit:
		value := functionalIndexValueForType(expr.Typ)
		if value.kind == functionalIndexExprInvalid {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression contains an unsupported literal type")
		}
		if value.kind == functionalIndexExprString && (e.Lit == nil || e.Lit.Isnull) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression contains an untyped string literal")
		}
		return value, nil

	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression contains an unresolved function")
		}
		fid, overload, ok := functionalIndexKnownOverload(e.F.Func.Obj)
		if !ok {
			return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx, "functional index expression uses an unknown overload for '%s'", e.F.Func.ObjName)
		}
		return validateFunctionalIndexFunction(ctx, fid, overload, e.F, expr, tableDef, states, values)

	default:
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression contains an unsupported plan node")
	}
}

func validateFunctionalIndexGeneratedColumn(
	ctx context.Context,
	pos int32,
	col *ColDef,
	tableDef *TableDef,
	states map[int32]functionalIndexExprVisit,
	values map[int32]functionalIndexExprValue,
) (functionalIndexExprValue, error) {
	switch states[pos] {
	case functionalIndexExprVisiting:
		return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx, "functional index expression has a cyclic generated-column dependency at '%s'", col.Name)
	case functionalIndexExprDone:
		return values[pos], nil
	}
	if col.GeneratedCol == nil || col.GeneratedCol.Expr == nil {
		return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx, "functional index expression depends on generated column '%s' without an expression", col.Name)
	}
	states[pos] = functionalIndexExprVisiting
	value, err := validateFunctionalIndexExprNode(ctx, col.GeneratedCol.Expr, tableDef, states, values)
	if err != nil {
		return functionalIndexExprValue{}, err
	}
	value, err = validateFunctionalIndexAssignmentTarget(ctx, value, col.Typ)
	if err != nil {
		return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx,
			"functional index expression depends on generated column '%s': %s", col.Name, err)
	}
	states[pos] = functionalIndexExprDone
	values[pos] = value
	return value, nil
}

func functionalIndexKnownOverload(overloadID int64) (fid, overload int32, ok bool) {
	fid, overload = function.DecodeOverloadID(overloadID)
	if fid < 0 || overload < 0 {
		return fid, overload, false
	}
	// Catalog metadata is not trusted input. The function package historically
	// assumes a valid overload index and can panic for a malformed one, so keep
	// the admission check total and fail closed at this boundary.
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	_, ok = function.GetFunctionByIdWithoutError(overloadID)
	return fid, overload, ok
}

func functionalIndexValueForType(typ Type) functionalIndexExprValue {
	switch types.T(typ.Id) {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return functionalIndexExprValue{kind: functionalIndexExprInteger, typ: typ}
	case types.T_varchar:
		if functionalIndexFixedVarchar(typ) {
			return functionalIndexExprValue{kind: functionalIndexExprString, typ: typ}
		}
	case types.T_json:
		return functionalIndexExprValue{kind: functionalIndexExprJSON, typ: typ}
	}
	return functionalIndexExprValue{kind: functionalIndexExprInvalid, typ: typ}
}

func functionalIndexFixedVarchar(typ Type) bool {
	if typ.Id != int32(types.T_varchar) || typ.Width <= 0 || typ.Width > types.MaxVarcharLen || typ.PadSpace {
		return false
	}
	switch typ.Charset {
	case uint32(types.CharsetUTF8), uint32(types.CharsetUTF8MB4Bin):
		return true
	default:
		return false
	}
}

func functionalIndexJSONPathLiteral(expr *plan.Expr) bool {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return false
	}
	return types.T(expr.Typ.Id).IsMySQLString() && expr.Typ.Id != int32(types.T_char) &&
		expr.Typ.Id != int32(types.T_binary) && expr.Typ.Id != int32(types.T_varbinary)
}

func functionalIndexTypesEqual(a, b Type) bool {
	return a.Id == b.Id && a.Width == b.Width && a.Scale == b.Scale &&
		a.Charset == b.Charset && a.PadSpace == b.PadSpace
}

func functionalIndexIntegerBits(oid types.T) int {
	switch oid {
	case types.T_int8, types.T_uint8:
		return 8
	case types.T_int16, types.T_uint16:
		return 16
	case types.T_int32, types.T_uint32:
		return 32
	case types.T_int64, types.T_uint64:
		return 64
	default:
		return 0
	}
}

func functionalIndexIntegerWidening(source, target Type) bool {
	if !types.T(source.Id).IsInteger() || !types.T(target.Id).IsInteger() {
		return false
	}
	if source.Id == target.Id {
		return true
	}
	sourceUnsigned := types.T(source.Id).IsUnsignedInt()
	targetUnsigned := types.T(target.Id).IsUnsignedInt()
	sourceBits, targetBits := functionalIndexIntegerBits(types.T(source.Id)), functionalIndexIntegerBits(types.T(target.Id))
	if sourceBits == 0 || targetBits == 0 {
		return false
	}
	if sourceUnsigned == targetUnsigned {
		return targetBits >= sourceBits
	}
	// An unsigned value can widen to a signed type only when the destination is
	// strictly wider.  The opposite direction can change the representable
	// range and is therefore never an admission-safe cast.
	return sourceUnsigned && !targetUnsigned && targetBits > sourceBits
}

// validateFunctionalIndexAssignmentTarget checks the conversion that the
// generated-column declaration applies after evaluating its expression.  A
// dependency is only safe when this boundary cannot truncate a value or turn
// an error into a session-mode-dependent warning.
func validateFunctionalIndexAssignmentTarget(
	ctx context.Context,
	source functionalIndexExprValue,
	target Type,
) (functionalIndexExprValue, error) {
	targetValue := functionalIndexValueForType(target)
	if targetValue.kind == functionalIndexExprInvalid {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "generated-column declaration has an unsupported type")
	}
	if functionalIndexTypesEqual(source.typ, target) {
		return targetValue, nil
	}
	switch targetValue.kind {
	case functionalIndexExprInteger:
		if source.kind == functionalIndexExprInteger && functionalIndexIntegerWidening(source.typ, target) {
			return targetValue, nil
		}
	case functionalIndexExprString:
		if source.kind == functionalIndexExprString && functionalIndexFixedVarchar(source.typ) &&
			functionalIndexFixedVarchar(target) && source.typ.Charset == target.Charset &&
			target.Width >= source.typ.Width {
			return targetValue, nil
		}
	}
	return functionalIndexExprValue{}, moerr.NewNotSupported(ctx,
		"generated-column declaration changes value or assignment error semantics")
}

func validateFunctionalIndexFunction(
	ctx context.Context,
	fid int32,
	overload int32,
	fn *plan.Function,
	expr *plan.Expr,
	tableDef *TableDef,
	states map[int32]functionalIndexExprVisit,
	values map[int32]functionalIndexExprValue,
) (functionalIndexExprValue, error) {
	args := fn.Args
	child := func(index int) (functionalIndexExprValue, error) {
		if index < 0 || index >= len(args) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index expression has invalid function arguments")
		}
		return validateFunctionalIndexExprNode(ctx, args[index], tableDef, states, values)
	}

	switch fid {
	case function.PLUS:
		if overload != 0 || len(args) != 2 {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index integer addition requires two arguments")
		}
		left, err := child(0)
		if err != nil {
			return functionalIndexExprValue{}, err
		}
		right, err := child(1)
		if err != nil {
			return functionalIndexExprValue{}, err
		}
		if left.kind != functionalIndexExprInteger || right.kind != functionalIndexExprInteger || !types.T(expr.Typ.Id).IsInteger() {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index integer addition requires integer operands")
		}
		return functionalIndexExprValue{kind: functionalIndexExprInteger, typ: expr.Typ}, nil

	case function.LOWER:
		if overload != 0 || len(args) != 1 {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index lower requires one argument")
		}
		arg, err := child(0)
		if err != nil {
			return functionalIndexExprValue{}, err
		}
		if arg.kind != functionalIndexExprString || !functionalIndexFixedVarchar(arg.typ) || !functionalIndexFixedVarchar(expr.Typ) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index lower requires fixed UTF8MB4 VARCHAR semantics")
		}
		return functionalIndexExprValue{kind: functionalIndexExprString, typ: expr.Typ}, nil

	case function.JSON_EXTRACT:
		if overload != 0 || len(args) != 2 || args[0] == nil || args[0].Typ.Id != int32(types.T_json) || !functionalIndexJSONPathLiteral(args[1]) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index JSON extraction requires one constant path")
		}
		arg, err := child(0)
		if err != nil {
			return functionalIndexExprValue{}, err
		}
		if arg.kind != functionalIndexExprJSON || expr.Typ.Id != int32(types.T_json) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index JSON extraction requires a JSON value")
		}
		return functionalIndexExprValue{kind: functionalIndexExprJSON, typ: expr.Typ}, nil

	case function.JSON_UNQUOTE:
		if overload != 0 || len(args) != 1 {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index JSON_UNQUOTE requires one argument")
		}
		arg, err := child(0)
		if err != nil {
			return functionalIndexExprValue{}, err
		}
		argFn := args[0].GetF()
		argFID := int32(-1)
		if argFn != nil && argFn.Func != nil {
			argFID, _, _ = functionalIndexKnownOverload(argFn.Func.Obj)
		}
		if arg.kind != functionalIndexExprJSON || argFID != function.JSON_EXTRACT || !functionalIndexFixedVarchar(expr.Typ) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index JSON_UNQUOTE requires a constant-path JSON extraction")
		}
		return functionalIndexExprValue{kind: functionalIndexExprString, typ: expr.Typ}, nil

	case function.CAST, function.CAST_STRICT, function.CAST_ASSIGN, function.CAST_IGNORE:
		if fid == function.CAST && overload > 1 {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast overload is not session-invariant")
		}
		if fid != function.CAST && overload != 0 {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast overload is not session-invariant")
		}
		return validateFunctionalIndexCast(ctx, fid, fn, expr, tableDef, states, values)

	default:
		return functionalIndexExprValue{}, moerr.NewNotSupportedf(ctx, "functional index function '%s' is not in the session-invariant allowlist", fn.Func.ObjName)
	}
}

func validateFunctionalIndexCast(
	ctx context.Context,
	fid int32,
	fn *plan.Function,
	expr *plan.Expr,
	tableDef *TableDef,
	states map[int32]functionalIndexExprVisit,
	values map[int32]functionalIndexExprValue,
) (functionalIndexExprValue, error) {
	if len(fn.Args) != 2 || fn.Args[1] == nil || fn.Args[1].GetT() == nil {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast has no fixed target type")
	}
	source, err := validateFunctionalIndexExprNode(ctx, fn.Args[0], tableDef, states, values)
	if err != nil {
		return functionalIndexExprValue{}, err
	}
	target := fn.Args[1].Typ
	targetValue := functionalIndexValueForType(target)
	if targetValue.kind == functionalIndexExprInvalid {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast targets an unsupported type")
	}
	switch targetValue.kind {
	case functionalIndexExprInteger:
		if source.kind != functionalIndexExprInteger || !functionalIndexIntegerWidening(source.typ, target) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast is not an integer widening conversion")
		}
	case functionalIndexExprString:
		if source.kind != functionalIndexExprString || !functionalIndexFixedVarchar(target) {
			return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast requires fixed UTF8MB4 VARCHAR semantics")
		}
		isAssignmentWrapper := fid != function.CAST
		if !functionalIndexTypesEqual(source.typ, target) {
			if isAssignmentWrapper {
				if target.Charset != source.typ.Charset || target.PadSpace || target.Width < source.typ.Width {
					return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index assignment cast may depend on session width or collation semantics")
				}
			} else if !functionalIndexJSONUnquoteExpr(fn.Args[0]) && target.Charset != source.typ.Charset {
				return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast changes string collation semantics")
			}
		}
	default:
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast result is not session-invariant")
	}
	if !functionalIndexTypesEqual(expr.Typ, target) {
		return functionalIndexExprValue{}, moerr.NewNotSupported(ctx, "functional index cast result metadata is inconsistent")
	}
	return targetValue, nil
}

func functionalIndexJSONUnquoteExpr(expr *plan.Expr) bool {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return false
	}
	fid, _, ok := functionalIndexKnownOverload(expr.GetF().Func.Obj)
	return ok && fid == function.JSON_UNQUOTE
}

func functionalIndexColumnName(indexName string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(indexName))))
	return catalog.FunctionalIndexColumnPrefix + hex.EncodeToString(sum[:])[:32]
}

func cloneFunctionalIndex(indexInfo *tree.Index) *tree.Index {
	if indexInfo == nil {
		return nil
	}
	clone := *indexInfo
	clone.KeyParts = make([]*tree.KeyPart, len(indexInfo.KeyParts))
	for i, part := range indexInfo.KeyParts {
		if part == nil {
			continue
		}
		partClone := *part
		clone.KeyParts[i] = &partClone
	}
	return &clone
}

func functionalIndexUsesRegularBTREE(indexInfo *tree.Index) bool {
	if indexInfo == nil {
		return false
	}
	if indexInfo.KeyType != tree.INDEX_TYPE_INVALID && indexInfo.KeyType != tree.INDEX_TYPE_BTREE {
		return false
	}
	return indexInfo.IndexOption == nil || indexInfo.IndexOption.IType == tree.INDEX_TYPE_INVALID || indexInfo.IndexOption.IType == tree.INDEX_TYPE_BTREE
}

// lowerFunctionalIndex turns one supported expression key part into a hidden
// virtual generated column. The returned AST index is planner-owned; the input
// AST is intentionally left untouched because rel_createsql must retain the
// user's expression rather than the reserved internal column name.
func lowerFunctionalIndex(ctx CompilerContext, indexInfo *tree.Index, tableDef *TableDef) (*tree.Index, error) {
	if indexInfo == nil || tableDef == nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "functional index metadata is nil")
	}
	hasExpr := false
	for _, part := range indexInfo.KeyParts {
		if part != nil && part.Expr != nil {
			hasExpr = true
			break
		}
	}
	if !hasExpr {
		return indexInfo, nil
	}
	if tableDef.IsTemporary || tableDef.TableType == catalog.SystemTemporaryTable {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes are not supported on temporary tables")
	}
	if util.TableIsClusterTable(tableDef.GetTableType()) {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes are not supported on cluster tables")
	}
	if tableDef.TableType == catalog.SystemExternalRel {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes are not supported on external tables")
	}
	if !functionalIndexUsesRegularBTREE(indexInfo) {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes only support regular BTREE indexes")
	}
	if len(indexInfo.KeyParts) != 1 || indexInfo.KeyParts[0] == nil || indexInfo.KeyParts[0].Expr == nil {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes require exactly one expression key part")
	}
	part := indexInfo.KeyParts[0]
	if part.ColName != nil {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional index key part cannot mix a column and an expression")
	}
	if part.Length != 0 || part.Direction != tree.DefaultDirection {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes do not support prefix or ASC/DESC key parts")
	}
	indexName := indexInfo.Name
	if indexName == "" {
		// Inline CREATE TABLE permits an unnamed secondary index. The regular
		// constraint-name pass runs after functional lowering, so give this
		// index the same stable seed that setEmptyIndexName uses; duplicate
		// names are still rejected by that pass.
		indexName = "functional_index"
	}
	if err := requireFunctionalIndexProtocol(ctx.GetContext(), ctx.GetProcess()); err != nil {
		return nil, err
	}

	colNames := make([]string, len(tableDef.Cols))
	colTypes := make([]plan.Type, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		if col == nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "functional index references nil table column")
		}
		colNames[i] = col.Name
		colTypes[i] = col.Typ
	}
	binder := NewGeneratedColBinder(ctx.GetContext(), colNames, colTypes)
	bound, err := binder.BindExpr(part.Expr, 0, false)
	if err != nil {
		return nil, err
	}
	if err = checkExprForVolatileFunc(ctx.GetContext(), bound); err != nil {
		return nil, err
	}
	if err = checkGeneratedExprReferences(ctx.GetContext(), bound, indexName, tableDef.Cols, make(map[int32]bool)); err != nil {
		return nil, err
	}
	if err = validateFunctionalIndexExpression(ctx.GetContext(), bound, tableDef); err != nil {
		return nil, err
	}
	if err = checkFunctionalIndexResultType(ctx.GetContext(), bound.Typ); err != nil {
		return nil, err
	}
	genExpr, err := makePlan2AssignmentCastExpr(ctx.GetContext(), bound, bound.Typ)
	if err != nil {
		return nil, err
	}
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(part.Expr, part.Expr, false)
	origin := trimFunctionalOuterParentheses(fmtCtx.String())
	if origin == "" {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "functional index expression cannot be empty")
	}

	hiddenName := functionalIndexColumnName(indexName)
	if existing := FindColumn(tableDef.Cols, hiddenName); existing != nil {
		return nil, moerr.NewInvalidInputf(ctx.GetContext(), "functional index internal column '%s' already exists", hiddenName)
	}
	hidden := &ColDef{
		ColId:      ^uint64(0),
		Name:       hiddenName,
		OriginName: hiddenName,
		Hidden:     true,
		Alg:        plan.CompressType_Lz4,
		Typ:        bound.Typ,
		Default:    &plan.Default{NullAbility: !bound.Typ.NotNullable},
		GeneratedCol: &plan.GeneratedCol{
			Expr:         genExpr,
			OriginString: origin,
			IsStored:     false,
		},
	}
	// A table without an explicit primary key keeps the synthetic fake-PK as
	// the final physical column. Insert the functional column immediately
	// before it so the existing DML/TAE layout contract is unchanged.
	if tableDef.Pkey != nil && catalog.IsFakePkName(tableDef.Pkey.PkeyColName) {
		fakePos := -1
		for i, col := range tableDef.Cols {
			if col != nil && catalog.IsFakePkName(col.Name) {
				fakePos = i
				break
			}
		}
		if fakePos >= 0 {
			tableDef.Cols = append(tableDef.Cols, nil)
			copy(tableDef.Cols[fakePos+1:], tableDef.Cols[fakePos:])
			tableDef.Cols[fakePos] = hidden
		} else {
			tableDef.Cols = append(tableDef.Cols, hidden)
		}
	} else {
		tableDef.Cols = append(tableDef.Cols, hidden)
	}
	rebuildTableColumnIndex(tableDef)

	lowered := cloneFunctionalIndex(indexInfo)
	lowered.Name = indexName
	lowered.KeyParts[0] = &tree.KeyPart{ColName: tree.NewUnresolvedColName(hiddenName)}
	return lowered, nil
}

// trimFunctionalOuterParentheses removes only one pair that encloses the
// complete formatted expression. The index DDL renderer adds its own pair,
// so retaining formatter-only grouping would otherwise produce (((expr))).
func trimFunctionalOuterParentheses(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return value
	}
	depth := 0
	var quote byte
	escaped := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(value)-1 {
				return value
			}
		}
	}
	if depth != 0 || quote != 0 {
		return value
	}
	return strings.TrimSpace(value[1 : len(value)-1])
}

func rebuildTableColumnIndex(tableDef *TableDef) {
	if tableDef == nil {
		return
	}
	tableDef.Name2ColIndex = make(map[string]int32, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		if col != nil {
			tableDef.Name2ColIndex[col.Name] = int32(i)
		}
	}
}

func checkFunctionalIndexResultType(ctx context.Context, typ Type) error {
	switch types.T(typ.Id) {
	case types.T_json, types.T_text, types.T_blob, types.T_datalink,
		types.T_geometry, types.T_geometry32,
		types.T_array_float32, types.T_array_float64, types.T_array_float16,
		types.T_array_bf16, types.T_array_int8, types.T_array_uint8:
		return moerr.NewNotSupportedf(ctx, "functional index expression returns unsupported type %s", types.T(typ.Id).String())
	}
	if typ.Id == int32(types.T_any) || typ.Id == 0 {
		return moerr.NewNotSupported(ctx, "functional index expression has no stable result type")
	}
	return nil
}

func hasFunctionalIndexColumnPart(indexDef *plan.IndexDef) bool {
	return indexDef != nil && len(indexDef.Parts) > 0 &&
		catalog.IsFunctionalIndexColumnName(catalog.ResolveAlias(indexDef.Parts[0]))
}

// isFunctionalIndexDef recognizes the catalog representation without adding a
// new protobuf field: the first key part names a reserved hidden generated col.
func isFunctionalIndexDef(tableDef *TableDef, indexDef *plan.IndexDef) bool {
	if tableDef == nil || indexDef == nil || indexDef.Unique || len(indexDef.Parts) != 2 {
		return false
	}
	if tableDef.IsTemporary || tableDef.TableType == catalog.SystemTemporaryTable ||
		util.TableIsClusterTable(tableDef.GetTableType()) || tableDef.TableType == catalog.SystemExternalRel {
		return false
	}
	if !catalog.IsNullIndexAlgo(indexDef.IndexAlgo) && catalog.ToLower(indexDef.IndexAlgo) != catalog.MoIndexBTreeAlgo.ToString() {
		return false
	}
	if !hasFunctionalIndexColumnPart(indexDef) {
		return false
	}
	if !catalog.IsAlias(indexDef.Parts[1]) {
		return false
	}
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(indexDef.IndexAlgoParams)
	if err != nil || len(prefixLengths) != 0 {
		return false
	}
	name := catalog.ResolveAlias(indexDef.Parts[0])
	col := FindColumn(tableDef.Cols, name)
	return col != nil && col.Hidden && catalog.IsFunctionalIndexColumnName(col.Name) &&
		col.GeneratedCol != nil && !col.GeneratedCol.IsStored &&
		col.GeneratedCol.Expr != nil && strings.TrimSpace(col.GeneratedCol.OriginString) != ""
}

func functionalIndexOrigin(tableDef *TableDef, indexDef *plan.IndexDef) (string, bool) {
	if !isFunctionalIndexDef(tableDef, indexDef) {
		return "", false
	}
	col := FindColumn(tableDef.Cols, catalog.ResolveAlias(indexDef.Parts[0]))
	if col == nil || col.GeneratedCol == nil || strings.TrimSpace(col.GeneratedCol.OriginString) == "" {
		return "", false
	}
	return strings.TrimSpace(col.GeneratedCol.OriginString), true
}

func normalizeFunctionalExpr(expr *plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}
	clone := DeepCopyExpr(expr)
	var walk func(*plan.Expr)
	walk = func(current *plan.Expr) {
		if current == nil {
			return
		}
		if col := current.GetCol(); col != nil {
			col.RelPos = 0
		}
		if fn := current.GetF(); fn != nil {
			for _, arg := range fn.Args {
				walk(arg)
			}
		}
		if list := current.GetList(); list != nil {
			for _, arg := range list.List {
				walk(arg)
			}
		}
	}
	walk(clone)
	return clone
}

func stripFunctionalAssignmentCast(expr *plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 1 || fn.Func == nil {
		return expr
	}
	switch strings.ToLower(fn.Func.ObjName) {
	case "cast", "cast_strict", "cast_assign", "cast_ignore":
		arg := fn.Args[0]
		if arg != nil && functionalIndexTypesEqual(arg.Typ, expr.Typ) {
			return arg
		}
	}
	return expr
}

func functionalExpressionMatches(generated, query *plan.Expr) bool {
	if generated == nil || query == nil {
		return false
	}
	left := normalizeFunctionalExpr(stripFunctionalAssignmentCast(generated))
	right := normalizeFunctionalExpr(stripFunctionalAssignmentCast(query))
	return functionalIndexExprTypesEqual(left, right) && exprStructuralEqual(left, right)
}

func functionalIndexExprTypesEqual(left, right *plan.Expr) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Typ.PadSpace != right.Typ.PadSpace {
		return false
	}
	switch l := left.Expr.(type) {
	case *plan.Expr_F:
		r, ok := right.Expr.(*plan.Expr_F)
		if !ok || l.F == nil || r.F == nil || len(l.F.Args) != len(r.F.Args) {
			return ok && l.F == r.F
		}
		for i := range l.F.Args {
			if !functionalIndexExprTypesEqual(l.F.Args[i], r.F.Args[i]) {
				return false
			}
		}
	case *plan.Expr_List:
		r, ok := right.Expr.(*plan.Expr_List)
		if !ok || l.List == nil || r.List == nil || len(l.List.List) != len(r.List.List) {
			return ok && l.List == r.List
		}
		for i := range l.List.List {
			if !functionalIndexExprTypesEqual(l.List.List[i], r.List.List[i]) {
				return false
			}
		}
	}
	return true
}

// functionalIndexQueryExpr returns the generated expression and its hidden
// column position for a catalog index. It is deliberately strict: malformed
// metadata disables the optional optimization instead of changing results.
func functionalIndexQueryExpr(tableDef *TableDef, indexDef *plan.IndexDef) (*plan.Expr, int32, bool) {
	if !isFunctionalIndexDef(tableDef, indexDef) {
		return nil, -1, false
	}
	if err := validateFunctionalIndexMetadata(context.Background(), tableDef); err != nil {
		return nil, -1, false
	}
	name := catalog.ResolveAlias(indexDef.Parts[0])
	pos, ok := tableDef.Name2ColIndex[name]
	if !ok || pos < 0 || int(pos) >= len(tableDef.Cols) {
		return nil, -1, false
	}
	col := tableDef.Cols[pos]
	if col == nil || col.GeneratedCol == nil || col.GeneratedCol.Expr == nil {
		return nil, -1, false
	}
	return col.GeneratedCol.Expr, pos, true
}

// validateFunctionalIndexMetadata checks every reserved index part before a
// metadata-facing statement is planned. Reserved names are never treated as
// ordinary user columns: a missing generated payload, an unexpected key shape,
// or a stale algorithm must surface as an internal catalog error instead of
// producing a DDL/SHOW result that cannot be restored faithfully.
func validateFunctionalIndexMetadata(ctx context.Context, tableDef *TableDef) error {
	if tableDef == nil {
		return nil
	}
	functionalColumns := make(map[string]struct{})
	for _, col := range tableDef.Cols {
		if col == nil || !catalog.IsFunctionalIndexColumnName(col.Name) {
			continue
		}
		if !col.Hidden || col.GeneratedCol == nil || col.GeneratedCol.IsStored ||
			col.GeneratedCol.Expr == nil || strings.TrimSpace(col.GeneratedCol.OriginString) == "" {
			return moerr.NewInternalError(ctx, "functional index has incomplete generated-column metadata")
		}
		functionalColumns[col.Name] = struct{}{}
	}
	functionalRefs := make(map[string]int)
	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil {
			continue
		}
		for partPos, part := range indexDef.Parts {
			if catalog.IsFunctionalIndexColumnName(catalog.ResolveAlias(part)) &&
				(partPos != 0 || !hasFunctionalIndexColumnPart(indexDef)) {
				return moerr.NewInternalError(ctx, "functional index hidden column is used by an unsupported key shape")
			}
		}
		if !hasFunctionalIndexColumnPart(indexDef) {
			continue
		}
		if !isFunctionalIndexDef(tableDef, indexDef) {
			return moerr.NewInternalError(ctx, "functional index has incomplete or unsupported metadata")
		}
		if _, ok := functionalIndexOrigin(tableDef, indexDef); !ok {
			return moerr.NewInternalError(ctx, "functional index has incomplete generated-column metadata")
		}
		functionalRefs[catalog.ResolveAlias(indexDef.Parts[0])]++
	}
	for name := range functionalColumns {
		if functionalRefs[name] != 1 {
			return moerr.NewInternalError(ctx, "functional index has orphaned hidden generated-column metadata")
		}
	}
	for _, col := range tableDef.Cols {
		if col == nil || !catalog.IsFunctionalIndexColumnName(col.Name) || col.GeneratedCol == nil {
			continue
		}
		value, err := validateFunctionalIndexExpressionValue(ctx, col.GeneratedCol.Expr, tableDef)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "functional index column '%s' has session-dependent expression: %s", col.Name, err)
		}
		if _, err = validateFunctionalIndexAssignmentTarget(ctx, value, col.Typ); err != nil {
			return moerr.NewInternalErrorf(ctx, "functional index column '%s' has unsafe declared type: %s", col.Name, err)
		}
		if err = checkFunctionalIndexResultType(ctx, col.Typ); err != nil {
			return moerr.NewInternalErrorf(ctx, "functional index column '%s' has unsupported key type: %s", col.Name, err)
		}
	}
	return nil
}
