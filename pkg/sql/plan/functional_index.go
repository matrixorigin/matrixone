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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const functionalIndexProtocolError = "functional indexes require all CNs to support protocol version 48"

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
	if !ok || version < defines.MORPCVersion48 {
		return moerr.NewNotSupported(ctx, functionalIndexProtocolError)
	}
	return nil
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
		col.GeneratedCol != nil && col.GeneratedCol.Expr != nil && strings.TrimSpace(col.GeneratedCol.OriginString) != ""
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
		if arg != nil && arg.Typ.Id == expr.Typ.Id && arg.Typ.Width == expr.Typ.Width && arg.Typ.Scale == expr.Typ.Scale && arg.Typ.Charset == expr.Typ.Charset {
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
	return exprStructuralEqual(left, right)
}

// functionalIndexQueryExpr returns the generated expression and its hidden
// column position for a catalog index. It is deliberately strict: malformed
// metadata disables the optional optimization instead of changing results.
func functionalIndexQueryExpr(tableDef *TableDef, indexDef *plan.IndexDef) (*plan.Expr, int32, bool) {
	if !isFunctionalIndexDef(tableDef, indexDef) {
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
	for _, indexDef := range tableDef.Indexes {
		if !hasFunctionalIndexColumnPart(indexDef) {
			continue
		}
		if !isFunctionalIndexDef(tableDef, indexDef) {
			return moerr.NewInternalError(ctx, "functional index has incomplete or unsupported metadata")
		}
		if _, ok := functionalIndexOrigin(tableDef, indexDef); !ok {
			return moerr.NewInternalError(ctx, "functional index has incomplete generated-column metadata")
		}
	}
	return nil
}
