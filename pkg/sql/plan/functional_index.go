// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

const functionalColumnPrefix = "__mo_fi_"

func functionalColumnName(indexName string, ordinal ...int) string {
	sum := sha256.Sum256([]byte(indexNameKey(indexName)))
	name := fmt.Sprintf("%s%x", functionalColumnPrefix, sum[:16])
	if len(ordinal) != 0 && ordinal[0] != 0 {
		name += fmt.Sprintf("_%d", ordinal[0])
	}
	return name
}

func hasFunctionalKey(parts []*tree.KeyPart) bool {
	for _, part := range parts {
		if part != nil && part.Expr != nil {
			return true
		}
	}
	return false
}

func isFunctionalColumn(col *ColDef) bool {
	return col != nil && col.Hidden && col.GeneratedCol != nil && strings.HasPrefix(col.Name, functionalColumnPrefix)
}

// functionalIndexColumn is a presence check, including indexes whose first
// part is an ordinary column. Consumers rendering/removing keys use all parts.
func functionalIndexColumn(table *TableDef, index *pb.IndexDef) *ColDef {
	if table == nil || index == nil {
		return nil
	}
	for ordinal := range index.Parts {
		if col := functionalIndexPartColumn(table, index, ordinal); col != nil {
			return col
		}
	}
	return nil
}

func functionalIndexPartColumn(table *TableDef, index *pb.IndexDef, ordinal int) *ColDef {
	if table == nil || index == nil || ordinal < 0 || ordinal >= len(index.Parts) || catalog.IsAlias(index.Parts[ordinal]) {
		return nil
	}
	name := index.Parts[ordinal]
	for _, col := range table.Cols {
		if isFunctionalColumn(col) && col.Name == name && name == functionalColumnName(index.IndexName, ordinal) {
			return col
		}
	}
	return nil
}

func functionalIndexColumns(table *TableDef, index *pb.IndexDef) []*ColDef {
	if table == nil || index == nil {
		return nil
	}
	var cols []*ColDef
	for i := range index.Parts {
		if col := functionalIndexPartColumn(table, index, i); col != nil {
			cols = append(cols, col)
		}
	}
	return cols
}

// functionalExpressionEligible deliberately does not equate foldability with
// persistence safety. Unknown functions and conversions fail closed. In
// particular, temporal casts can read the writer's timezone or SQL mode.
func functionalExpressionEligible(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	if expr == nil {
		return false
	}
	if _, ok := functionalRuntimeType(expr.Typ); !ok {
		return false
	}
	switch e := expr.Expr.(type) {
	case *pb.Expr_Lit:
		return e != nil && e.Lit != nil
	case *pb.Expr_Col:
		if e == nil || e.Col == nil {
			return false
		}
		pos := e.Col.ColPos
		if e.Col.RelPos != 0 || pos < 0 || int(pos) >= len(cols) || visiting[pos] {
			return false
		}
		col := cols[pos]
		if col == nil || col.Hidden || col.Typ.AutoIncr || !sameFunctionalValueType(expr.Typ, col.Typ) {
			return false
		}
		if col.GeneratedCol != nil {
			visiting[pos] = true
			ok := functionalGeneratedExpressionEligible(col, cols, visiting)
			delete(visiting, pos)
			return ok
		}
		return true
	case *pb.Expr_T:
		return e != nil && e.T != nil
	case *pb.Expr_F:
		resolved, ok := functionalResolvedFunction(expr)
		if !ok {
			return false
		}
		fid, overload := function.DecodeOverloadID(e.F.Func.Obj)
		switch fid {
		case function.LOWER, function.UPPER:
			if len(e.F.Args) != 1 || !functionalTextType(e.F.Args[0].Typ) || !functionalTextType(expr.Typ) {
				return false
			}
		case function.PLUS, function.MINUS, function.MULTI, function.ABS, function.UNARY_PLUS, function.UNARY_MINUS:
			arity := 2
			if fid == function.ABS || fid == function.UNARY_PLUS || fid == function.UNARY_MINUS {
				arity = 1
			}
			if len(e.F.Args) != arity {
				return false
			}
			if !types.T(expr.Typ.Id).IsInteger() {
				return false
			}
			for _, arg := range e.F.Args {
				if !types.T(arg.Typ.Id).IsInteger() {
					return false
				}
			}
		case function.CAST:
			// These are the implicit, explicit and comparison casts emitted by
			// the binder. Other registered CAST kernels have different contracts.
			if overload < 0 || overload > 2 {
				return false
			}
			if functionalJSONTextCastEligible(expr, cols, visiting) {
				return true
			}
			if len(e.F.Args) == 2 {
				from, to := types.T(e.F.Args[0].Typ.Id), types.T(expr.Typ.Id)
				if from.IsInteger() && to.IsInteger() && from.IsUnsignedInt() == to.IsUnsignedInt() && from.TypeLen() <= to.TypeLen() {
					break
				}
				// WithModeIndependentStringLiterals renders text containing a
				// backslash as cast(0x... as varchar). The source is a binary-form
				// literal, so this form is deterministic across SQL modes
				// and is safe to replay as a persisted functional expression.
				if functionalTextType(expr.Typ) && functionalTextType(e.F.Args[0].Typ) &&
					e.F.Args[0].GetLit() != nil && e.F.Args[0].GetLit().IsBin {
					break
				}
			}
			// Identity casts inserted by generated-column assignment are safe.
			if !sameFunctionalValueType(e.F.Args[0].Typ, expr.Typ) {
				return false
			}
		default:
			return false
		}
		for _, arg := range e.F.Args {
			if !functionalExpressionEligible(arg, cols, visiting) {
				return false
			}
		}
		expected := resolved.GetReturnType().PlanType()
		// The binder carries CHAR/padded argument provenance on text results;
		// it is deliberately not part of the runtime registry Type.
		if fid == function.CAST {
			expected = e.F.Args[1].Typ
		} else if fid == function.LOWER || fid == function.UPPER {
			expected.PadSpace = hasPadSpaceStringProvenance(e.F.Args[0])
		}
		if !sameFunctionalValueType(expected, expr.Typ) {
			return false
		}
		return true
	default:
		return false
	}
}

// Visible generated columns retain their declared assignment width during COPY.
// A strict VARCHAR width check can reject a row but cannot change a successfully
// assigned value with the session timezone or SQL mode. Admit only this wrapper
// at the generated-column boundary; arbitrary casts remain deny-by-default.
func functionalGeneratedExpressionEligible(col *ColDef, cols []*ColDef, visiting map[int32]bool) bool {
	expr := col.GeneratedCol.Expr
	if expr == nil || !sameFunctionalValueType(expr.Typ, col.Typ) {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast_strict" {
		if len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
			!sameFunctionalValueType(fn.Args[1].Typ, col.Typ) {
			return false
		}
		from, to := fn.Args[0].Typ, col.Typ
		if from.Id != int32(types.T_varchar) || to.Id != int32(types.T_varchar) || from.Width <= 0 || to.Width <= 0 {
			return false
		}
		from.Width = to.Width
		if !sameFunctionalValueType(from, to) {
			return false
		}
		inputType, inputOK := functionalRuntimeType(fn.Args[0].Typ)
		targetType, targetOK := functionalRuntimeType(to)
		if !inputOK || !targetOK {
			return false
		}
		resolved, err := function.GetFunctionByName(context.Background(), "cast_strict", []types.Type{inputType, targetType})
		if err != nil || resolved.GetEncodedOverloadID() != fn.Func.Obj {
			return false
		}
		expr = fn.Args[0]
	}
	return functionalExpressionEligible(expr, cols, visiting)
}

// Validate the serialized identity using the same registry/type checker as the
// binder, without inserting coercions or trusting the display name. The
// eligibility owner still decides which of these resolved kernels are safe.
func functionalResolvedFunction(expr *Expr) (function.FuncGetResult, bool) {
	invalid := function.FuncGetResult{}
	if expr == nil {
		return invalid, false
	}
	e, ok := expr.Expr.(*pb.Expr_F)
	if !ok || e == nil || e.F == nil || e.F.Func == nil {
		return invalid, false
	}
	fn := e.F
	// Limit registry lookup to this owner's scalar families. In particular,
	// damaged names must not enter unrelated functions' type checkers.
	switch fn.Func.ObjName {
	case "lower", "to_lower", "lcase", "upper", "to_upper", "ucase",
		"+", "-", "*", "abs", "unary_plus", "unary_minus", "cast",
		"json_extract", "json_unquote":
	default:
		return invalid, false
	}
	if _, ok := functionalRuntimeType(expr.Typ); !ok {
		return invalid, false
	}
	if _, exists := function.GetFunctionByIdWithoutError(fn.Func.Obj); !exists {
		return invalid, false
	}
	args := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		if arg == nil {
			return invalid, false
		}
		var ok bool
		args[i], ok = functionalRuntimeType(arg.Typ)
		if !ok {
			return invalid, false
		}
	}
	resolved, ok := function.GetFunctionByNameWithoutError(fn.Func.ObjName, args)
	if !ok {
		return invalid, false
	}
	fid, overload := function.DecodeOverloadID(fn.Func.Obj)
	if fid == function.CAST {
		// CAST's normal lookup selects overload 0; explicit/comparison casts
		// are selected by the binder's overload-aware API.
		if fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
			!sameFunctionalValueType(expr.Typ, fn.Args[1].Typ) {
			return invalid, false
		}
		target, ok := fn.Args[1].Expr.(*pb.Expr_T)
		if !ok || target == nil || target.T == nil {
			return invalid, false
		}
		var err error
		resolved, err = function.GetFunctionByNameWithOverload(context.Background(), "cast", args, overload)
		if err != nil {
			return invalid, false
		}
	}
	if resolved.GetEncodedOverloadID() != fn.Func.Obj {
		return invalid, false
	}
	if targets, needed := resolved.ShouldDoImplicitTypeCast(); needed {
		if len(targets) != len(args) {
			return invalid, false
		}
		for i := range args {
			if !args[i].Eq(targets[i]) {
				return invalid, false
			}
		}
	}
	return resolved, true
}

func functionalRuntimeType(typ Type) (types.Type, bool) {
	// TypeFromPlan checks wire widths/collations, but TypeLen panics for an
	// unassigned OID inside that range. Catalog admission must fail closed.
	if err := typ.ValidateCollation(); err != nil || strings.HasPrefix(types.T(typ.Id).String(), "unexpected type:") {
		return types.Type{}, false
	}
	runtimeType, err := types.TypeFromPlan(typ)
	return runtimeType, err == nil
}

func functionalTextType(typ Type) bool {
	return typ.Id == int32(types.T_varchar) || typ.Id == int32(types.T_char)
}

// lowerFunctionalIndex lowers each expression to an index-owned generated value. Ordinary
// generated-column writes and secondary-index maintenance then share exactly
// the same row expression for backfill and future DML.
func lowerFunctionalIndex(ctx CompilerContext, table *TableDef, index *tree.Index) (*tree.Index, error) {
	if !hasFunctionalKey(index.KeyParts) {
		return index, nil
	}
	if table.IsTemporary || util.TableIsClusterTable(table.TableType) {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes require ordinary persistent tables")
	}
	fail := func() (*tree.Index, error) {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "functional indexes require a named ordinary non-unique BTREE index")
	}
	if index.Name == "" ||
		(index.KeyType != tree.INDEX_TYPE_INVALID && index.KeyType != tree.INDEX_TYPE_BTREE) {
		return fail()
	}
	if option := index.IndexOption; option != nil {
		allowed := tree.IndexOption{IType: option.IType, Comment: option.Comment, Visible: option.Visible}
		if (option.IType != tree.INDEX_TYPE_INVALID && option.IType != tree.INDEX_TYPE_BTREE) || !reflect.DeepEqual(*option, allowed) {
			return fail()
		}
	}
	for _, key := range index.KeyParts {
		if key == nil || key.Length != 0 || key.Direction != tree.DefaultDirection || (key.Expr != nil && key.ColName != nil) {
			return fail()
		}
	}
	if err := RequirePersistedProtocolVersionForAuthoring(ctx.GetContext(), ctx.GetProcess(), defines.MORPCVersion107); err != nil {
		return nil, err
	}
	names := make([]string, len(table.Cols))
	typs := make([]Type, len(table.Cols))
	for i, col := range table.Cols {
		names[i], typs[i] = col.Name, col.Typ
	}
	copy := *index
	copy.KeyParts = append([]*tree.KeyPart(nil), index.KeyParts...)
	// Bind every expression against the original user schema, not siblings
	// appended by this index. Publish columns only after all parts validate.
	added := make([]*ColDef, 0, len(index.KeyParts))
	for ordinal, key := range index.KeyParts {
		if key.Expr == nil {
			continue
		}
		name := functionalColumnName(index.Name, ordinal)
		for _, col := range table.Cols {
			if col.Name == name {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "functional index backing column already exists")
			}
		}
		bindCtx := ddlExpressionContext(ctx, ctx.GetContext())
		expr, err := NewGeneratedColBinder(bindCtx, names, typs).bindPersistedExpr(key.Expr, 0, false)
		if err != nil {
			return nil, err
		}
		if !functionalExpressionEligible(expr, table.Cols, make(map[int32]bool)) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "functional index expression must have session-independent semantics")
		}
		if functionalExpressionUsesJSON(expr, table.Cols, make(map[int32]bool)) {
			if err := RequirePersistedProtocolVersionForAuthoring(ctx.GetContext(), ctx.GetProcess(), defines.MORPCVersion120); err != nil {
				return nil, err
			}
		}
		if err := preservePersistedFormatCompatibility(bindCtx, expr); err != nil {
			return nil, err
		}
		if err := checkExprForVolatileFunc(bindCtx, expr); err != nil {
			return nil, err
		}
		if err := checkGeneratedExprReferences(bindCtx, expr, name, table.Cols, make(map[int32]bool)); err != nil {
			return nil, err
		}
		if !types.T(expr.Typ.Id).IsInteger() && !functionalTextType(expr.Typ) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "functional index result type")
		}
		col := &ColDef{Name: name, OriginName: name, Hidden: true, Typ: expr.Typ,
			Alg: pb.CompressType_Lz4, Default: &pb.Default{NullAbility: true},
			GeneratedCol: &pb.GeneratedCol{Expr: expr, OriginString: tree.StringWithOpts(key.Expr, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString(), tree.WithModeIndependentStringLiterals()), IsStored: false}}
		replacement := &tree.KeyPart{ColName: tree.NewUnresolvedColName(name)}
		if err := checkIndexColumnSupportability(ctx.GetContext(), col, replacement, "index"); err != nil {
			return nil, err
		}
		added = append(added, col)
		copy.KeyParts[ordinal] = replacement
	}
	// PRE_INSERT appends synthesized composite keys after ordinary/generated
	// values. Keep that physical layout even though index lowering happens
	// after the primary/cluster key definitions have already been constructed.
	pos := len(table.Cols)
	for i, col := range table.Cols {
		if col.Name == catalog.CPrimaryKeyColName ||
			(table.ClusterBy != nil && col.Name == table.ClusterBy.Name && util.JudgeIsCompositeClusterByColumn(col.Name)) {
			pos = i
			break
		}
	}
	table.Cols = slices.Insert(table.Cols, pos, added...)
	return &copy, nil
}

// ValidateFunctionalIndexMetadata applies the same typed ownership and eligibility
// contract to checkpoint catalog reconstruction before SQL can re-author keys.
func ValidateFunctionalIndexMetadata(ctx context.Context, table *TableDef) error {
	return validateFunctionalTable(ctx, table)
}

func validateFunctionalTable(ctx context.Context, table *TableDef) error {
	owners := make(map[string]int)
	for _, index := range table.Indexes {
		if index == nil {
			return moerr.NewInvalidInput(ctx, "invalid index metadata")
		}
		if err := validateFunctionalJSONIndexSemantics(ctx, table, index); err != nil {
			return err
		}
		functional := false
		for _, part := range index.Parts {
			if catalog.IsAlias(part) || !strings.HasPrefix(part, functionalColumnPrefix) {
				continue
			}
			col := FindColumn(table.Cols, part)
			if col == nil {
				return moerr.NewInvalidInput(ctx, "invalid functional index metadata")
			}
			functional = functional || col.Hidden
		}
		if !functional {
			continue
		}
		if index.Unique || !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
			return moerr.NewInvalidInput(ctx, "invalid functional index metadata")
		}
		for ordinal, part := range index.Parts {
			if catalog.IsAlias(part) || !strings.HasPrefix(part, functionalColumnPrefix) {
				continue
			}
			if col := FindColumn(table.Cols, part); col != nil && !col.Hidden {
				continue
			}
			col := functionalIndexPartColumn(table, index, ordinal)
			if col == nil || col.GeneratedCol.IsStored || col.GeneratedCol.Expr == nil || col.GeneratedCol.OriginString == "" ||
				!sameFunctionalValueType(col.Typ, col.GeneratedCol.Expr.Typ) ||
				!functionalExpressionEligible(col.GeneratedCol.Expr, table.Cols, make(map[int32]bool)) {
				return moerr.NewInvalidInput(ctx, "invalid functional index metadata")
			}
			owners[col.Name]++
		}
	}
	for _, col := range table.Cols {
		if col.Hidden && strings.HasPrefix(col.Name, functionalColumnPrefix) && (!isFunctionalColumn(col) || owners[col.Name] != 1) {
			return moerr.NewInvalidInput(ctx, "orphaned functional index backing column")
		}
	}
	return nil
}

// renameFunctionalColumnDependencies is used only by COPY RENAME/CHANGE.
// Ordinary generated dependencies remain unsupported. Owned index expressions
// are rewritten as syntax, then replayed by COPY against the final schema.
func renameFunctionalColumnDependencies(ctx context.Context, table *TableDef, oldName, newName string) error {
	if err := validateFunctionalTable(ctx, table); err != nil {
		return err
	}
	names := make([]string, len(table.Cols))
	types := make([]Type, len(table.Cols))
	for i, col := range table.Cols {
		names[i], types[i] = col.Name, col.Typ
		if strings.EqualFold(col.Name, oldName) {
			names[i] = strings.ToLower(newName)
		}
	}
	rewritten := make(map[int]*pb.GeneratedCol)
	for i, col := range table.Cols {
		if col.GeneratedCol == nil || !exprReferencesColumn(col.GeneratedCol.Expr, oldName, table.Cols) {
			continue
		}
		if !isFunctionalColumn(col) {
			return moerr.NewInvalidInputf(ctx, "Cannot modify column '%s': generated column '%s' depends on it", oldName, col.Name)
		}
		generated, err := rewriteFunctionalColumnDependency(ctx, col, names, types, oldName, newName)
		if err != nil {
			return err
		}
		rewritten[i] = generated
	}
	for i, generated := range rewritten {
		table.Cols[i].GeneratedCol = generated
	}
	return nil
}

func rewriteFunctionalColumnDependency(ctx context.Context, col *ColDef, names []string, types []Type, oldName, newName string) (*pb.GeneratedCol, error) {
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+col.GeneratedCol.OriginString, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return nil, moerr.NewInvalidInput(ctx, "invalid functional index expression")
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(clause.Exprs) != 1 {
		return nil, moerr.NewInvalidInput(ctx, "invalid functional index expression")
	}
	visitor := &renameCheckColumnVisitor{oldName: oldName, newName: newName}
	expr, ok := clause.Exprs[0].Expr.Accept(visitor)
	if !ok || !visitor.changed {
		return nil, moerr.NewInvalidInput(ctx, "functional index source cannot be renamed")
	}
	bound, err := NewGeneratedColBinder(ctx, names, types).BindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	return &pb.GeneratedCol{Expr: bound, OriginString: tree.StringWithOpts(expr, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString(), tree.WithModeIndependentStringLiterals()), IsStored: false}, nil
}

// COPY MODIFY/CHANGE updates the source schema before replaying its CREATE SQL.
// Refresh backing values and their generated dependency closure at that boundary
// so strict catalog validation never compares old argument types with the new
// source schema. Visible generated columns keep their declared assignment types.
// The caller validates the original metadata before changing the planner-owned table.
func rebindFunctionalIndexColumns(ctx CompilerContext, table *TableDef) error {
	names := make([]string, len(table.Cols))
	colTypes := make([]Type, len(table.Cols))
	for i, col := range table.Cols {
		names[i], colTypes[i] = col.Name, col.Typ
	}
	bind := func(col *ColDef) (*Expr, error) {
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "select "+col.GeneratedCol.OriginString, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		selectStmt, ok := stmt.(*tree.Select)
		if !ok {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "invalid functional index expression")
		}
		clause, ok := selectStmt.Select.(*tree.SelectClause)
		if !ok || len(clause.Exprs) != 1 {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "invalid functional index expression")
		}
		return NewGeneratedColBinder(ddlExpressionContext(ctx, ctx.GetContext()), names, colTypes).BindExpr(clause.Exprs[0].Expr, 0, true)
	}
	state := make([]uint8, len(table.Cols))
	var rebind func(int) error
	rebind = func(i int) error {
		col := table.Cols[i]
		if col.GeneratedCol == nil || state[i] == 2 {
			return nil
		}
		if state[i] == 1 {
			return moerr.NewInvalidInput(ctx.GetContext(), "cyclic functional index dependency")
		}
		state[i] = 1
		if err := pb.VisitExprTree(col.GeneratedCol.Expr, func(expr *Expr) error {
			if ref := expr.GetCol(); ref != nil {
				if ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(table.Cols) {
					return moerr.NewInvalidInput(ctx.GetContext(), "invalid functional index dependency")
				}
				return rebind(int(ref.ColPos))
			}
			return nil
		}); err != nil {
			return err
		}
		expr, err := bind(col)
		if err != nil {
			return err
		}
		if isFunctionalColumn(col) {
			col.Typ = expr.Typ
		} else {
			expr, err = makePlan2AssignmentCastExpr(ctx.GetContext(), expr, col.Typ)
			if err != nil {
				return err
			}
		}
		col.GeneratedCol.Expr = expr
		colTypes[i] = col.Typ
		state[i] = 2
		return nil
	}
	for i, col := range table.Cols {
		if isFunctionalColumn(col) {
			if err := rebind(i); err != nil {
				return err
			}
		}
	}
	return validateFunctionalTable(ctx.GetContext(), table)
}

func sameFunctionalValueType(a, b Type) bool {
	a.Table, b.Table = "", ""
	a.NotNullable, b.NotNullable = false, false
	return proto.Equal(&a, &b)
}

// addFunctionalIndexFilters supplies an indexable equality while retaining the
// original predicate as a base-row residual. Matching is structural and typed;
// it never uses algebraic equivalence or session-dependent evaluation.
func (builder *QueryBuilder) addFunctionalIndexFilters(node *pb.Node) {
	if node.TableDef == nil || len(node.BindingTags) != 1 {
		return
	}
	originals := append([]*Expr(nil), node.FilterList...)
	for _, index := range node.TableDef.Indexes {
		for _, col := range functionalIndexColumns(node.TableDef, index) {
			if col == nil || !functionalExpressionEligible(col.GeneratedCol.Expr, node.TableDef.Cols, make(map[int32]bool)) {
				continue
			}
			var pos int32 = -1
			for i, c := range node.TableDef.Cols {
				if c.Name == col.Name {
					pos = int32(i)
				}
			}
			for _, filter := range originals {
				f := filter.GetF()
				if f == nil || f.Func.ObjName != "=" || len(f.Args) != 2 {
					continue
				}
				for side := 0; side < 2; side++ {
					constant := f.Args[1-side]
					if constant.GetLit() == nil || constant.GetLit().Isnull {
						continue
					}
					candidate := DeepCopyExpr(functionalJSONComparisonOperand(f.Args[side], node.TableDef.Cols, node.BindingTags[0]))
					persisted := DeepCopyExpr(col.GeneratedCol.Expr)
					if !normalizeFunctionalExpression(candidate, node.BindingTags[0]) || !normalizeFunctionalExpression(persisted, 0) ||
						!functionalExpressionEligible(candidate, node.TableDef.Cols, make(map[int32]bool)) || !proto.Equal(candidate, persisted) {
						continue
					}
					ref := &Expr{Typ: col.Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: node.BindingTags[0], ColPos: pos}}}
					equality, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{ref, DeepCopyExpr(constant)})
					if err == nil {
						equality.Selectivity = filter.Selectivity
						node.FilterList = append(node.FilterList, equality)
					}
				}
			}
		}
	}
}

func normalizeFunctionalExpression(expr *Expr, tag int32) bool {
	expr.AuxId, expr.Ndv, expr.Selectivity = 0, 0, 0
	// Provenance and inferred nullability do not change expression values.
	// Keep value-bearing type metadata (width, scale, charset, padding) exact.
	expr.Typ.Table = ""
	expr.Typ.NotNullable = false
	if col := expr.GetCol(); col != nil {
		if col.RelPos != tag {
			return false
		}
		col.RelPos = 0
		col.Name = ""
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			if !normalizeFunctionalExpression(arg, tag) {
				return false
			}
		}
	}
	return true
}
