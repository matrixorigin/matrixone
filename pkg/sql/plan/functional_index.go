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
	switch e := expr.Expr.(type) {
	case *pb.Expr_Lit:
		return true
	case *pb.Expr_Col:
		pos := e.Col.ColPos
		if e.Col.RelPos != 0 || pos < 0 || int(pos) >= len(cols) || visiting[pos] {
			return false
		}
		col := cols[pos]
		if col == nil || col.Hidden || col.Typ.AutoIncr {
			return false
		}
		if col.GeneratedCol != nil {
			visiting[pos] = true
			ok := functionalExpressionEligible(col.GeneratedCol.Expr, cols, visiting)
			delete(visiting, pos)
			return ok
		}
		return true
	case *pb.Expr_T:
		return true
	case *pb.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		if functionalJSONTextCastEligible(expr, cols) {
			return true
		}
		if functionalJSONNumericCastEligible(expr, cols) {
			return true
		}
		switch e.F.Func.ObjName {
		case "lower", "upper":
			if len(e.F.Args) != 1 || !functionalTextType(e.F.Args[0].Typ) || !functionalTextType(expr.Typ) {
				return false
			}
		case "substring", "substr", "mid", "left", "reverse", "md5", "crc32", "hex", "length", "concat":
			if !functionalScalarFunctionEligible(expr) {
				return false
			}
		case "+", "-", "*", "abs":
			if !types.T(expr.Typ.Id).IsInteger() {
				return false
			}
			for _, arg := range e.F.Args {
				if !types.T(arg.Typ.Id).IsInteger() {
					return false
				}
			}
		case "unary_minus":
			// The persisted bind keeps an authored negative integer constant
			// in its unary_minus spelling while query binding folds it, so a
			// suffix-position key like substring(v,-2,2) re-binds through
			// every replay in exactly this form. Admit precisely that
			// constant shape - one non-null signed-integer literal argument
			// under the identical signed-integer result type - and nothing
			// else. Negated columns (with or without their widening or
			// signedness adapters) and unsigned or non-integer negations
			// keep failing closed, so the unsigned-column boundary stays
			// observable.
			if len(e.F.Args) != 1 {
				return false
			}
			lit := e.F.Args[0].GetLit()
			if lit == nil || lit.Isnull ||
				!types.T(e.F.Args[0].Typ.Id).IsSignedInt() || !types.T(expr.Typ.Id).IsSignedInt() ||
				e.F.Args[0].Typ.Id != expr.Typ.Id {
				return false
			}
		case "cast":
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
				// The eight scalar functions can require one implicit argument
				// adapter: md5 has no CHAR overload, so the binder inserts a
				// flag-clear ordinary CAST from a T_char column into the fixed
				// non-truncating T_varchar envelope with the charset retained.
				// Every user-explicit or width-declaring cast stays excluded.
				if functionalImplicitCharWidenCast(expr) {
					break
				}
			}
			// Identity casts inserted by generated-column assignment are safe.
			if len(e.F.Args) != 2 || e.F.Args[0].Typ.Id != expr.Typ.Id ||
				e.F.Args[0].Typ.Width != expr.Typ.Width || e.F.Args[0].Typ.Scale != expr.Typ.Scale ||
				e.F.Args[0].Typ.Charset != expr.Typ.Charset || e.F.Args[0].Typ.PadSpace != expr.Typ.PadSpace {
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
		return true
	default:
		return false
	}
}

func functionalTextType(typ Type) bool {
	return typ.Id == int32(types.T_varchar) || typ.Id == int32(types.T_char)
}

// functionalScalarFunctionEligible admits the eight scalar functions of this
// increment — substring (with its substr/mid spellings), left, reverse, md5,
// crc32, hex, length and concat — by resolved identity and argument shape,
// never by SQL spelling alone. The bound node's encoded overload ID must
// decode to one of the eight function IDs with the arity, argument-domain and
// result predicates below, and must re-resolve in the registry to the same
// encoded overload ID with the same return type, permitting only the
// constant-value-refined narrowing of the type-level width envelope
// (verifyFunctionalIndexOverload). Argument-node structure is validated by
// the caller's recursive walk.
func functionalScalarFunctionEligible(expr *Expr) bool {
	if expr == nil || expr.Typ.ValidateCollation() != nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	fid, overload := function.DecodeOverloadID(fn.Func.Obj)
	textResult := functionalTextType(expr.Typ)
	integerResult := types.T(expr.Typ.Id).IsInteger()
	textArg := func(arg *Expr) bool { return arg != nil && functionalTextType(arg.Typ) }
	integerArg := func(arg *Expr) bool { return arg != nil && types.T(arg.Typ.Id).IsInteger() }
	switch fid {
	case function.SUBSTRING:
		// Position/count arguments are integer plan types (integer literals,
		// or signed integer columns through the walk's widening-cast rule).
		if !textResult || len(fn.Args) < 2 || len(fn.Args) > 3 || !textArg(fn.Args[0]) {
			return false
		}
		for _, arg := range fn.Args[1:] {
			if !integerArg(arg) {
				return false
			}
		}
	case function.LEFT:
		if !textResult || len(fn.Args) != 2 || !textArg(fn.Args[0]) || !integerArg(fn.Args[1]) {
			return false
		}
	case function.REVERSE, function.MD5, function.HEX:
		if !textResult || len(fn.Args) != 1 || !textArg(fn.Args[0]) {
			return false
		}
	case function.LENGTH, function.CRC32:
		// Integer results: uint64 (crc32) and int64 (length) keys.
		if !integerResult || len(fn.Args) != 1 || !textArg(fn.Args[0]) {
			return false
		}
	case function.CONCAT:
		// Every argument must already be in the text domain: a formatted
		// scalar argument owns a conversion the recursive walk rejects.
		if !textResult || len(fn.Args) == 0 {
			return false
		}
		for _, arg := range fn.Args {
			if !textArg(arg) {
				return false
			}
		}
	default:
		return false
	}
	// The registry re-derivation reproduces the encoded overload and the
	// type-level return envelope from the argument types; substring/left can
	// carry a constant-value-refined width that narrows the envelope, so the
	// scalar path permits the refined width (see
	// verifyFunctionalIndexOverload).
	return verifyFunctionalIndexOverload(expr, fn.Func.ObjName, fid, overload, true)
}

// functionalImplicitCharWidenCast recognizes the one implicit argument
// adapter the eight scalar functions can require: md5 has no CHAR overload,
// so the binder inserts a flag-clear ordinary CAST from the T_char column
// into the matcher's fixed T_varchar envelope of MaxVarcharLen with the
// source charset retained — a pure, non-truncating widening whose execution
// path copies the bytes unchanged. A user-explicit or width-declaring cast
// never matches this rule and stays excluded; the recursive walk validates
// the column source itself.
func functionalImplicitCharWidenCast(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.SyntaxExplicitCast || len(fn.Args) != 2 ||
		fn.Func.Obj != int64(function.CAST)<<32 {
		return false
	}
	from, to := fn.Args[0], expr.Typ
	if from == nil || from.GetCol() == nil || from.GetLit() != nil ||
		from.Typ.Id != int32(types.T_char) || to.Id != int32(types.T_varchar) ||
		to.Width != int32(types.MaxVarcharLen) || from.Typ.Charset != to.Charset {
		return false
	}
	return fn.Args[1].GetT() != nil && sameFunctionalValueType(to, fn.Args[1].Typ)
}

// functionalSessionDependentConversion reports whether a fail-closed
// eligibility miss contains a conversion whose implementation demonstrably
// reads session state, and is therefore reported with the retained
// session-independence message rather than as rejection-by-coverage:
//   - casts whose source or target plan type is TIMESTAMP — those conversion
//     paths read the session time zone;
//   - string-to-float32/64 casts — they read the session native-compatibility
//     mode;
//   - string-to-DATETIME/DATE/TIME casts — the parsing paths apply the
//     session zero-date SQL-mode policy and TIME_TRUNCATE_FRACTIONAL.
//
// Deterministic temporal conversions such as DATE-to-CHAR stay outside this
// audit and report rejection-by-coverage.
func functionalSessionDependentConversion(expr *Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *pb.Expr_F:
		if e.F == nil {
			return false
		}
		if e.F.Func != nil {
			if fid, _ := function.DecodeOverloadID(e.F.Func.Obj); fid == function.CAST && len(e.F.Args) == 2 {
				from, to := types.T(e.F.Args[0].Typ.Id), types.T(expr.Typ.Id)
				if from == types.T_timestamp || to == types.T_timestamp {
					return true
				}
				if from.IsMySQLString() {
					// typeswitch:partial the audited session-dependent string-parse targets are exactly the design-reviewed float32/64 boundary (func_cast.go reads the native-compatibility mode); bf16/float16/float8/float4 cast targets deliberately report rejection-by-coverage
					switch to {
					case types.T_float32, types.T_float64, types.T_datetime, types.T_date, types.T_time:
						return true
					}
				}
			}
		}
		for _, arg := range e.F.Args {
			if functionalSessionDependentConversion(arg) {
				return true
			}
		}
	}
	return false
}

// lowerFunctionalIndex lowers each expression to an index-owned generated value. Ordinary
// generated-column writes and secondary-index maintenance then share exactly
// the same row expression for backfill and future DML.
// functionalIndexErrorExprText renders the authored key expression for the
// category-3 diagnostic message. It preserves the authored spelling -
// identifiers, function names, string literals in single quotes, letter case
// - and joins expression-list items with "," (no space) so the message keeps
// the authored word shape of multi-argument calls. This is diagnostic-only:
// persisted SQL keeps the standard ", " separator because it is re-parsed.
func functionalIndexErrorExprText(expr tree.Expr) string {
	return tree.StringWithOpts(expr, dialect.MYSQL,
		tree.WithSingleQuoteString(), tree.WithCompactArgLists())
}

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
			// A bind-stage resolution failure - unknown function, wrong
			// arity, no matching signature - never reaches the eligibility
			// walk, and the raw registry error (`invalid argument function
			// substring, bad value [VARCHAR]`) misreports it. These are
			// coverage rejections per the approved error model, so route
			// them into the category-3 message naming the raw text. Other
			// bind errors keep their native messages: the retained MEDIUMINT
			// NYI cast target, parse errors, and identifier errors are not
			// resolution failures.
			if moerr.IsMoErrCode(err, moerr.ErrInvalidArg) {
				return nil, moerr.NewNotSupportedf(ctx.GetContext(), "functional index expression is not supported: %s",
					functionalIndexErrorExprText(key.Expr))
			}
			return nil, err
		}
		// Genuinely non-deterministic functions are reported before
		// eligibility with the generated-column message that names the
		// offending function; the walk does not evaluate volatility.
		if err := checkExprForVolatileFunc(bindCtx, expr); err != nil {
			return nil, err
		}
		if !functionalExpressionEligible(expr, table.Cols, make(map[int32]bool)) {
			// Three-way diagnostics: demonstrably session-dependent
			// conversions keep the session-independence message, everything
			// else is rejection-by-coverage naming the expression.
			if functionalSessionDependentConversion(expr) {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "functional index expression must have session-independent semantics")
			}
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "functional index expression is not supported: %s",
				functionalIndexErrorExprText(key.Expr))
		}
		if err := preservePersistedFormatCompatibility(bindCtx, expr); err != nil {
			return nil, err
		}
		if err := checkGeneratedExprReferences(bindCtx, expr, name, table.Cols, make(map[int32]bool)); err != nil {
			return nil, err
		}
		if !types.T(expr.Typ.Id).IsInteger() && !functionalTextType(expr.Typ) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "functional index result type")
		}
		origin := renderFunctionalIndexOriginString(key.Expr)
		if err := verifyFunctionalIndexOriginRoundTrip(ctx.GetContext(), bindCtx, table, names, typs, expr, origin); err != nil {
			return nil, err
		}
		col := &ColDef{Name: name, OriginName: name, Hidden: true, Typ: expr.Typ,
			Alg: pb.CompressType_Lz4, Default: &pb.Default{NullAbility: true},
			GeneratedCol: &pb.GeneratedCol{Expr: expr, OriginString: origin, IsStored: false}}
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

func validateFunctionalTable(ctx context.Context, table *TableDef) error {
	owners := make(map[string]int)
	for _, index := range table.Indexes {
		if index == nil {
			return moerr.NewInvalidInput(ctx, "invalid index metadata")
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
				(!types.T(col.Typ.Id).IsInteger() && !functionalTextType(col.Typ)) ||
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
	oldNames := make([]string, len(table.Cols))
	names := make([]string, len(table.Cols))
	types := make([]Type, len(table.Cols))
	for i, col := range table.Cols {
		oldNames[i], names[i], types[i] = col.Name, col.Name, col.Typ
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
		// The rewritten persisted text is the artifact every later replay
		// rebuilds from: require it to re-bind the rewritten tree's hidden
		// key type and to keep the binding class of the text it replaces
		// before the ALTER can commit.
		if err := verifyFunctionalIndexRewriteRoundTrip(ctx, table, oldNames, names, types, col.GeneratedCol.OriginString, generated); err != nil {
			return err
		}
		rewritten[i] = generated
	}
	for i, generated := range rewritten {
		table.Cols[i].GeneratedCol = generated
	}
	return nil
}

// parseFunctionalIndexOriginString parses persisted functional-expression SQL
// in its select-statement storage form and returns the rebuilt expression.
func parseFunctionalIndexOriginString(ctx context.Context, origin string) (tree.Expr, error) {
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+origin, 1)
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
	return clause.Exprs[0].Expr, nil
}

// verifyFunctionalIndexRewriteRoundTrip mirrors the authoring assertion at the
// identifier-rewrite seam. The freshly rewritten persisted text must re-bind
// against the post-rename schema and carry the rewritten tree's hidden key
// plan type exactly — the same OID, width and scale, with no tolerance. The
// rewrite must not degrade the class it was handed: the rewritten text and
// the text it replaces must re-bind equal types, and a text whose pre-rename
// re-bind admitted under the walk must still re-admit. Older binaries'
// widthless texts do not re-admit before the rewrite either; their rename
// keeps base behavior, so the ALTER fails only when the rewrite itself would
// degrade the persisted text every later replay rebuilds from.
func verifyFunctionalIndexRewriteRoundTrip(ctx context.Context, table *TableDef, oldNames, names []string, types []Type, previous string, generated *pb.GeneratedCol) error {
	rebuilt, err := parseFunctionalIndexOriginString(ctx, generated.OriginString)
	if err != nil {
		return err
	}
	rebound, err := NewGeneratedColBinder(ctx, names, types).BindExpr(rebuilt, 0, true)
	if err != nil {
		return err
	}
	if rebound.Typ.Id != generated.Expr.Typ.Id ||
		rebound.Typ.Width != generated.Expr.Typ.Width ||
		rebound.Typ.Scale != generated.Expr.Typ.Scale {
		return moerr.NewInvalidInput(ctx, "cannot rewrite functional index expression")
	}
	prior, err := parseFunctionalIndexOriginString(ctx, previous)
	if err != nil {
		return err
	}
	priorBound, err := NewGeneratedColBinder(ctx, oldNames, types).BindExpr(prior, 0, true)
	if err != nil {
		return err
	}
	if rebound.Typ.Id != priorBound.Typ.Id ||
		rebound.Typ.Width != priorBound.Typ.Width ||
		rebound.Typ.Scale != priorBound.Typ.Scale {
		return moerr.NewInvalidInput(ctx, "cannot rewrite functional index expression")
	}
	if functionalExpressionEligible(priorBound, table.Cols, make(map[int32]bool)) &&
		!functionalExpressionEligible(rebound, table.Cols, make(map[int32]bool)) {
		return moerr.NewInvalidInput(ctx, "cannot rewrite functional index expression")
	}
	return nil
}

func rewriteFunctionalColumnDependency(ctx context.Context, col *ColDef, names []string, types []Type, oldName, newName string) (*pb.GeneratedCol, error) {
	expr, err := parseFunctionalIndexOriginString(ctx, col.GeneratedCol.OriginString)
	if err != nil {
		return nil, err
	}
	visitor := &renameCheckColumnVisitor{oldName: oldName, newName: newName}
	rewritten, ok := expr.Accept(visitor)
	if !ok || !visitor.changed {
		return nil, moerr.NewInvalidInput(ctx, "functional index source cannot be renamed")
	}
	bound, err := NewGeneratedColBinder(ctx, names, types).BindExpr(rewritten, 0, true)
	if err != nil {
		return nil, err
	}
	// The width-preserving literal serialization and the CAST-spelling
	// canonicalization are shared with the lowering render, so a literal's
	// persisted spelling class survives parsing and identifier rewriting.
	return &pb.GeneratedCol{Expr: bound, OriginString: renderFunctionalIndexOriginString(rewritten), IsStored: false}, nil
}

func sameFunctionalValueType(a, b Type) bool {
	if a.ValidateCollation() != nil || b.ValidateCollation() != nil ||
		a.CollationMergeConflict || b.CollationMergeConflict {
		return false
	}
	a.Table, b.Table = "", ""
	a.NotNullable, b.NotNullable = false, false
	// The catalog's physical type codec omits expression coercibility. Keep
	// identity, revision, width, scale and padding exact after validating both
	// plans; a generated expression still owns its binding provenance.
	a.CollationCoercibility, b.CollationCoercibility = 0, 0
	a.CollationCoercibilitySet, b.CollationCoercibilitySet = false, false
	return proto.Equal(&a, &b)
}

// renderFunctionalIndexOriginString is the single serialization shared by
// every producer of persisted functional-expression SQL: the lowering render
// and the identifier-rewrite render of RENAME COLUMN / renaming CHANGE
// COLUMN. It runs after parsing and identifier rewriting, keeps
// backslash-containing text literals in the width-preserving hex-introducer
// spelling (the persisted-SQL formatting already provided for view
// definitions) and canonicalizes the two LONGLONG-family CAST spellings whose
// textual rendering would otherwise re-bind a narrower target.
func renderFunctionalIndexOriginString(expr tree.Expr) string {
	return tree.StringWithOpts(canonicalizeFunctionalIndexCastSpellings(expr), dialect.MYSQL, tree.WithViewDefinition())
}

// canonicalizeFunctionalIndexCastSpellings rewrites, copy-on-write, the two
// CAST targets that bind int64/uint64 but render a narrowed textual target:
// `AS SIGNED INTEGER` persists as `AS SIGNED` and `AS UNSIGNED INTEGER` as
// `AS UNSIGNED` — the same bound targets in a lossless rendering. Every other
// CAST head renders verbatim. On a miss the input tree is returned unchanged;
// the lowering round-trip assertion is the fail-closed backstop for any
// future spelling this rule does not cover.
func canonicalizeFunctionalIndexCastSpellings(expr tree.Expr) tree.Expr {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		args, changed := canonicalizeFunctionalIndexCastArgs(e.Exprs)
		if !changed {
			return e
		}
		rewrite := *e
		rewrite.Exprs = args
		return &rewrite
	case *tree.CastExpr:
		inner := canonicalizeFunctionalIndexCastSpellings(e.Expr)
		target, canonicalized := canonicalizeFunctionalIndexCastTarget(e.Type)
		if inner == e.Expr && !canonicalized {
			return e
		}
		rewrite := *e
		rewrite.Expr = inner
		rewrite.Type = target
		return &rewrite
	case *tree.BinaryExpr:
		left := canonicalizeFunctionalIndexCastSpellings(e.Left)
		right := canonicalizeFunctionalIndexCastSpellings(e.Right)
		if left == e.Left && right == e.Right {
			return e
		}
		rewrite := *e
		rewrite.Left, rewrite.Right = left, right
		return &rewrite
	case *tree.UnaryExpr:
		inner := canonicalizeFunctionalIndexCastSpellings(e.Expr)
		if inner == e.Expr {
			return e
		}
		rewrite := *e
		rewrite.Expr = inner
		return &rewrite
	case *tree.ParenExpr:
		inner := canonicalizeFunctionalIndexCastSpellings(e.Expr)
		if inner == e.Expr {
			return e
		}
		rewrite := *e
		rewrite.Expr = inner
		return &rewrite
	default:
		return expr
	}
}

func canonicalizeFunctionalIndexCastArgs(exprs tree.Exprs) (tree.Exprs, bool) {
	rewritten := make(tree.Exprs, len(exprs))
	changed := false
	for i, arg := range exprs {
		rewritten[i] = canonicalizeFunctionalIndexCastSpellings(arg)
		if rewritten[i] != arg {
			changed = true
		}
	}
	return rewritten, changed
}

// canonicalizeFunctionalIndexCastTarget narrows the SIGNED/UNSIGNED family's
// LONGLONGInternalGrammar spelling recorded from the trailing INTEGER token
// to the bare family spelling of the identical bound target.
func canonicalizeFunctionalIndexCastTarget(target tree.ResolvableTypeReference) (tree.ResolvableTypeReference, bool) {
	typ, ok := target.(*tree.T)
	if !ok || typ.InternalType.Family != tree.IntFamily ||
		typ.InternalType.Oid != uint32(defines.MYSQL_TYPE_LONGLONG) ||
		typ.InternalType.Width != 64 ||
		!strings.EqualFold(typ.InternalType.FamilyString, "integer") {
		return target, false
	}
	rewrite := *typ
	if rewrite.InternalType.Unsigned {
		rewrite.InternalType.FamilyString = ""
	} else {
		rewrite.InternalType.FamilyString = "signed"
	}
	return &rewrite, true
}

// verifyFunctionalIndexOriginRoundTrip re-parses and re-binds the rendered
// persistence text before the authoring DDL is accepted: the re-bound
// expression must re-admit under the eligibility walk and carry the authored
// hidden key plan type exactly — the same OID, width and scale, with no
// tolerance. A diverging rendering fails the DDL fail-closed instead of
// persisting a replay-lossy text.
func verifyFunctionalIndexOriginRoundTrip(ctx, bindCtx context.Context, table *TableDef, names []string, typs []Type, authored *Expr, origin string) error {
	rebuilt, err := parseFunctionalIndexOriginString(ctx, origin)
	if err != nil {
		return err
	}
	rebound, err := NewGeneratedColBinder(bindCtx, names, typs).bindPersistedExpr(rebuilt, 0, false)
	if err != nil {
		return err
	}
	if !functionalExpressionEligible(rebound, table.Cols, make(map[int32]bool)) ||
		rebound.Typ.Id != authored.Typ.Id ||
		rebound.Typ.Width != authored.Typ.Width ||
		rebound.Typ.Scale != authored.Typ.Scale {
		return moerr.NewNotSupported(ctx, "functional index expression does not persist losslessly")
	}
	return nil
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
		if indexUsesVirtualTimestamp(index, node.TableDef) {
			continue
		}
		for _, col := range functionalIndexColumns(node.TableDef, index) {
			if col == nil || !functionalExpressionEligible(col.GeneratedCol.Expr, node.TableDef.Cols, make(map[int32]bool)) ||
				!functionalHiddenKeyByteStable(col, node.TableDef.Cols) {
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
	if expr == nil || expr.Typ.ValidateCollation() != nil || expr.Typ.CollationMergeConflict {
		return false
	}
	expr.AuxId, expr.Ndv, expr.Selectivity = 0, 0, 0
	// The prepared_numeric pointer is sparse planner provenance -
	// prepared-statement envelopes, runtime string-domain witnesses - and no
	// executor evaluates it (remote_expr strips the witnesses before
	// dispatch). Query binding annotates admitted substring/left keys with a
	// string-domain source the DDL-side bind never carries, so clearing the
	// provenance pointer on both comparison copies keeps the structural
	// equality meaningful; a value-bearing difference can only come from the
	// validated tree, which the eligibility walk and the pending proto.Equal
	// still pin exactly. Value-bearing type metadata (width, scale, charset,
	// padding) stays exact.
	expr.PreparedNumeric = nil
	// Provenance and inferred nullability do not change expression values.
	// Keep value-bearing type metadata (width, scale, charset, padding) exact.
	expr.Typ.Table = ""
	expr.Typ.NotNullable = false
	// Compare fully bound values across catalog generations. Coercibility
	// affects binding, while the resolved identity/revision remains exact.
	expr.Typ.CollationCoercibility, expr.Typ.CollationCoercibilitySet = 0, false
	if col := expr.GetCol(); col != nil {
		if col.RelPos != tag {
			return false
		}
		col.RelPos = 0
		col.Name = ""
	}
	if lit := expr.GetLit(); lit != nil && types.T(expr.Typ.Id).IsInteger() {
		// string_source and literal_form are binder-syntax provenance
		// encodings: the query-side numeric binder records value ownership
		// explicitly on a folded negated constant while the DDL-side bind
		// keeps the legacy zero, and the spelling class mirrors how the
		// constant was authored. An integer literal's entire value is the
		// constant under the pinned type, so clearing the encodings on the
		// comparison copies cannot manufacture an equality the visibly
		// pinned trees do not already have. The executed plans are never
		// the normalized copies.
		lit.StringSource, lit.LiteralForm = 0, 0
	}
	if f := expr.GetF(); f != nil {
		if foldFunctionalIndexSignedConstant(expr) {
			return true
		}
		for _, arg := range f.Args {
			if !normalizeFunctionalExpression(arg, tag) {
				return false
			}
		}
	}
	return true
}

// foldFunctionalIndexSignedConstant rewrites, in the comparison copy only, the
// persisted binder's negative-integer-constant spelling `unary_minus(<lit>)`
// into the folded literal the query-side bind produces, so the structural
// comparison sees one spelling of the same constant. It admits exactly the
// DDL-admitted shape: one non-null i64 literal argument under the identical
// signed-integer result type. Every other unary-minus form is returned
// unchanged and keeps failing the pending equality instead of being coerced.
func foldFunctionalIndexSignedConstant(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "unary_minus" || len(fn.Args) != 1 {
		return false
	}
	arg, lit := fn.Args[0], fn.Args[0].GetLit()
	if lit == nil || lit.Isnull ||
		!types.T(arg.Typ.Id).IsSignedInt() || !types.T(expr.Typ.Id).IsSignedInt() ||
		arg.Typ.Id != expr.Typ.Id {
		return false
	}
	i64, ok := lit.GetValue().(*pb.Literal_I64Val)
	if !ok {
		return false
	}
	expr.Expr = &pb.Expr_Lit{Lit: &pb.Literal{
		Isnull: false,
		Value:  &pb.Literal_I64Val{I64Val: -i64.I64Val},
	}}
	return true
}

// functionalHiddenKeyByteStable states the match-time-only rule for adding
// the physical hidden-column equality: the hidden key must be an integer or
// VARCHAR plan type — a T_char key is padded at scan time per reader session
// by PAD_CHAR_TO_FULL_LENGTH — and the persisted expression must reference no
// T_char column, recursively. Every other DDL-admitted index keeps its
// base-row residual and matches through that residual alone, exactly like the
// CHAR-keyed JSON text indexes.
func functionalHiddenKeyByteStable(col *ColDef, cols []*ColDef) bool {
	if col == nil || col.GeneratedCol == nil || col.GeneratedCol.Expr == nil {
		return false
	}
	if col.Typ.Id != int32(types.T_varchar) && !types.T(col.Typ.Id).IsInteger() {
		return false
	}
	return !functionalExprReferencesCharColumn(col.GeneratedCol.Expr, cols, make(map[int32]bool))
}

// functionalExprReferencesCharColumn walks a bound plan expression (and its
// generated-column dependencies) for a T_char column reference. An unknown
// or cycle-blocked reference is treated conservatively as a CHAR reference.
func functionalExprReferencesCharColumn(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *pb.Expr_Col:
		pos := e.Col.ColPos
		if e.Col.RelPos != 0 || pos < 0 || int(pos) >= len(cols) || visiting[pos] {
			return true
		}
		col := cols[pos]
		if col == nil || col.Typ.Id == int32(types.T_char) {
			return true
		}
		if col.GeneratedCol != nil {
			visiting[pos] = true
			charRef := functionalExprReferencesCharColumn(col.GeneratedCol.Expr, cols, visiting)
			delete(visiting, pos)
			return charRef
		}
		return false
	case *pb.Expr_F:
		for _, arg := range e.F.Args {
			if functionalExprReferencesCharColumn(arg, cols, visiting) {
				return true
			}
		}
	}
	return false
}
