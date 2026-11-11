// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Expression provenance belongs to plan.Type, not to the persisted runtime
// Type. Resolving an expression must never upgrade its source table's identity.
type expressionCollation struct {
	charset, version, rank uint32
	conflict               bool
}

func (b *baseBinder) bindCollateExpr(ast *tree.CollateExpr, depth int32) (*Expr, error) {
	ctx := b.GetContext()
	definition, known := collation.Lookup(ast.Collation)
	if !known {
		return nil, moerr.NewUnknownCollation(ctx, ast.Collation)
	}
	identity, admitted := collation.ResolveSQL(ast.Collation)
	version := uint32(collation.RevisionLegacy)
	if definition.Semantics == collation.UCA400 {
		identity, admitted, version = definition.Identity, true, uint32(collation.RevisionV1)
	}
	if !admitted {
		return nil, moerr.NewNotSupportedf(ctx, "collation '%s' is disabled", ast.Collation)
	}
	if err := collation.RequireLegacy(uint32(identity), version, 0); err != nil {
		return nil, err
	}
	expr, err := func() (*Expr, error) {
		// Validate COLLATE against its operand's source charset. VALUES and
		// assignments apply the destination conversion after this expression;
		// using their type hint here can invert binary literal admission.
		previous := b.suppressDefaultValueBindType
		b.suppressDefaultValueBindType = true
		defer func() { b.suppressDefaultValueBindType = previous }()
		return b.impl.BindExpr(ast.Expr, depth, false)
	}()
	if err != nil {
		return nil, err
	}
	source := makeTypeByPlan2Expr(expr)
	_, parameter := unwrapParenExpr(ast.Expr).(*tree.ParamExpr)
	if parameter {
		// COLLATE fixes a marker's text domain across EXECUTE type changes,
		// including binary protocol values and NULL. A binary literal has no
		// such parameter constraint and remains subject to charset validation.
		source.Oid, source.Charset = types.T_varchar, types.CharsetUTF8
	}
	if (source.Oid.IsMySQLString() && source.Charset == types.CharsetBinary) || (!parameter && isNullLiteralExpr(expr)) {
		if definition.Charset != collation.CharsetBinary {
			return nil, moerr.NewCollationCharsetMismatch(ctx, definition.Name, "binary")
		}
	} else if definition.Charset == collation.CharsetBinary {
		return nil, moerr.NewCollationCharsetMismatch(ctx, definition.Name, "utf8mb4")
	} else if !parameter && source.Oid.IsMySQLString() {
		sourceDefinition, err := collation.EffectiveDefinition(uint32(source.Charset), uint32(source.CollationVersion))
		if err != nil {
			return nil, err
		}
		sourceCharset := sourceDefinition.Charset
		if sourceCharset == collation.CharsetUnspecified {
			// Historical text and ordinary literals carry identity zero, whose
			// SQL encoding remains utf8mb4 even without a comparison rule.
			sourceCharset = collation.CharsetUTF8MB4
		}
		// Validate the resolved domain, not the native spelling: historical
		// utf8/utf8mb3 aliases resolve to utf8mb4, while UCA keeps its native
		// identity and therefore still rejects cross-charset requests.
		targetDefinition, err := collation.EffectiveDefinition(uint32(identity), version)
		if err != nil {
			return nil, err
		}
		if sourceCharset != targetDefinition.Charset {
			return nil, moerr.NewCollationCharsetMismatch(ctx, definition.Name, sourceCharset.Name())
		}
	}
	target := expr.Typ
	if !types.T(target.Id).IsMySQLString() {
		textType := types.New(types.T_varchar, types.MaxVarcharLen, 0)
		target = makePlan2Type(&textType)
	}
	if parameter && (types.T(target.Id) == types.T_binary || types.T(target.Id) == types.T_varbinary) {
		target.Id = int32(types.T_varchar)
	}
	if identity == collation.BinaryIdentity {
		// 保留原生 byte 域及其未知宽度，不能改成 VARBINARY 的无效/默认最大宽度。
		if types.T(target.Id) != types.T_blob && types.T(target.Id) != types.T_binary {
			target.Id = int32(types.T_varbinary)
		}
		target.PadSpace = false
	}
	selected := expressionCollation{charset: uint32(identity), version: version, rank: 0}
	selected.apply(&target)
	// A real cast preserves the raw source column for projection, index safety,
	// and runtime parameter rebinding. Relabeling a shared node loses all three.
	return appendCastBeforeExpr(ctx, expr, target)
}

func (c expressionCollation) apply(typ *plan.Type) {
	typ.Charset, typ.CollationVersion = c.charset, c.version
	typ.CollationCoercibility, typ.CollationCoercibilitySet = c.rank, true
	typ.CollationMergeConflict = c.conflict
}

func collationForExpression(expr *Expr) (expressionCollation, bool) {
	if expr == nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return expressionCollation{}, false
	}
	c := expressionCollation{charset: expr.Typ.Charset, version: expr.Typ.CollationVersion,
		conflict: expr.Typ.CollationMergeConflict, rank: 5}
	if expr.Typ.CollationCoercibilitySet {
		c.rank = expr.Typ.CollationCoercibility
		return c, true
	}
	switch {
	case isNullLiteralExpr(expr):
		c.rank = 6
	case expr.GetCol() != nil || expr.GetCorr() != nil || expr.GetV() != nil:
		c.rank = 2
	case expr.GetSub() != nil && expr.GetSub().Typ == plan.SubqueryRef_SCALAR:
		// Old plans may omit provenance on a materialized scalar result.
		// Newly bound subqueries retain the projected source's exact rank.
		c.rank = 2
	case expr.GetLit() != nil || expr.GetP() != nil:
		c.rank = 4
	case expr.GetF() != nil && expr.GetF().Func != nil:
		fn := expr.GetF()
		name := strings.ToLower(fn.Func.ObjName)
		switch name {
		case "user", "current_user", "version", "database", "schema":
			c.rank = 3
		case "cast", "convert", "binary":
			c.rank = 2
		default:
			// Foreign/old plans may lack provenance. Read types only; never
			// evaluate a CASE branch while inferring its collation.
			if index, _ := preparedStringDomainSourceIndex(name, len(fn.Args)); index >= 0 {
				if child, ok := collationForExpression(fn.Args[index]); ok {
					c.rank = child.rank
				}
			}
		}
	}
	return c, true
}

func expressionCollationName(c expressionCollation) string {
	if c.charset == uint32(types.CharsetLegacy) {
		return "legacy"
	}
	d, err := collation.EffectiveDefinition(c.charset, c.version)
	if err != nil {
		return "unknown"
	}
	return d.Name
}

func illegalExpressionCollations(ctx context.Context, operation string, left, right expressionCollation) error {
	ranks := [...]string{"EXPLICIT", "NONE", "IMPLICIT", "SYSCONST", "COERCIBLE", "NUMERIC", "IGNORABLE"}
	return moerr.NewIllegalMixOfCollations(ctx, expressionCollationName(left), ranks[min(left.rank, 6)],
		expressionCollationName(right), ranks[min(right.rank, 6)], operation)
}

// At equal non-explicit coercibility, a text _bin collation wins over another
// collation of the same charset, even when their metadata revisions differ.
func sameCharsetTextBinWinner(left, right expressionCollation) (expressionCollation, bool) {
	bin, other := left, right
	if bin.charset != uint32(types.CharsetUTF8MB4Bin) {
		bin, other = right, left
	}
	if bin.charset != uint32(types.CharsetUTF8MB4Bin) || other.charset == bin.charset {
		return expressionCollation{}, false
	}
	binDefinition, binErr := collation.EffectiveDefinition(bin.charset, bin.version)
	otherDefinition, otherErr := collation.EffectiveDefinition(other.charset, other.version)
	if binErr != nil || otherErr != nil || binDefinition.Charset != otherDefinition.Charset ||
		binDefinition.Charset != collation.CharsetUTF8MB4 {
		return expressionCollation{}, false
	}
	bin.conflict = left.conflict || right.conflict
	return bin, true
}

// Resolve the strongest rank before checking equal-rank conflicts. Otherwise
// adding a stronger third operand makes a left-to-right resolver order-dependent.
func resolveExpressionCollation(ctx context.Context, operation string, candidates []expressionCollation) (expressionCollation, bool, error) {
	if len(candidates) == 0 {
		return expressionCollation{}, false, nil
	}
	winningRank := uint32(6)
	active := false
	for _, candidate := range candidates {
		winningRank = min(winningRank, candidate.rank)
		active = active || candidate.rank == 0 || candidate.version != 0 || candidate.conflict
	}
	var chosen expressionCollation
	chosenSet := false
	if winningRank != 0 {
		// Choose an equal-rank text _bin candidate before comparing CI peers.
		// Otherwise a revision mismatch between two CI inputs can make the
		// result depend on whether the _bin branch appears first or last.
		for _, candidate := range candidates {
			if candidate.rank == winningRank && candidate.charset == uint32(types.CharsetUTF8MB4Bin) && candidate.version == 0 {
				chosen, chosenSet = candidate, true
				break
			}
		}
	}
	for _, candidate := range candidates {
		if candidate.rank != winningRank {
			continue
		}
		if !chosenSet {
			chosen, chosenSet = candidate, true
			continue
		}
		if chosen.charset == candidate.charset && chosen.version == candidate.version {
			chosen.conflict = chosen.conflict || candidate.conflict
			continue
		}
		if winningRank != 0 {
			if bin, ok := sameCharsetTextBinWinner(chosen, candidate); ok {
				chosen = bin
				continue
			}
		}
		if winningRank == 0 || chosen.version != candidate.version {
			return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, candidate)
		}
		// Equal implicit ranks in one charset prefer the text _bin rule.
		// Binary bytes keep their own NO PAD domain, distinct from text _bin.
		if chosen.charset == uint32(types.CharsetBinary) || candidate.charset == uint32(types.CharsetBinary) {
			chosen.charset, chosen.version = uint32(types.CharsetBinary), 0
		} else if chosen.charset == uint32(types.CharsetUTF8MB4Bin) || candidate.charset == uint32(types.CharsetUTF8MB4Bin) {
			chosen.charset = uint32(types.CharsetUTF8MB4Bin)
		} else if types.IsUnicodeCollation(uint8(chosen.charset)) && types.IsUnicodeCollation(uint8(candidate.charset)) {
			// utf8mb4 is the repertoire superset for UCA400, not an alias.
			chosen.charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		} else if active {
			return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, candidate)
		}
	}
	if chosen.conflict {
		return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, chosen)
	}
	return chosen, active, nil
}

func expressionCollationIndexes(name string, arity int) []int {
	if indexes := padSpaceValueArgumentIndexes(name, arity); len(indexes) > 0 {
		return indexes
	}
	switch name {
	case "bit_and", "bit_or", "bit_xor":
		if arity == 1 {
			return []int{0}
		}
	case "name_const":
		if arity == 2 {
			return []int{1}
		}
	case "regexp_like":
		// MySQL resolves the comparison collation of REGEXP_LIKE from the
		// subject and pattern only; the match_type argument never
		// participates, so a COLLATE on it cannot conflict. LIKE and ILIKE
		// differ here: MySQL aggregates every LIKE argument, ESCAPE included.
		if arity >= 2 {
			return []int{0, 1}
		}
	case "=", "<=>", "<", "<=", ">", ">=", "<>", "!=", "between", "in", "not_in",
		"concat", "concat_ws", "nullif", "union", "values",
		"reg_match", "not_reg_match", "find_in_set", "like", "ilike":
		indexes := make([]int, arity)
		for i := range indexes {
			indexes[i] = i
		}
		return indexes
	}
	if index, _ := preparedStringDomainSourceIndex(name, arity); index >= 0 {
		return []int{index}
	}
	return nil
}

// regexpBinaryDomainGuard names the regexp collation consumers whose
// compatibility check must still see an operand's original binary domain.
// Converting a binary operand to the selected text collation here would
// erase a _binary introducer before that check can reject the mix the way
// MySQL does (error 3995). LIKE and FIND_IN_SET are deliberately outside
// this guard: MySQL resolves their comparison collation from both operands,
// so an explicit COLLATE converts a binary operand and folds, while an
// unconverted binary operand keeps the executors' byte-wise routing.
func regexpBinaryDomainGuard(name string) bool {
	switch name {
	case "reg_match", "not_reg_match", "regexp_like":
		return true
	}
	return false
}

func hasStaticBinaryStringOperand(args []*Expr, indexes []int) bool {
	for _, index := range indexes {
		if args[index] != nil &&
			types.StaticStringDomain(makeTypeByPlan2Expr(args[index])) == types.StringDomainBinary {
			return true
		}
	}
	return false
}

// patternSelectedCollationConsumer names the matchers whose executor reads
// the comparison collation from one operand (the LIKE/REGEXP subject, the
// FIND_IN_SET list). They need the selected collation delivered even when
// legacy conversion stays inactive. ILIKE folds on every string domain by
// definition, so it is deliberately absent.
func patternSelectedCollationConsumer(name string) bool {
	switch name {
	case "like", "reg_match", "not_reg_match", "regexp_like", "find_in_set":
		return true
	}
	return false
}

// patternBinaryCollationAdmission names the matchers this issue aggregated
// into collation resolution. Before that, their executors owned the
// binary-versus-text mix: LIKE/ILIKE/FIND_IN_SET routed byte-wise through
// the binary operand's string domain, and the regexp functions rejected the
// mix with error 3995. Equality and the other aggregated functions keep the
// generic resolution, including its rejection of the same mix.
func patternBinaryCollationAdmission(name string) bool {
	switch name {
	case "like", "ilike", "find_in_set", "reg_match", "not_reg_match", "regexp_like":
		return true
	}
	return false
}

// pairwiseBinaryTieWinner replays MySQL's pairwise aggregation for the one
// mix the generic resolver rejects but the pattern executors always owned:
// at equal non-explicit coercibility a binary operand absorbs every operand
// it meets, so a case-insensitive column matched against CAST(x AS BINARY)
// resolves to the binary character set instead of raising error 1267. A
// text/text conflict or a carried merge conflict keeps the generic verdict,
// and an explicit COLLATE wins before any tie matters.
func pairwiseBinaryTieWinner(candidates []expressionCollation) (expressionCollation, bool) {
	winningRank := uint32(6)
	for _, candidate := range candidates {
		winningRank = min(winningRank, candidate.rank)
	}
	if winningRank == 0 {
		return expressionCollation{}, false
	}
	var acc expressionCollation
	accSet, sawBinary := false, false
	for _, candidate := range candidates {
		if candidate.rank != winningRank {
			continue
		}
		if candidate.conflict {
			return expressionCollation{}, false
		}
		if !accSet {
			acc, accSet = candidate, true
			sawBinary = candidate.charset == uint32(types.CharsetBinary)
			continue
		}
		if acc.charset == uint32(types.CharsetBinary) || candidate.charset == uint32(types.CharsetBinary) {
			acc = expressionCollation{charset: uint32(types.CharsetBinary), rank: winningRank}
			sawBinary = true
			continue
		}
		if acc.charset == candidate.charset && acc.version == candidate.version {
			continue
		}
		return expressionCollation{}, false
	}
	if !sawBinary {
		return expressionCollation{}, false
	}
	return acc, true
}

// stringOperandsMatchSelectedCollation reports whether every aggregated
// string operand already carries the selected charset and version.
// Coercibility alone does not require delivery: the matchers read only the
// comparison charset, and keeping the legacy identities avoids no-op casts
// on the common column-versus-literal shapes.
func stringOperandsMatchSelectedCollation(args []*Expr, indexes []int, selected expressionCollation) bool {
	for _, index := range indexes {
		expr := args[index]
		if expr == nil || !types.T(expr.Typ.Id).IsMySQLString() {
			continue
		}
		if expr.Typ.Charset != selected.charset || expr.Typ.CollationVersion != selected.version {
			return false
		}
	}
	return true
}

func expressionCollationCandidates(args []*Expr, indexes []int) []expressionCollation {
	var candidates []expressionCollation
	var collect func(*Expr)
	collect = func(expr *Expr) {
		if expr == nil {
			return
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				collect(item)
			}
		} else if candidate, ok := collationForExpression(expr); ok {
			candidates = append(candidates, candidate)
		}
	}
	for _, index := range indexes {
		collect(args[index])
	}
	return candidates
}

func normalizeExpressionCollations(ctx context.Context, name string, args []*Expr) ([]*Expr, error) {
	// Tuple IN resolves each position independently in handleTupleIn.
	if (name == "in" || name == "not_in") && len(args) > 0 && args[0].GetList() != nil {
		return args, nil
	}
	if (name == "in" || name == "not_in") && len(args) == 2 && args[0] != nil && makeTypeByPlan2Expr(args[0]).IsNumeric() {
		// Numeric IN owns the comparison domain. Its existing typed-list and
		// mixed-comparison paths must decide casts before considering text.
		return args, nil
	}
	if name == "between" {
		if len(args) == 3 && args[0] != nil && makeTypeByPlan2Expr(args[0]).IsNumeric() {
			// Leave the existing overload and column-preserving cast rules in
			// charge; only string-domain validation is inapplicable here.
			return args, nil
		}
		var err error
		args, err = numericBetweenCollationArguments(ctx, args)
		if err != nil {
			return nil, err
		}
	}
	indexes := expressionCollationIndexes(name, len(args))
	candidates := expressionCollationCandidates(args, indexes)
	selected, active, err := resolveExpressionCollation(ctx, name, candidates)
	if err != nil && patternBinaryCollationAdmission(name) {
		if restored, ok := pairwiseBinaryTieWinner(candidates); ok {
			// Restore the routing these matchers had before they joined
			// collation resolution: the binary operand wins the tie, and the
			// unchanged operands reach the executor, which compares byte-wise
			// (LIKE/ILIKE/FIND_IN_SET) or rejects the mix (error 3995).
			selected, active, err = restored, false, nil
		}
	}
	if err != nil {
		return args, err
	}
	if regexpBinaryDomainGuard(name) && hasStaticBinaryStringOperand(args, indexes) {
		// A binary string operand must reach the regexp compatibility check
		// with its original domain. Converting it to the selected collation
		// here would erase a _binary introducer before the check can reject
		// the mix the way MySQL does (error 3995), regardless of whether any
		// other operand activates conversion.
		return args, nil
	}
	if !active {
		// Pattern matchers read the comparison collation from a single
		// operand, so the selected collation must still be delivered when
		// implicit version-zero operands disagree with it (for example a
		// text _bin column matched against a case-insensitive column).
		// Every other function keeps its legacy operand identities here,
		// operands already carrying the selection need no conversion, and a
		// binary selection already routes every matcher byte-wise through
		// the binary operand's own string domain.
		if selected.charset == uint32(types.CharsetBinary) ||
			!patternSelectedCollationConsumer(name) ||
			stringOperandsMatchSelectedCollation(args, indexes, selected) {
			return args, nil
		}
	}
	result := append([]*Expr(nil), args...)
	var convert func(*Expr) (*Expr, error)
	convert = func(expr *Expr) (*Expr, error) {
		if expr == nil {
			return expr, nil
		}
		if list := expr.GetList(); list != nil {
			copy := *expr
			items := make([]*Expr, len(list.List))
			for i, item := range list.List {
				items[i], err = convert(item)
				if err != nil {
					return nil, err
				}
			}
			copy.Expr = &plan.Expr_List{List: &plan.ExprList{List: items}}
			return &copy, nil
		}
		if !types.T(expr.Typ.Id).IsMySQLString() {
			return expr, nil
		}
		target := expr.Typ
		selected.apply(&target)
		if selected.charset == uint32(types.CharsetBinary) {
			target.PadSpace = false
		} else if types.T(target.Id) == types.T_binary || types.T(target.Id) == types.T_varbinary {
			// The losing byte operand must become a text vector, not just carry
			// text metadata on a physical type whose comparator ignores it.
			target.Id = int32(types.T_varchar)
		}
		if target.Id == expr.Typ.Id && target.SameCollation(expr.Typ) {
			return expr, nil
		}
		if target.Id == expr.Typ.Id && target.Charset == expr.Typ.Charset && target.CollationVersion == expr.Typ.CollationVersion {
			copy := *expr
			copy.Typ = target
			return &copy, nil
		}
		if types.IsCaseInsensitiveCollation(uint8(selected.charset)) {
			clearConvertedBinaryLiteralForm(expr, selected.rank)
		}
		return appendCastBeforeExpr(ctx, expr, target)
	}
	for _, index := range indexes {
		result[index], err = convert(args[index])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// clearConvertedBinaryLiteralForm drops the binary string provenance of a
// constant binary literal whose value the conversion cast reinterprets under
// a case-insensitive selected collation, but only when that collation won by
// strictly stronger coercibility than the literal's own (an explicit COLLATE,
// or a winning implicit one such as a column's). MySQL keeps a binary string
// byte-wise at equal coercibility, so a coercible-versus-coercible tie (for
// example 'abc' LIKE _binary 'ABC%' or FIND_IN_SET('A', _binary 'a,b')) must
// retain the literal's binary domain and stay byte-wise. When the strict win
// holds, the converted value compares under the selected collation instead of
// routing the pattern matchers byte-wise (issue #31175). The cast target type
// is unchanged; only the domain the literal claims for itself follows the
// conversion.
func clearConvertedBinaryLiteralForm(expr *Expr, selectedRank uint32) {
	literal := expr.GetLit()
	if literal == nil {
		return
	}
	switch literal.LiteralForm {
	case plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
		plan.StringLiteralForm_STRING_LITERAL_HEX,
		plan.StringLiteralForm_STRING_LITERAL_BIT:
	default:
		return
	}
	candidate, ok := collationForExpression(expr)
	if !ok || selectedRank >= candidate.rank {
		return
	}
	literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_TEXT
}

func numericBetweenCollationArguments(ctx context.Context, args []*Expr) ([]*Expr, error) {
	if len(args) != 3 {
		return args, nil
	}
	var inputTypes [3]types.Type
	numericIndex, hasText := -1, false
	for i, arg := range args {
		if arg == nil {
			return args, nil
		}
		inputTypes[i] = makeTypeByPlan2Expr(arg)
		switch {
		case inputTypes[i].IsNumeric():
			numericIndex = i
		case inputTypes[i].Oid.IsMySQLString():
			hasText = true
		case inputTypes[i].Oid == types.T_any && isNullLiteralExpr(arg):
		default:
			return args, nil
		}
	}
	if numericIndex < 0 || !hasText {
		return args, nil
	}
	// Reuse the registered comparison casts with a numeric peer first. With
	// a text left operand, the ordinary pairwise check could otherwise reject
	// its text/text pair before seeing the numeric bound. Only lookup types
	// are permuted; executable operands and their cast targets keep SQL order.
	inputTypes[0], inputTypes[numericIndex] = inputTypes[numericIndex], inputTypes[0]
	resolved, err := function.GetFunctionByName(ctx, "between", inputTypes[:])
	if err != nil {
		return args, nil
	}
	targets, cast := resolved.ShouldDoImplicitTypeCast()
	if !cast || len(targets) != len(args) {
		return args, nil
	}
	for _, target := range targets {
		if !target.IsNumeric() {
			return args, nil
		}
	}
	targets[0], targets[numericIndex] = targets[numericIndex], targets[0]
	converted := make([]*Expr, len(args))
	for i, arg := range args {
		converted[i], err = appendCastBeforeExpr(ctx, arg, makePlan2Type(&targets[i]))
		if err != nil {
			return nil, err
		}
	}
	return converted, nil
}

func setExpressionCollation(ctx context.Context, name string, args []*Expr, typ *plan.Type) error {
	if !types.T(typ.Id).IsMySQLString() {
		return nil
	}
	if name == "cast" && len(args) == 2 && args[1].GetT() != nil {
		if args[1].Typ.CollationCoercibilitySet {
			selected, _ := collationForExpression(args[1])
			selected.apply(typ)
		}
		return nil
	}
	if name == "binary" {
		// BINARY is a cast operator, so its result keeps MySQL's IMPLICIT
		// coercibility (2) rather than inheriting an argument's text collation
		// or falling to the default rank: either would let a case-insensitive
		// column strictly outrank it and convert the binary operand back to
		// text (issue #31175). Constant folding preserves this Typ, so the
		// stamp also covers a folded BINARY literal.
		if !typ.CollationCoercibilitySet {
			typ.CollationCoercibility, typ.CollationCoercibilitySet = 2, true
		}
		return nil
	}
	indexes := expressionCollationIndexes(name, len(args))
	if len(indexes) == 0 {
		return nil
	}
	selected, active, err := resolveExpressionCollation(ctx, name, expressionCollationCandidates(args, indexes))
	if err != nil {
		return err
	}
	if active {
		selected.apply(typ)
	} else if name != "union" && name != "values" && len(expressionCollationCandidates(args, indexes)) > 0 {
		// Keep historical byte semantics, while retaining the expression's
		// actual coercibility for a later native or explicit consumer.
		typ.CollationCoercibility, typ.CollationCoercibilitySet = selected.rank, true
	}
	return nil
}
