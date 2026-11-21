// Copyright 2026 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// MySQL types COALESCE(txt, bin) as binary, yet the value it returns keeps the
// charset of the argument it came from. Character-aware functions applied to
// it (CHAR_LENGTH, LEFT, REVERSE, ...) therefore see text for rows taken from
// a text argument. The value-level charset survives only inside one
// expression: pass-through string functions forward it, while storage, casts
// and materialized derived tables do not.
//
// The executor has no value-level charset. Instead, when a statement contains
// such a mixed-domain COALESCE, the planner distributes the consumer over the
// COALESCE arguments:
//
//	char_length(coalesce(a, b)) =>
//	    case when a is not null then char_length(a) else char_length(b) end
//
// so each branch is evaluated on its own statically typed argument. Plans
// without a mixed-domain COALESCE are not visited.

// textDomainConsumers see the text charset of a value.
var textDomainConsumers = map[string]bool{
	"char_length": true, "character_length": true,
	"left": true, "right": true, "substring": true, "substr": true, "mid": true,
	"reverse": true, "lpad": true, "rpad": true, "insert": true, "ord": true,
}

// textDomainForwarders return their first argument's value with its charset.
var textDomainForwarders = map[string]bool{
	"left": true, "right": true, "substring": true, "substr": true, "mid": true,
	"reverse": true, "insert": true, "repeat": true, "rpad": true, "quote": true,
	"upper": true, "ucase": true, "to_upper": true,
	"lower": true, "lcase": true, "to_lower": true,
	"trim": true, "ltrim": true, "rtrim": true,
}

func funcName(expr *Expr) string {
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		return strings.ToLower(fn.Func.ObjName)
	}
	return ""
}

// implicitStringSource looks through a binder-inserted conversion of a string
// operand to another string type, such as coalesce's cast(txt AS VARBINARY).
// A conversion of a binary operand (a materialized derived column) is kept: it
// is where MySQL drops the value-level charset.
func implicitStringSource(expr *Expr) *Expr {
	for {
		fn := expr.GetF()
		if !isBinderConversion(expr) {
			return expr
		}
		source := fn.Args[0]
		if source == nil || !isStaticTextString(source) || !types.T(expr.Typ.Id).IsMySQLString() {
			return expr
		}
		expr = source
	}
}

// isBinderConversion reports a cast the binder inserted to align types. A SQL
// CAST, and the explicit overload that declares a binary value, are not.
func isBinderConversion(expr *Expr) bool {
	fn := expr.GetF()
	return fn != nil && funcName(expr) == "cast" && !fn.SyntaxExplicitCast &&
		!isCastOverload(expr, 1) && len(fn.Args) == 2 && fn.Args[0] != nil
}

func isStaticTextString(expr *Expr) bool {
	return types.T(expr.Typ.Id).IsMySQLString() &&
		types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainText
}

func isStaticBinaryString(expr *Expr) bool {
	return types.T(expr.Typ.Id).IsMySQLString() &&
		types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainBinary
}

// isMixedDomainCoalesce reports a binary-typed COALESCE that can return a
// text value.
func isMixedDomainCoalesce(expr *Expr) bool {
	if funcName(expr) != "coalesce" || !isStaticBinaryString(expr) {
		return false
	}
	for _, arg := range expr.GetF().Args {
		if arg != nil && carriesTextValue(arg) {
			return true
		}
	}
	return false
}

// carriesTextValue reports whether expr's value can have a text charset.
func carriesTextValue(expr *Expr) bool {
	if isStaticTextString(implicitStringSource(expr)) {
		return true
	}
	return reachMixedDomainCoalesce(expr) != nil
}

// reachMixedDomainCoalesce follows the forwarded value argument from expr to a
// mixed-domain COALESCE, or returns nil.
func reachMixedDomainCoalesce(expr *Expr) *Expr {
	for expr != nil {
		if isMixedDomainCoalesce(expr) {
			return expr
		}
		next := forwardedArgument(expr)
		if next == nil {
			return nil
		}
		expr = next
	}
	return nil
}

// forwardedArgument returns the argument whose value (and charset) expr
// returns: the first argument of a forwarding function, or the value of
// NULLIF, which the binder spells case when a = b then null else a end.
func forwardedArgument(expr *Expr) *Expr {
	name := funcName(expr)
	args := expr.GetF().GetArgs()
	if textDomainForwarders[name] && len(args) > 0 {
		return args[0]
	}
	if name == "case" && len(args) == 3 && isNullLiteralExpr(stripImplicitCasts(args[1])) {
		if cond := args[0]; funcName(cond) == "=" && len(cond.GetF().Args) == 2 &&
			sameValueShape(cond.GetF().Args[0], args[2]) {
			return args[2]
		}
	}
	return nil
}

// sameValueShape compares two bindings of one SQL expression, ignoring binder
// conversions and type metadata such as nullability.
func sameValueShape(left, right *Expr) bool {
	left, right = stripConversions(left), stripConversions(right)
	if left == nil || right == nil {
		return left == right
	}
	if l, r := left.GetCol(), right.GetCol(); l != nil || r != nil {
		return l != nil && r != nil && l.RelPos == r.RelPos && l.ColPos == r.ColPos
	}
	if l, r := left.GetLit(), right.GetLit(); l != nil || r != nil {
		return l != nil && r != nil && l.String() == r.String()
	}
	lf, rf := left.GetF(), right.GetF()
	if lf == nil || rf == nil || funcName(left) != funcName(right) || len(lf.Args) != len(rf.Args) {
		return exprStructurallyEqual(left, right)
	}
	for i := range lf.Args {
		if lf.Args[i].GetT() != nil && rf.Args[i].GetT() != nil {
			continue
		}
		if !sameValueShape(lf.Args[i], rf.Args[i]) {
			return false
		}
	}
	return true
}

func exprStructurallyEqual(left, right *Expr) bool {
	return left != nil && right != nil && left.String() == right.String()
}

// textDomainRewriter distributes consumers of mixed-domain COALESCE values.
// A consumer may reach the COALESCE inside its own expression, or through a
// column of a Project below it (a derived table or flattened scalar
// subquery). In the latter case the Project gains one text-typed column, the
// value restricted to rows whose selected argument was text.
type textDomainRewriter struct {
	ctx      context.Context
	projects map[int32]*Node
	textCols map[[2]int32]*Expr
}

// distributeTextDomain rewrites every consumer of a mixed-domain COALESCE in
// expr. Expressions with volatile COALESCE arguments are left unchanged, since
// distribution evaluates an argument in both its test and its branch.
func (r *textDomainRewriter) distributeTextDomain(expr *Expr) (*Expr, error) {
	if expr == nil {
		return nil, nil
	}
	if textDomainConsumers[funcName(expr)] && len(expr.GetF().Args) > 0 {
		if target := reachMixedDomainCoalesce(expr.GetF().Args[0]); target != nil {
			if !containsVolatileFunction(target) {
				return r.distributeOverCoalesce(expr, target)
			}
		} else if column := reachColumn(expr.GetF().Args[0]); column != nil {
			if text, err := r.textColumn(column.GetCol(), 0); err != nil {
				return nil, err
			} else if text != nil {
				return r.distributeOverTextColumn(expr, column, text)
			}
		}
	}
	fn := expr.GetF()
	if fn == nil {
		return expr, nil
	}
	var args []*Expr
	for i, arg := range fn.Args {
		rewritten, err := r.distributeTextDomain(arg)
		if err != nil {
			return nil, err
		}
		if rewritten != arg {
			if args == nil {
				args = append([]*Expr(nil), fn.Args...)
			}
			args[i] = rewritten
		}
	}
	if args == nil {
		return expr, nil
	}
	copy := *expr
	copyFn := *fn
	copyFn.Args = args
	copy.Expr = &plan.Expr_F{F: &copyFn}
	return &copy, nil
}

// reachColumn follows forwarded arguments from expr to a column reference.
func reachColumn(expr *Expr) *Expr {
	for expr != nil {
		if expr.GetCol() != nil {
			return expr
		}
		expr = forwardedArgument(expr)
	}
	return nil
}

// distributeOverCoalesce rewrites consumer(..coalesce(a1..an)..) as
// case when a1 is not null then consumer(..a1..) ... else consumer(..an..) end.
// A text argument replaces the COALESCE without its binary conversion, so the
// consumer is rebound on the text value.
func (r *textDomainRewriter) distributeOverCoalesce(consumer, target *Expr) (*Expr, error) {
	coalesceArgs := target.GetF().Args
	caseArgs := make([]*Expr, 0, 2*len(coalesceArgs)-1)
	for i, arg := range coalesceArgs {
		value := implicitStringSource(arg)
		if !isStaticTextString(value) {
			value = arg
		}
		branch, err := substituteAndRebind(r.ctx, consumer, target, value)
		if err != nil {
			return nil, err
		}
		if branch, err = r.distributeTextDomain(branch); err != nil {
			return nil, err
		}
		if i == len(coalesceArgs)-1 {
			caseArgs = append(caseArgs, branch)
			break
		}
		test, err := BindFuncExprImplByPlanExpr(r.ctx, "isnotnull", []*Expr{DeepCopyExpr(arg)})
		if err != nil {
			return nil, err
		}
		caseArgs = append(caseArgs, test, branch)
	}
	return bindDistributedCase(r.ctx, consumer, caseArgs)
}

// distributeOverTextColumn rewrites consumer(..x..) as
// case when xt is not null then consumer(..xt..) else consumer(..x..) end,
// where xt is x restricted to rows whose selected value is text.
func (r *textDomainRewriter) distributeOverTextColumn(consumer, column, text *Expr) (*Expr, error) {
	branch, err := substituteAndRebind(r.ctx, consumer, column, text)
	if err != nil {
		return nil, err
	}
	test, err := BindFuncExprImplByPlanExpr(r.ctx, "isnotnull", []*Expr{DeepCopyExpr(text)})
	if err != nil {
		return nil, err
	}
	return bindDistributedCase(r.ctx, consumer, []*Expr{test, branch, DeepCopyExpr(consumer)})
}

func bindDistributedCase(ctx context.Context, consumer *Expr, caseArgs []*Expr) (*Expr, error) {
	result, err := BindFuncExprImplByPlanExpr(ctx, "case", caseArgs)
	if err != nil {
		return nil, err
	}
	if result.Typ.Id != consumer.Typ.Id {
		return consumer, nil
	}
	result.Typ = consumer.Typ
	return result, nil
}

// textColumn returns a reference to the text-restricted value of column,
// adding it to the defining Project on first use, or nil when the column
// cannot carry a text value.
func (r *textDomainRewriter) textColumn(col *plan.ColRef, depth int) (*Expr, error) {
	if col == nil || depth > 8 {
		return nil, nil
	}
	key := [2]int32{col.RelPos, col.ColPos}
	if text, ok := r.textCols[key]; ok {
		return text, nil
	}
	r.textCols[key] = nil
	node := r.projects[col.RelPos]
	if node == nil || col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
		return nil, nil
	}
	definition := node.ProjectList[col.ColPos]
	var text *Expr
	if inner := definition.GetCol(); inner != nil {
		deeper, err := r.textColumn(inner, depth+1)
		if err != nil || deeper == nil {
			return nil, err
		}
		text = deeper
	} else {
		target := reachMixedDomainCoalesce(definition)
		if target == nil || containsVolatileFunction(target) {
			return nil, nil
		}
		restricted, err := textOnlyCoalesce(r.ctx, target)
		if err != nil {
			return nil, err
		}
		if text, err = substituteAndRebind(r.ctx, definition, target, restricted); err != nil {
			return nil, err
		}
		if !isStaticTextString(text) {
			return nil, nil
		}
	}
	node.ProjectList = append(node.ProjectList, text)
	ref := &Expr{
		Typ: text.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: col.RelPos,
			ColPos: int32(len(node.ProjectList) - 1),
			Name:   col.Name + "_text",
		}},
	}
	r.textCols[key] = ref
	return ref, nil
}

// textOnlyCoalesce is the COALESCE's value when its selected argument is text
// and NULL otherwise:
// case when a1 is not null then t1 ... else tn end, ti = ai or NULL.
func textOnlyCoalesce(ctx context.Context, target *Expr) (*Expr, error) {
	args := target.GetF().Args
	caseArgs := make([]*Expr, 0, 2*len(args)-1)
	for i, arg := range args {
		value := implicitStringSource(arg)
		if isStaticTextString(value) {
			value = DeepCopyExpr(value)
		} else {
			value = makePlan2NullConstExprWithType()
		}
		if i == len(args)-1 {
			caseArgs = append(caseArgs, value)
			break
		}
		test, err := BindFuncExprImplByPlanExpr(ctx, "isnotnull", []*Expr{DeepCopyExpr(arg)})
		if err != nil {
			return nil, err
		}
		caseArgs = append(caseArgs, test, value)
	}
	return BindFuncExprImplByPlanExpr(ctx, "case", caseArgs)
}

// substituteAndRebind replaces target with value inside expr and rebinds each
// function on the path, so overloads are chosen for the substituted type.
func substituteAndRebind(ctx context.Context, expr, target, value *Expr) (*Expr, error) {
	if exprStructurallyEqual(expr, target) {
		return DeepCopyExpr(value), nil
	}
	fn := expr.GetF()
	if fn == nil {
		return DeepCopyExpr(expr), nil
	}
	changed := false
	args := make([]*Expr, len(fn.Args))
	for i, arg := range fn.Args {
		rewritten, err := substituteAndRebind(ctx, arg, target, value)
		if err != nil {
			return nil, err
		}
		args[i] = rewritten
		changed = changed || !exprStructurallyEqual(rewritten, arg)
	}
	if !changed {
		return DeepCopyExpr(expr), nil
	}
	if isBinderConversion(expr) {
		// A binder conversion of the substituted operand is chosen again by
		// rebinding its consumer for the operand's own type.
		return args[0], nil
	}
	for i, arg := range args {
		if !exprStructurallyEqual(arg, fn.Args[i]) {
			continue
		}
		args[i] = fn.Args[i]
		if source := stripImplicitCasts(arg); isNullLiteralExpr(source) {
			// A typed NULL takes its type from the rebound function.
			args[i] = source
		}
	}
	return BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args)
}

// stripConversions removes every cast that is not SQL CAST syntax, for
// comparing two bindings of one expression.
func stripConversions(expr *Expr) *Expr {
	for expr != nil {
		fn := expr.GetF()
		if fn == nil || funcName(expr) != "cast" || fn.SyntaxExplicitCast || len(fn.Args) == 0 {
			return expr
		}
		expr = fn.Args[0]
	}
	return expr
}

// stripImplicitCasts removes binder conversions from expr.
func stripImplicitCasts(expr *Expr) *Expr {
	for expr != nil {
		fn := expr.GetF()
		if !isBinderConversion(expr) {
			return expr
		}
		expr = fn.Args[0]
	}
	return expr
}

// distributeTextDomainInQuery applies the rewrite to the expressions of every
// node. It runs only for statements that bound a mixed-domain COALESCE.
func (builder *QueryBuilder) distributeTextDomainInQuery() error {
	if !builder.mixedDomainCoalesce {
		return nil
	}
	r := &textDomainRewriter{
		ctx:      builder.GetContext(),
		projects: make(map[int32]*Node),
		textCols: make(map[[2]int32]*Expr),
	}
	positional := make(map[int32]bool)
	for _, node := range builder.qry.Nodes {
		if node == nil {
			continue
		}
		switch node.NodeType {
		case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
			plan.Node_MINUS, plan.Node_MINUS_ALL:
			// Set operations read their children's columns by position.
			for _, child := range node.Children {
				positional[child] = true
			}
		}
	}
	for _, node := range builder.qry.Nodes {
		if node != nil && node.NodeType == plan.Node_PROJECT && len(node.BindingTags) > 0 &&
			!positional[node.NodeId] {
			r.projects[node.BindingTags[0]] = node
		}
	}
	rewrite := func(list []*Expr) error {
		for i, expr := range list {
			rewritten, err := r.distributeTextDomain(expr)
			if err != nil {
				return err
			}
			list[i] = rewritten
		}
		return nil
	}
	for _, node := range builder.qry.Nodes {
		if node == nil {
			continue
		}
		for _, list := range [][]*Expr{node.ProjectList, node.FilterList, node.OnList, node.AggList, node.GroupBy} {
			if err := rewrite(list); err != nil {
				return err
			}
		}
		for _, spec := range node.OrderBy {
			if spec == nil {
				continue
			}
			rewritten, err := r.distributeTextDomain(spec.Expr)
			if err != nil {
				return err
			}
			spec.Expr = rewritten
		}
	}
	return nil
}
