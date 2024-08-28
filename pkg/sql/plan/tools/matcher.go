// Copyright 2024 Matrix Origin
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

package tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

var _ Matcher = new(NodeMatcher)
var _ Matcher = new(TableScanMatcher)
var _ Matcher = new(AliasMatcher)
var _ Matcher = new(SymbolsMatcher)
var _ Matcher = new(AssignedSymbolsMatcher)
var _ Matcher = new(OutputMatcher)
var _ Matcher = new(JoinMatcher)

var _ RValueMatcher = new(ColumnRef)
var _ RValueMatcher = new(ExprMatcher)
var _ RValueMatcher = new(AggrFuncMatcher)

type NodeMatcher struct {
	NodeType plan2.Node_NodeType
	Not      bool
}

func (matcher *NodeMatcher) String() string {
	if matcher.Not {
		return "NOT(" + matcher.NodeType.String() + ")"
	}
	return matcher.NodeType.String()
}

func (matcher *NodeMatcher) SimpleMatch(node *plan2.Node) bool {
	if matcher.Not {
		return node.NodeType != matcher.NodeType
	}
	return node.NodeType == matcher.NodeType
}

func (matcher *NodeMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}
	return Matched(), nil
}

type TableScanMatcher struct {
	TableName  string
	Constraint UnorderedMap[string, *Domain]
}

func (matcher *TableScanMatcher) String() string {
	return fmt.Sprintf("TableScanMatcher{table:%s}", matcher.TableName)
}

func (matcher *TableScanMatcher) SimpleMatch(node *plan.Node) bool {
	return node.NodeType == plan2.Node_TABLE_SCAN
}

func (matcher *TableScanMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}

	if strings.Compare(matcher.TableName, node.TableDef.Name) != 0 {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}
	if matcher.Constraint != nil {
		return nil, moerr.NewInternalError(ctx, "add constraint check")
	}
	return Matched(), nil
}

type AliasMatcher struct {
	Alias   string
	Matcher RValueMatcher
}

func (matcher *AliasMatcher) String() string {
	if len(matcher.Alias) != 0 {
		return fmt.Sprintf("bind %s -> %s", matcher.Alias, matcher.Matcher.String())
	}
	return fmt.Sprintf("bind %s", matcher.Matcher)
}

func (matcher *AliasMatcher) SimpleMatch(node *plan.Node) bool {
	return true
}

func (matcher *AliasMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	varRef, err := matcher.Matcher.GetAssignedVar(node, aliases)
	if err != nil {
		return nil, err
	}
	if varRef != nil && len(matcher.Alias) != 0 {
		return MatchedWithAlias(matcher.Alias, varRef.Name), nil
	}
	return NewMatchResult(varRef != nil, nil), nil
}

type ColumnRef struct {
	TableName  string
	ColumnName string
}

func (matcher *ColumnRef) String() string {
	return fmt.Sprintf("Column %s:%s", matcher.TableName, matcher.ColumnName)
}

func (matcher *ColumnRef) GetAssignedVar(node *plan2.Node, aliases UnorderedMap[string, string]) (*VarRef, error) {
	if node.NodeType != plan2.Node_TABLE_SCAN {
		return nil, nil
	}

	if matcher.TableName != node.TableDef.Name {
		return nil, nil
	}

	for _, col := range node.TableDef.Cols {
		if col.Name == matcher.ColumnName {
			return &VarRef{
				Name: col.Name,
				Type: col.Typ,
			}, nil
		}
	}
	return nil, nil
}

type GetFunc func(*plan.QueryBuilder, *plan2.Node) []VarRef

type SymbolsMatcher struct {
	GetFunc         GetFunc
	ExpectedAliases []string
}

func (matcher *SymbolsMatcher) String() string {
	return fmt.Sprintf("SymbolsMatcher{expectedAliases:%v}", matcher.ExpectedAliases)
}

func (matcher *SymbolsMatcher) SimpleMatch(node *plan.Node) bool {
	matcher.GetFunc(nil, node)
	return true
}

func (matcher *SymbolsMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return FailMatched(), nil
	}
	realCols := matcher.GetFunc(nil, node)
	realNames := make([]string, len(realCols))
	for i, name := range realCols {
		realNames[i] = name.Name
	}

	eNames := make([]string, 0)
	for _, alias := range matcher.ExpectedAliases {
		if ok, v := aliases.Find(alias); ok {
			eNames = append(eNames, v)
		}
	}

	return NewMatchResult(StringsEqual(realNames, eNames), nil), nil
}

type AssignedSymbolsMatcher struct {
	GetFunc          GetFunc
	ExpectedMatchers []RValueMatcher
}

func (matcher *AssignedSymbolsMatcher) String() string {
	return fmt.Sprintf("AssignedSymbolsMatcher{expectedMatchers:%v}", matcher.ExpectedMatchers)
}

func (matcher *AssignedSymbolsMatcher) SimpleMatch(node *plan.Node) bool {
	matcher.GetFunc(nil, node)
	return true
}

func (matcher *AssignedSymbolsMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return FailMatched(), nil
	}
	realCols := matcher.GetFunc(nil, node)

	expectCols := make([]VarRef, 0)
	for _, eMatcher := range matcher.ExpectedMatchers {
		varRef, err := eMatcher.GetAssignedVar(node, aliases)
		if err != nil {
			return nil, err
		}
		if varRef == nil {
			return FailMatched(), nil
		}
		expectCols = append(expectCols, *varRef)
	}

	return NewMatchResult(VarRefsEqual(realCols, expectCols), nil), nil
}

type OutputMatcher struct {
	Aliases []string
}

func (matcher *OutputMatcher) String() string {
	return fmt.Sprintf("OutputMatcher{outputs%v}", matcher.Aliases)
}

func (matcher *OutputMatcher) SimpleMatch(node *plan.Node) bool {
	return node.NodeType == plan2.Node_PROJECT
}

func (matcher *OutputMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	for _, alias := range matcher.Aliases {
		ok, ref := aliases.Find(alias)
		if !ok {
			return FailMatched(), nil
		}
		found := false
		for _, projExpr := range node.ProjectList {
			name := projExpr.GetCol().Name
			if strings.Contains(name, "(") {
				found = strings.HasPrefix(strings.ToLower(name), ref)
			} else {
				names := strings.Split(name, ".")
				if len(names) != 2 {
					return FailMatched(), moerr.NewInternalErrorf(ctx, "special name %s ", name)
				}
				colName := names[1]
				if ref == colName {
					found = true
					break
				}
			}
		}
		if !found {
			return FailMatched(), nil
		}
	}
	return Matched(), nil
}

type ExprMatcher struct {
	Sql  string
	Expr tree.Expr
}

func NewExprMatcher(sql string) *ExprMatcher {
	expr := parseSql(sql)
	return &ExprMatcher{
		Sql:  sql,
		Expr: expr,
	}
}

func (matcher *ExprMatcher) GetAssignedVar(node *plan2.Node, aliases UnorderedMap[string, string]) (*VarRef, error) {
	var res *VarRef
	checkedExprs := make([]*plan2.Expr, 0)
	for _, expr := range node.ProjectList {
		eChecker := &ExprChecker{
			Aliases: aliases,
		}
		check, err := eChecker.Check(matcher.Expr, expr)
		if err != nil {
			return nil, err
		}
		if check {
			if expr.GetCol() != nil {
				res = &VarRef{
					Name: strings.Split(expr.GetCol().Name, ".")[1],
					Type: expr.GetTyp(),
				}
			} else {
				res = &VarRef{
					Name: expr.String(),
					Type: expr.GetTyp(),
				}
			}

			checkedExprs = append(checkedExprs, expr)
		}
	}

	if len(checkedExprs) > 1 {
		return nil, moerr.NewInternalErrorf(context.Background(),
			"expr %s matches multiple assignments",
			matcher.Sql,
		)
	}

	return res, nil
}

func (matcher *ExprMatcher) String() string {
	return matcher.Sql
}

type JoinMatcher struct {
	JoinTyp    plan2.Node_JoinType
	OnCondsStr []string
	OnConds    []tree.Expr
	FiltersStr []string
	Filters    []tree.Expr
}

func NewJoinMatcher(joinType plan2.Node_JoinType, conds []string, filters []string) *JoinMatcher {
	ret := &JoinMatcher{
		JoinTyp:    joinType,
		OnCondsStr: conds,
		FiltersStr: filters,
	}
	for _, cond := range conds {
		ret.OnConds = append(ret.OnConds, parseSql(cond))
	}

	for _, filter := range filters {
		ret.Filters = append(ret.Filters, parseSql(filter))
	}
	return ret
}

func (matcher *JoinMatcher) SimpleMatch(node *plan2.Node) bool {
	return node.NodeType == plan2.Node_JOIN &&
		node.JoinType == matcher.JoinTyp
}

func (matcher *JoinMatcher) DeepMatch(ctx context.Context, node *plan2.Node, aliases UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return FailMatched(), nil
	}
	if len(matcher.OnConds) != len(node.OnList) {
		return FailMatched(), nil
	}

	if len(matcher.Filters) != len(node.FilterList) {
		return FailMatched(), nil
	}

	for i, cond := range matcher.OnConds {
		checker := ExprChecker{Aliases: aliases}
		ok, err := checker.Check(cond, node.OnList[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			return FailMatched(), nil
		}
	}

	for i, filter := range matcher.Filters {
		checker := ExprChecker{Aliases: aliases}
		ok, err := checker.Check(filter, node.OnList[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			return FailMatched(), nil
		}
	}
	return Matched(), nil
}

func (matcher *JoinMatcher) String() string {
	return fmt.Sprintf("JoinMatcher{%v}, OnConds:%v, Filters:%v",
		matcher.JoinTyp,
		matcher.OnCondsStr,
		matcher.FiltersStr,
	)
}

func parseSql(sql string) tree.Expr {
	exSql := "select " + sql
	one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, exSql, 1)
	if err != nil {
		panic(err)
	}
	expr := one.(*tree.Select).Select.(*tree.SelectClause).Exprs[0]
	return expr.Expr
}

type AggrFuncMatcher struct {
	Aggr    tree.Expr
	AggrStr string
}

func NewAggrFuncMatcher(s string) *AggrFuncMatcher {
	ret := &AggrFuncMatcher{
		Aggr:    parseSql(s),
		AggrStr: s,
	}

	return ret
}

func (matcher *AggrFuncMatcher) GetAssignedVar(node *plan2.Node, aliases UnorderedMap[string, string]) (*VarRef, error) {
	if node.NodeType != plan2.Node_AGG {
		return nil, nil
	}
	var res *VarRef

	for _, expr := range node.AggList {
		eChecker := &ExprChecker{
			Aliases: aliases,
		}
		check, err := eChecker.Check(matcher.Aggr, expr)
		if err != nil {
			return nil, err
		}
		if check {
			if expr.GetF() != nil {
				res = &VarRef{
					Name: expr.GetF().GetFunc().ObjName,
					Type: expr.GetTyp(),
				}
			} else {
				res = &VarRef{
					Name: expr.String(),
					Type: expr.GetTyp(),
				}
			}
		}
	}
	return res, nil
}

func (matcher *AggrFuncMatcher) String() string {
	return matcher.AggrStr
}

type ValuesMatcher struct {
}
