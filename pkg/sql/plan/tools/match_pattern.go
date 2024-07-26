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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TNode(nodeType plan2.Node_NodeType, children ...*MatchPattern) *MatchPattern {
	return TAny(children...).With(&NodeMatcher{NodeType: nodeType})
}

// TAny yields a pattern without designated node type
func TAny(children ...*MatchPattern) *MatchPattern {
	return &MatchPattern{
		Children: children,
	}
}

// TAnyTree denotes children tree matches the patterns
func TAnyTree(children ...*MatchPattern) *MatchPattern {
	return TAny(children...).MatchAnyTree()
}

func TAnyNot(nodeType plan2.Node_NodeType, children ...*MatchPattern) *MatchPattern {
	return TAny(children...).With(&NodeMatcher{NodeType: nodeType, Not: true})
}

func TTableScanWithoutColRef(tableName string) *MatchPattern {
	return TNode(plan2.Node_TABLE_SCAN).With(&TableScanMatcher{
		TableName: tableName,
	})
}

func TTableScan(tableName string, colRefs UnorderedMap[string, string]) *MatchPattern {
	ret := TTableScanWithoutColRef(tableName)
	return ret.AddColRefs(tableName, colRefs)
}

func TStrictTableScan(tableName string, colRefs UnorderedMap[string, string]) *MatchPattern {
	ret := TTableScan(tableName, colRefs)
	newColRes := make([]RValueMatcher, 0)
	for _, val := range colRefs {
		newColRes = append(newColRes, TColumnRef(tableName, val))
	}
	return ret.WithExactAssignedOutputs(newColRes)
}

func TColumnRef(tableName, colName string) RValueMatcher {
	return &ColumnRef{TableName: tableName, ColumnName: colName}
}

func TOutput(outputs []string, child *MatchPattern) *MatchPattern {
	res := TOutputWithoutOutputs(child)
	res.WithOutputs(outputs...)
	return res
}

func TOutputWithoutOutputs(child *MatchPattern) *MatchPattern {
	return TNode(plan2.Node_PROJECT, child)
}

func TStrictOutput(outputs []string, child *MatchPattern) *MatchPattern {
	ret := TOutput(outputs, child)
	return ret.WithExactOutputs(outputs...)
}

func TProjectWithoutAssignments(child *MatchPattern) *MatchPattern {
	return TNode(plan2.Node_PROJECT, child)
}

func TProject(assigns UnorderedMap[string, *ExprMatcher], child *MatchPattern) *MatchPattern {
	ret := TProjectWithoutAssignments(child)
	for k, matcher := range assigns {
		ret.WithAlias(k, matcher)
	}
	return ret
}

func TStrictProject(assigns UnorderedMap[string, *ExprMatcher], child *MatchPattern) *MatchPattern {
	ret := TProject(assigns, child)
	matchers := make([]RValueMatcher, 0)
	for _, matcher := range assigns {
		matchers = append(matchers, matcher)
	}

	return ret.WithExactAssignedOutputs(matchers).WithExactAssignments(matchers)
}

func TExpr(e string) *ExprMatcher {
	return NewExprMatcher(e)
}

func TJoin(joinTyp plan2.Node_JoinType,
	onConds []string,
	filters []string,
	left, right *MatchPattern) *MatchPattern {
	return TNode(plan2.Node_JOIN, left, right).With(
		NewJoinMatcher(joinTyp, onConds, filters))
}

func TAggr(aggrs UnorderedMap[string, *AggrFuncMatcher], children ...*MatchPattern) *MatchPattern {
	ret := TNode(plan2.Node_AGG, children...)

	for k, matcher := range aggrs {
		ret.WithAlias(k, matcher)
	}
	return ret
}

func (pattern *MatchPattern) With(matcher Matcher) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers, matcher)
	return pattern
}

func (pattern *MatchPattern) MatchAnyTree() *MatchPattern {
	pattern.AnyTree = true
	return pattern
}

func (pattern *MatchPattern) AddColRefs(name string, refs UnorderedMap[string, string]) *MatchPattern {
	for key, val := range refs {
		pattern.WithAlias(key, TColumnRef(name, val))
	}
	return pattern
}

func (pattern *MatchPattern) WithAlias(alias string, matcher RValueMatcher) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers, &AliasMatcher{Alias: alias, Matcher: matcher})
	return pattern
}

func (pattern *MatchPattern) WithExactAssignedOutputs(expectedAliases []RValueMatcher) *MatchPattern {
	fun := func(builder *plan.QueryBuilder, node *plan2.Node) []VarRef {
		ret := make([]VarRef, 0)
		for _, expr := range node.ProjectList {
			col := expr.GetCol()
			if col != nil {
				ret = append(ret, VarRef{
					Name: strings.Split(col.Name, ".")[1],
					Type: expr.GetTyp(),
				})
			} else {
				ret = append(ret, VarRef{
					Name: expr.String(),
					Type: expr.GetTyp(),
				})
			}
		}
		return ret
	}
	pattern.Matchers = append(pattern.Matchers,
		&AssignedSymbolsMatcher{
			GetFunc:          fun,
			ExpectedMatchers: expectedAliases,
		},
	)
	return pattern
}

func (pattern *MatchPattern) WithExactAssignments(expectedAliases []RValueMatcher) *MatchPattern {
	fun := func(builder *plan.QueryBuilder, node *plan2.Node) []VarRef {
		ret := make([]VarRef, 0)
		for _, expr := range node.ProjectList {
			col := expr.GetCol()
			if col != nil {
				ret = append(ret, VarRef{
					Name: strings.Split(col.Name, ".")[1],
					Type: expr.GetTyp(),
				})
			} else {
				ret = append(ret, VarRef{
					Name: expr.String(),
					Type: expr.GetTyp(),
				})
			}
		}
		return ret
	}
	pattern.Matchers = append(pattern.Matchers,
		&AssignedSymbolsMatcher{
			GetFunc:          fun,
			ExpectedMatchers: expectedAliases,
		},
	)
	return pattern
}

func (pattern *MatchPattern) WithOutputs(aliases ...string) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers, &OutputMatcher{Aliases: aliases})
	return pattern
}

func (pattern *MatchPattern) WithExactOutputs(outputs ...string) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers,
		&SymbolsMatcher{
			GetFunc: func(builder *plan.QueryBuilder, node *plan2.Node) []VarRef {
				AssertFunc(node.NodeType == plan2.Node_PROJECT, "must be project node")
				ret := make([]VarRef, 0)
				for _, expr := range node.ProjectList {
					col := expr.GetCol()
					ret = append(ret, VarRef{
						Name: strings.Split(col.Name, ".")[1],
						Type: expr.GetTyp(),
					})
				}
				return ret
			},
			ExpectedAliases: outputs,
		})
	return pattern
}

func (pattern *MatchPattern) IsEnd() bool {
	return len(pattern.Children) == 0
}

func SimpleMatch(pattern *MatchPattern, node *plan2.Node) []*MatchingState {
	states := make([]*MatchingState, 0)
	if pattern.AnyTree {
		if len(node.Children) > 1 {
			childPatterns := make([]*MatchPattern, len(node.Children))
			for i := 0; i < len(node.Children); i++ {
				childPatterns[i] = pattern
			}
			states = append(states, &MatchingState{
				Patterns: childPatterns,
			})
		} else {
			states = append(states, &MatchingState{
				Patterns: []*MatchPattern{pattern},
			})
		}
	}

	if len(node.Children) == len(pattern.Children) &&
		SimpleMatchMatchers(pattern, node) {
		states = append(states, &MatchingState{
			Patterns: pattern.Children,
		})
	}
	return states
}

func SimpleMatchMatchers(pattern *MatchPattern, node *plan2.Node) bool {
	for _, matcher := range pattern.Matchers {
		if !matcher.SimpleMatch(node) {
			return false
		}
	}
	return true
}

func DeepMatch(
	ctx context.Context,
	node *plan2.Node,
	pattern *MatchPattern,
	aliases UnorderedMap[string, string]) (*MatchResult, error) {
	newAliases := make(UnorderedMap[string, string])
	for _, matcher := range pattern.Matchers {
		res, err := matcher.DeepMatch(ctx, node, aliases)
		if err != nil {
			return nil, err
		}
		if !res.IsMatch {
			return res, nil
		}

		err = MergeAliases(ctx, newAliases, res.RetAliases)
		if err != nil {
			return nil, err
		}
	}
	return MatchedWithAliases(newAliases), nil
}

func (pattern *MatchPattern) String() string {
	sb := strings.Builder{}
	pattern.toString(&sb, 0)
	return sb.String()
}

func (pattern *MatchPattern) toString(sb *strings.Builder, prefix int) {
	sb.WriteString(strings.Repeat("    ", prefix))
	sb.WriteString("- ")

	if pattern.AnyTree {
		sb.WriteString("anyTree")
	} else {
		sb.WriteString("node")
	}

	for _, matcher := range pattern.Matchers {
		if _, ok := matcher.(*NodeMatcher); ok {
			sb.WriteString("(")
			sb.WriteString(matcher.String())
			sb.WriteString(")")
			break
		}
	}
	sb.WriteString("\n")
	has := false
	for _, matcher := range pattern.Matchers {
		if _, ok := matcher.(*NodeMatcher); !ok {
			sb.WriteString(strings.Repeat("    ", prefix+1))
			sb.WriteString(matcher.String())
			sb.WriteString("\n")
			has = true
		} else if has {
			sb.WriteString(strings.Repeat("    ", prefix+1))
			sb.WriteString(matcher.String())
			sb.WriteString("\n")
		}
	}

	for _, child := range pattern.Children {
		child.toString(sb, prefix+1)
	}
}

func (state *MatchingState) IsEnd() bool {
	if len(state.Patterns) == 0 {
		return true
	}
	for _, pattern := range state.Patterns {
		if !pattern.IsEnd() {
			return false
		}
	}
	return true
}

func MatchSteps(ctx context.Context, query *plan2.Query, pattern *MatchPattern) (*MatchResult, error) {
	//fmt.Println(pattern)
	res, err := Match(ctx, query.Nodes, query.Nodes[query.Steps[0]], pattern)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func Match(ctx context.Context, nodes []*plan2.Node, node *plan2.Node, pattern *MatchPattern) (*MatchResult, error) {
	states := SimpleMatch(pattern, node)
	if len(states) == 0 {
		return FailMatched(), nil
	}

	//leaf node
	if len(node.Children) == 0 {
		return MatchLeaf(ctx, node, pattern, states)
	}

	res := FailMatched()
	for _, state := range states {
		childRes, err := MatchChildren(ctx, nodes, node, state)
		if err != nil {
			return nil, err
		}
		if !childRes.IsMatch {
			continue
		}

		deepRes, err := DeepMatch(ctx, node, pattern, childRes.RetAliases)
		if err != nil {
			return nil, err
		}
		if deepRes.IsMatch {
			if res.IsMatch {
				return nil, moerr.NewInternalError(ctx, "multiple match")
			}
			mergedRes, err := MergeAliasesReturnNew(ctx, childRes.RetAliases, deepRes.RetAliases)
			if err != nil {
				return nil, err
			}
			res = MatchedWithAliases(mergedRes)
		}
	}

	return res, nil
}

func MatchChildren(ctx context.Context,
	nodes []*plan2.Node,
	node *plan2.Node,
	state *MatchingState) (*MatchResult, error) {
	if len(node.Children) != len(state.Patterns) {
		return nil, moerr.NewInternalError(ctx, "patterns count != children count")
	}

	resAliases := make(UnorderedMap[string, string])
	for i, child := range node.Children {
		childRes, err := Match(ctx, nodes, nodes[child], state.Patterns[i])
		if err != nil {
			return nil, err
		}
		if !childRes.IsMatch {
			return FailMatched(), nil
		}
		err = MergeAliases(ctx, resAliases, childRes.RetAliases)
		if err != nil {
			return nil, err
		}
	}
	return MatchedWithAliases(resAliases), nil
}

func MatchLeaf(ctx context.Context,
	node *plan2.Node,
	pattern *MatchPattern,
	states []*MatchingState) (*MatchResult, error) {
	res := FailMatched()
	for _, state := range states {
		if !state.IsEnd() {
			continue
		}
		deepRes, err := DeepMatch(ctx, node, pattern, make(UnorderedMap[string, string]))
		if err != nil {
			return nil, err
		}
		if deepRes.IsMatch {
			if res.IsMatch {
				return nil, moerr.NewInternalError(ctx, "multiple match on leaf node ")
			}
			res = deepRes
		}
	}
	return res, nil
}

func MergeAliasesReturnNew(ctx context.Context,
	aliases1, aliases2 UnorderedMap[string, string]) (UnorderedMap[string, string], error) {
	ret := make(UnorderedMap[string, string])
	for k, v := range aliases1 {
		ret.Insert(k, v)
	}

	//merge aliases2
	for k, v := range aliases2 {
		err := Insert(ctx, ret, k, v)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func MergeAliases(ctx context.Context,
	aliases1, aliases2 UnorderedMap[string, string]) error {

	//merge aliases2
	for k, v := range aliases2 {
		err := Insert(ctx, aliases1, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func Insert(ctx context.Context, aliases UnorderedMap[string, string], k, v string) error {
	ok, ev := aliases.Find(k)
	if ok && ev == v {
		return nil
	}
	if ok {
		return moerr.NewInternalError(ctx, " %s -> %s already exists", k, ev)
	}
	aliases.Insert(k, v)
	return nil
}
