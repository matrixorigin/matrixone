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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

var _ Matcher = new(NodeMatcher)
var _ Matcher = new(TableScanMatcher)
var _ Matcher = new(AliasMatcher)
var _ Matcher = new(SymbolsMatcher)
var _ Matcher = new(AssignedSymbolsMatcher)
var _ Matcher = new(OutputMatcher)

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

func (matcher *NodeMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}
	return Matched(), nil
}

type TableScanMatcher struct {
	TableName  string
	Constraint plan.UnorderedMap[string, *Domain]
}

func (matcher *TableScanMatcher) String() string {
	return fmt.Sprintf("TableScanMatcher{table:%s}", matcher.TableName)
}

func (matcher *TableScanMatcher) SimpleMatch(node *plan.Node) bool {
	return node.NodeType == plan2.Node_TABLE_SCAN
}

func (matcher *TableScanMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}

	if strings.Compare(matcher.TableName, node.TableDef.Name) != 0 {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}
	if matcher.Constraint != nil {
		panic("add constraint check")
	}
	return Matched(), nil
}

type AliasMatcher struct {
	Alias   string
	Matcher RValueMatcher
}

func (matcher *AliasMatcher) String() string {
	if len(matcher.Alias) != 0 {
		return fmt.Sprintf("bind %s -> %s", matcher.Alias, matcher.Matcher)
	}
	return fmt.Sprintf("bind %s", matcher.Matcher)
}

func (matcher *AliasMatcher) SimpleMatch(node *plan.Node) bool {
	return true
}

func (matcher *AliasMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
	colDef := matcher.Matcher.GetAssignedVar(node, aliases)
	if colDef != nil && len(matcher.Alias) != 0 {
		return MatchedWithAlias(matcher.Alias, colDef.Name), nil
	}
	return NewMatchResult(colDef != nil, nil), nil
}

var _ RValueMatcher = new(ColumnRef)

type ColumnRef struct {
	TableName  string
	ColumnName string
}

func (matcher *ColumnRef) String() string {
	return fmt.Sprintf("Column %s:%s", matcher.TableName, matcher.ColumnName)
}

func (matcher *ColumnRef) GetAssignedVar(node *plan2.Node, aliases plan.UnorderedMap[string, string]) *SColDef {
	if node.NodeType != plan2.Node_TABLE_SCAN {
		return nil
	}

	if matcher.TableName != node.TableDef.Name {
		return nil
	}

	for _, col := range node.TableDef.Cols {
		if col.Name == matcher.ColumnName {
			return &SColDef{
				Name: col.Name,
				Type: col.Typ,
			}
		}
	}
	return nil
}

type GetFunc func(*plan.QueryBuilder, *plan2.Node) []SColDef

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

func (matcher *SymbolsMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
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

func (matcher *AssignedSymbolsMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return FailMatched(), nil
	}
	realCols := matcher.GetFunc(nil, node)

	expectCols := make([]SColDef, 0)
	for _, eMatcher := range matcher.ExpectedMatchers {
		colDef := eMatcher.GetAssignedVar(node, aliases)
		if colDef == nil {
			return FailMatched(), nil
		}
		expectCols = append(expectCols, *colDef)
	}

	return NewMatchResult(SColDefsEqual(realCols, expectCols), nil), nil
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

func (matcher *OutputMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases plan.UnorderedMap[string, string]) (*MatchResult, error) {
	for _, alias := range matcher.Aliases {
		ok, ref := aliases.Find(alias)
		if !ok {
			return FailMatched(), nil
		}
		found := false
		for _, projExpr := range node.ProjectList {
			name := projExpr.GetCol().Name
			names := strings.Split(name, ".")
			if len(names) != 2 {
				return FailMatched(), moerr.NewInternalError(ctx, "special name %s ", name)
			}
			colName := names[1]
			if ref == colName {
				found = true
				break
			}
		}
		if !found {
			return FailMatched(), nil
		}
	}
	return Matched(), nil
}
