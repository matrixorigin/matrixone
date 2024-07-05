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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TNode(nodeType plan.Node_NodeType, children []*MatchPattern) *MatchPattern {
	return TAny(children).With(&NodeMatcher{NodeType: nodeType})
}

// TAny yields a pattern without designated node type
func TAny(children []*MatchPattern) *MatchPattern {
	return &MatchPattern{
		Children: children,
	}
}

// TAnyTree denotes children tree matches the patterns
func TAnyTree(children []*MatchPattern) *MatchPattern {
	return TAny(children).MatchAnyTree()
}

func TAnyNot(nodeType plan.Node_NodeType,,children []*MatchPattern) *MatchPattern {
	return TAny(children).With(&NodeMatcher{NodeType: nodeType,Not: true})
}

func TTableScan(tableName string) *MatchPattern {
	return TNode(plan.Node_TABLE_SCAN,nil).With(&TableScanMatcher{
		TableName: tableName,
	})
}

func TTableScanWithColRef(tableName string,colRefs plan2.UnorderedMap[string,string]) *MatchPattern {
	ret := TTableScan(tableName)
	return ret.AddColRefs(tableName,colRefs)
}

func TColumnRef(tableName,colName string)  RValueMatcher{
	return &ColumnRef{TableName: tableName,ColumnName: colName}
}

func TAggregate() *MatchPattern {}

func (pattern *MatchPattern) With(matcher Matcher) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers, matcher)
	return pattern
}

func (pattern *MatchPattern) MatchAnyTree() *MatchPattern {
	pattern.AnyTree = true
	return pattern
}

func (pattern *MatchPattern) AddColRefs(name string, refs plan2.UnorderedMap[string, string])*MatchPattern {
	for key, val := range refs {
		pattern.WithAlias(key, TColumnRef(name,val))
	}
	return pattern
}

func (pattern *MatchPattern) WithAlias(alias string,matcher RValueMatcher) *MatchPattern {
	pattern.Matchers = append(pattern.Matchers,&AliasMatcher{Alias: alias, Matcher: matcher})
	return pattern
}

