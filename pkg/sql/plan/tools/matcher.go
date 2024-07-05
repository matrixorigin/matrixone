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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

var _ Matcher = new(NodeMatcher)
var _ Matcher = new(TableScanMatcher)
var _ Matcher = new(AliasMatcher)

type NodeMatcher struct {
	NodeType plan.Node_NodeType
	Not      bool
}

func (matcher *NodeMatcher) SimpleMatch(node *plan.Node) bool {
	if matcher.Not {
		return node.NodeType != matcher.NodeType
	}
	return node.NodeType == matcher.NodeType
}

func (matcher *NodeMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases Aliases) (*MatchResult, error) {
	if !matcher.SimpleMatch(node) {
		return nil, moerr.NewInternalError(ctx, "simple mach failed.")
	}
	return Matched(), nil
}

type TableScanMatcher struct {
	TableName  string
	Constraint plan2.UnorderedMap[string, *Domain]
}

func (matcher *TableScanMatcher) SimpleMatch(node *plan.Node) bool {
	return node.NodeType == plan.Node_TABLE_SCAN
}

func (matcher *TableScanMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases Aliases) (*MatchResult, error) {
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

func (matcher *AliasMatcher) SimpleMatch(node *plan.Node) bool {
	//TODO implement me
	panic("implement me")
}

func (matcher *AliasMatcher) DeepMatch(ctx context.Context, node *plan.Node, aliases Aliases) (*MatchResult, error) {
	//TODO implement me
	panic("implement me")
}

var _ RValueMatcher = new(ColumnRef)

type ColumnRef struct {
	TableName  string
	ColumnName string
}

func (matcher *ColumnRef) GetAssignedVar(node *plan.Node, aliases Aliases) *plan.ColDef {
	//TODO implement me
	panic("implement me")
}
