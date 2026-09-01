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
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const informationSchemaStatistics = "statistics"

// PreparedPlanDependsOnSubscriptionMetadata reports whether a prepared plan
// expands INFORMATION_SCHEMA.STATISTICS. The expansion captures the complete
// visible subscription set while the plan is built, so reusing that plan after
// a subscription is created, dropped, or withdrawn would retain stale UNION
// branches even though no table schema version changed.
func PreparedPlanDependsOnSubscriptionMetadata(p *Plan) bool {
	if p == nil {
		return false
	}
	query := p.GetQuery()
	if query == nil {
		return false
	}
	statisticsView := objectkey.Encode(INFORMATION_SCHEMA, informationSchemaStatistics)
	for _, node := range query.GetNodes() {
		if node == nil {
			continue
		}
		for _, originView := range node.GetOriginViews() {
			if strings.EqualFold(originView, statisticsView) {
				return true
			}
		}
	}
	return false
}

// bindSubscriptionStatisticsView exposes one relational source containing the
// subscriber's local catalog and every subscription visible to that account.
// Outer predicates can consequently live in WHERE, JOIN ON, a derived query,
// or a prepared parameter without changing which catalogs are represented.
func (builder *QueryBuilder) bindSubscriptionStatisticsView(
	ctx *BindContext,
	tableDef *TableDef,
	snapshot *Snapshot,
	obj *ObjectRef,
	schema, table string,
) (int32, error) {
	subscriptions, err := builder.visibleSubscriptionMetadata(snapshot)
	if err != nil {
		return 0, err
	}

	nodes := make([]int32, 0, len(subscriptions)+1)
	localNode, err := builder.bindView(ctx, tableDef, snapshot, obj, schema, table, nil)
	if err != nil {
		return 0, err
	}
	nodes = append(nodes, localNode)
	for _, subscription := range subscriptions {
		nodeID, bindErr := builder.bindView(
			ctx, tableDef, snapshot, obj, schema, table, subscription,
		)
		if bindErr != nil {
			return 0, bindErr
		}
		nodes = append(nodes, nodeID)
	}
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	return builder.unionSubscriptionStatistics(nodes)
}

func (builder *QueryBuilder) visibleSubscriptionMetadata(
	snapshot *Snapshot,
) ([]*SubscriptionMeta, error) {
	provider, ok := builder.compCtx.(SubscriptionMetadataProvider)
	if !ok {
		return nil, nil
	}
	subscriptions, err := provider.GetSubscriptionMetas(snapshot)
	if err != nil {
		return nil, err
	}
	subscriptions = append([]*SubscriptionMeta(nil), subscriptions...)
	lowerCaseTableNames := builder.compCtx.GetLowerCaseTableNames()

	sort.SliceStable(subscriptions, func(i, j int) bool {
		if subscriptions[i] == nil {
			return false
		}
		if subscriptions[j] == nil {
			return true
		}
		leftName := subscriptions[i].SubName
		rightName := subscriptions[j].SubName
		leftKey := subscriptionMetadataNameKey(leftName, lowerCaseTableNames)
		rightKey := subscriptionMetadataNameKey(rightName, lowerCaseTableNames)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		// Make the selected representative deterministic when names compare equal
		// under a case-insensitive mode.
		return leftName < rightName
	})
	seen := make(map[string]struct{}, len(subscriptions))
	visible := subscriptions[:0]
	for _, subscription := range subscriptions {
		if subscription == nil || subscription.SubName == "" {
			continue
		}
		nameKey := subscriptionMetadataNameKey(subscription.SubName, lowerCaseTableNames)
		if _, duplicate := seen[nameKey]; duplicate {
			continue
		}
		seen[nameKey] = struct{}{}
		visible = append(visible, subscription)
	}
	return visible, nil
}

// subscriptionMetadataNameKey follows the server's database-identifier
// comparison rules. In mode 0 differently-cased subscription schema names are
// distinct databases; modes 1 and 2 resolve them case-insensitively.
func subscriptionMetadataNameKey(name string, lowerCaseTableNames int64) string {
	if lowerCaseTableNames == 0 {
		return name
	}
	return strings.ToLower(name)
}

func (builder *QueryBuilder) unionSubscriptionStatistics(nodes []int32) (int32, error) {
	firstCtx := builder.ctxByNode[nodes[0]]
	if firstCtx == nil {
		return 0, moerr.NewInternalError(builder.GetContext(), "STATISTICS view has no bind context")
	}
	columnCount := len(firstCtx.results)
	lastNodeID := nodes[0]
	for _, rightNodeID := range nodes[1:] {
		rightCtx := builder.ctxByNode[rightNodeID]
		if rightCtx == nil || len(rightCtx.results) != columnCount {
			return 0, moerr.NewInternalError(
				builder.GetContext(), "STATISTICS branches have different columns",
			)
		}

		leftTag := builder.ctxByNode[lastNodeID].rootTag()
		unionTag := builder.genNewBindTag()
		inputProjects := make([]*planpb.Expr, columnCount)
		outputProjects := make([]*planpb.Expr, columnCount)
		for i, result := range firstCtx.results {
			inputProjects[i] = GetColExpr(result.Typ, leftTag, int32(i))
			outputProjects[i] = GetColExpr(result.Typ, unionTag, int32(i))
			builder.nameByColRef[[2]int32{unionTag, int32(i)}] = firstCtx.headings[i]
		}

		unionCtx := NewBindContext(builder, nil)
		unionCtx.lower = firstCtx.lower
		unionCtx.snapshot = firstCtx.snapshot
		unionCtx.defaultDatabase = firstCtx.defaultDatabase
		unionCtx.cteName = firstCtx.cteName
		unionCtx.directView = firstCtx.directView
		unionCtx.viewChain = append([]string{}, firstCtx.viewChain...)
		unionCtx.restoreViewMySQLSpecialTypes = firstCtx.restoreViewMySQLSpecialTypes
		unionCtx.headings = append([]string{}, firstCtx.headings...)
		unionCtx.projectTag = unionTag
		unionCtx.projects = outputProjects
		unionCtx.results = outputProjects
		for i := range outputProjects {
			unionCtx.outputColumnProvenance[int32(i)] =
				firstCtx.outputColumnProvenanceForProject(int32(i))
		}

		lastNodeID = builder.appendNode(&planpb.Node{
			NodeType:    planpb.Node_UNION_ALL,
			Children:    []int32{lastNodeID, rightNodeID},
			BindingTags: []int32{unionTag},
			ProjectList: inputProjects,
		}, unionCtx)
	}
	return lastNodeID, nil
}

func selectClauseOf(stmt *tree.Select) *tree.SelectClause {
	if stmt == nil {
		return nil
	}
	for {
		switch selectStatement := stmt.Select.(type) {
		case *tree.SelectClause:
			return selectStatement
		case *tree.ParenSelect:
			stmt = selectStatement.Select
		default:
			return nil
		}
	}
}

func isSubscriptionStatisticsView(schema, table string, subscription *SubscriptionMeta) bool {
	return subscription != nil &&
		strings.EqualFold(schema, INFORMATION_SCHEMA) &&
		strings.EqualFold(table, informationSchemaStatistics)
}

// rewriteSubscriptionStatisticsAccount makes the persisted view's explicit
// account predicate agree with the publisher identity attached to its catalog
// scans. The rewrite is limited to the built-in STATISTICS view.
func rewriteSubscriptionStatisticsAccount(stmt *tree.Select, accountID uint32) {
	clause := selectClauseOf(stmt)
	if clause == nil || clause.Where == nil || clause.Where.Expr == nil {
		return
	}
	visitor := currentAccountIDVisitor{accountID: accountID}
	clause.Where.Expr, _ = clause.Where.Expr.Accept(visitor)
}

type currentAccountIDVisitor struct {
	accountID uint32
}

func (v currentAccountIDVisitor) Enter(expr tree.Expr) (tree.Expr, bool) {
	function, ok := expr.(*tree.FuncExpr)
	if !ok || function.FuncName == nil ||
		!strings.EqualFold(function.FuncName.Origin(), "current_account_id") {
		return expr, false
	}
	return tree.NewNumVal(
		uint64(v.accountID), strconv.FormatUint(uint64(v.accountID), 10), false, tree.P_uint64,
	), true
}

func (v currentAccountIDVisitor) Exit(expr tree.Expr) (tree.Expr, bool) {
	return expr, true
}

func rewriteSubscriptionStatisticsOutput(
	builder *QueryBuilder,
	nodeID int32,
	viewCtx *BindContext,
	subscriptionName string,
) {
	root := builder.qry.Nodes[nodeID]
	for i, heading := range viewCtx.headings {
		if !strings.EqualFold(heading, "table_schema") &&
			!strings.EqualFold(heading, "index_schema") {
			continue
		}
		expr := makePlan2StringConstExprWithType(subscriptionName)
		if i < len(viewCtx.results) {
			viewCtx.results[i] = expr
		}
		if i < len(viewCtx.projects) {
			viewCtx.projects[i] = expr
		}
		if root.GetNodeType() == planpb.Node_PROJECT && i < len(root.ProjectList) {
			root.ProjectList[i] = expr
		}
	}
}

func subscriptionMoTablesFilter(subscription *SubscriptionMeta) tree.Expr {
	if subscription == nil || subscription.DbName == "" {
		return nil
	}
	filter := tree.NewComparisonExpr(
		tree.EQUAL,
		tree.NewUnresolvedColName("reldatabase"),
		tree.NewNumVal(subscription.DbName, subscription.DbName, false, tree.P_char),
	)
	if subscription.Tables == "" || subscription.Tables == "*" {
		return filter
	}

	tableNames := strings.Split(subscription.Tables, ",")
	values := make(tree.Exprs, 0, len(tableNames))
	for _, tableName := range tableNames {
		tableName = strings.TrimSpace(tableName)
		if tableName != "" {
			values = append(values, tree.NewNumVal(tableName, tableName, false, tree.P_char))
		}
	}
	if len(values) == 0 {
		return filter
	}
	return tree.NewAndExpr(filter, tree.NewComparisonExpr(
		tree.IN,
		tree.NewUnresolvedColName("relname"),
		tree.NewTuple(values),
	))
}
