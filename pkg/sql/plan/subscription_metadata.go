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
	"strconv"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const informationSchemaStatistics = "statistics"

// enterSubscriptionMetadataScope routes INFORMATION_SCHEMA.STATISTICS to the
// publisher when the query is scoped to a subscription database. Connector/J
// emits exactly this shape for getIndexInfo and getPrimaryKeys.
func (builder *QueryBuilder) enterSubscriptionMetadataScope(stmt *tree.Select) (func(), error) {
	previous := builder.subscriptionMetadataScope
	if previous != nil || !selectReadsInformationSchemaStatistics(stmt) {
		return func() {}, nil
	}

	databaseName, allowDefaultDatabase := selectTableSchemaEquality(stmt)
	if databaseName == "" && allowDefaultDatabase {
		databaseName = builder.compCtx.DefaultDatabase()
	}
	if databaseName == "" || strings.EqualFold(databaseName, INFORMATION_SCHEMA) {
		return func() {}, nil
	}

	subscription, err := builder.compCtx.GetSubscriptionMeta(databaseName, nil)
	if err != nil {
		return nil, err
	}
	if subscription == nil {
		return func() {}, nil
	}

	builder.subscriptionMetadataScope = subscription
	return func() {
		builder.subscriptionMetadataScope = previous
	}, nil
}

func selectReadsInformationSchemaStatistics(stmt *tree.Select) bool {
	clause := selectClauseOf(stmt)
	if clause == nil || clause.From == nil {
		return false
	}
	for _, table := range clause.From.Tables {
		if tableExprReadsInformationSchemaStatistics(table) {
			return true
		}
	}
	return false
}

func tableExprReadsInformationSchemaStatistics(expr tree.TableExpr) bool {
	switch table := expr.(type) {
	case *tree.AliasedTableExpr:
		return tableExprReadsInformationSchemaStatistics(table.Expr)
	case *tree.ParenTableExpr:
		return tableExprReadsInformationSchemaStatistics(table.Expr)
	case *tree.JoinTableExpr:
		return tableExprReadsInformationSchemaStatistics(table.Left) ||
			tableExprReadsInformationSchemaStatistics(table.Right)
	case *tree.TableName:
		return strings.EqualFold(string(table.SchemaName), INFORMATION_SCHEMA) &&
			strings.EqualFold(string(table.ObjectName), informationSchemaStatistics)
	default:
		return false
	}
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

func selectTableSchemaEquality(stmt *tree.Select) (string, bool) {
	clause := selectClauseOf(stmt)
	if clause == nil || clause.Where == nil || clause.Where.Expr == nil {
		return "", true
	}
	databaseName, ambiguous := tableSchemaConjunct(clause.Where.Expr)
	if ambiguous {
		return "", false
	}
	return databaseName, true
}

func tableSchemaConjunct(expr tree.Expr) (string, bool) {
	switch typed := expr.(type) {
	case *tree.AndExpr:
		left, leftAmbiguous := tableSchemaConjunct(typed.Left)
		right, rightAmbiguous := tableSchemaConjunct(typed.Right)
		if leftAmbiguous || rightAmbiguous ||
			(left != "" && right != "" && !strings.EqualFold(left, right)) {
			return "", true
		}
		if left != "" {
			return left, false
		}
		return right, false
	case *tree.ParenExpr:
		return tableSchemaConjunct(typed.Expr)
	case *tree.ComparisonExpr:
		if typed.Op != tree.EQUAL {
			return "", false
		}
		if databaseName, ok := tableSchemaLiteral(typed.Left, typed.Right); ok {
			return databaseName, false
		}
		if databaseName, ok := tableSchemaLiteral(typed.Right, typed.Left); ok {
			return databaseName, false
		}
	case *tree.OrExpr, *tree.XorExpr, *tree.NotExpr:
		return "", true
	}
	// OR/XOR/NOT and other compound predicates do not identify one safe
	// publisher scope for the complete query.
	return "", false
}

func tableSchemaLiteral(column, value tree.Expr) (string, bool) {
	name, ok := column.(*tree.UnresolvedName)
	if !ok || !strings.EqualFold(name.ColName(), "table_schema") {
		return "", false
	}
	literal, ok := value.(*tree.NumVal)
	if !ok || literal.ValType != tree.P_char {
		return "", false
	}
	return literal.String(), true
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
