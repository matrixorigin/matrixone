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
	"reflect"
	"strconv"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const informationSchemaStatistics = "statistics"

// configureSubscriptionMetadataScopes resolves each STATISTICS occurrence in
// one query block independently. A nested SELECT gets a new BindContext owner,
// while structural JOIN contexts keep the owner's pointer-keyed map.
func (builder *QueryBuilder) configureSubscriptionMetadataScopes(
	stmt *tree.Select,
	ctx *BindContext,
) error {
	ctx.subscriptionMetadataScopes = nil
	clause := selectClauseOf(stmt)
	if clause == nil || clause.From == nil {
		return nil
	}

	var sources []subscriptionStatisticsSource
	tableCount := 0
	for _, table := range clause.From.Tables {
		collectSubscriptionStatisticsSources(table, "", &sources, &tableCount)
	}
	allowUnqualified := len(sources) == 1 && tableCount == 1
	for _, source := range sources {
		constraint := tableSchemaConstraintForSource(
			whereExpr(clause), source.qualifier, allowUnqualified,
		)
		databaseName, ok := builder.subscriptionSchemaConstraintValue(constraint)
		if !ok || databaseName == "" || strings.EqualFold(databaseName, INFORMATION_SCHEMA) {
			continue
		}

		subscription, err := builder.compCtx.GetSubscriptionMeta(databaseName, nil)
		if err != nil {
			return err
		}
		if subscription == nil {
			continue
		}
		if ctx.subscriptionMetadataScopes == nil {
			ctx.subscriptionMetadataScopes = make(map[string]*SubscriptionMeta)
		}
		ctx.subscriptionMetadataScopes[strings.ToLower(source.qualifier)] = subscription
	}
	return nil
}

func subscriptionMetadataScopeForQualifier(
	ctx *BindContext,
	qualifier string,
) *SubscriptionMeta {
	owner := ctx.queryBlockOwner
	if owner == nil {
		owner = ctx
	}
	return owner.subscriptionMetadataScopes[strings.ToLower(qualifier)]
}

func tableExprWithoutParens(expr tree.TableExpr) tree.TableExpr {
	for {
		paren, ok := expr.(*tree.ParenTableExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

func subscriptionStatisticsTable(expr tree.TableExpr) *tree.TableName {
	table, ok := expr.(*tree.TableName)
	if !ok || !strings.EqualFold(string(table.SchemaName), INFORMATION_SCHEMA) ||
		!strings.EqualFold(string(table.ObjectName), informationSchemaStatistics) {
		return nil
	}
	return table
}

type subscriptionStatisticsSource struct {
	table     *tree.TableName
	qualifier string
}

func collectSubscriptionStatisticsSources(
	expr tree.TableExpr,
	alias string,
	sources *[]subscriptionStatisticsSource,
	tableCount *int,
) {
	switch table := expr.(type) {
	case nil:
		return
	case *tree.AliasedTableExpr:
		collectSubscriptionStatisticsSources(table.Expr, string(table.As.Alias), sources, tableCount)
	case *tree.ParenTableExpr:
		collectSubscriptionStatisticsSources(table.Expr, alias, sources, tableCount)
	case *tree.JoinTableExpr:
		collectSubscriptionStatisticsSources(table.Left, "", sources, tableCount)
		collectSubscriptionStatisticsSources(table.Right, "", sources, tableCount)
	case *tree.TableName:
		(*tableCount)++
		if !strings.EqualFold(string(table.SchemaName), INFORMATION_SCHEMA) ||
			!strings.EqualFold(string(table.ObjectName), informationSchemaStatistics) {
			return
		}
		if alias == "" {
			alias = string(table.ObjectName)
		}
		*sources = append(*sources, subscriptionStatisticsSource{table: table, qualifier: alias})
	default:
		// A derived table is one source in this query block. Its SELECT owns and
		// configures any STATISTICS occurrences inside it separately.
		(*tableCount)++
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

func whereExpr(clause *tree.SelectClause) tree.Expr {
	if clause == nil || clause.Where == nil {
		return nil
	}
	return clause.Where.Expr
}

type subscriptionSchemaConstraint struct {
	value tree.Expr
}

func tableSchemaConstraintForSource(
	expr tree.Expr,
	qualifier string,
	allowUnqualified bool,
) subscriptionSchemaConstraint {
	switch typed := expr.(type) {
	case nil:
		return subscriptionSchemaConstraint{}
	case *tree.AndExpr:
		left := tableSchemaConstraintForSource(typed.Left, qualifier, allowUnqualified)
		right := tableSchemaConstraintForSource(typed.Right, qualifier, allowUnqualified)
		if left.value != nil {
			return left
		}
		return right
	case *tree.OrExpr:
		left := tableSchemaConstraintForSource(typed.Left, qualifier, allowUnqualified)
		right := tableSchemaConstraintForSource(typed.Right, qualifier, allowUnqualified)
		if sameSubscriptionSchemaConstraint(left.value, right.value) {
			return left
		}
		return subscriptionSchemaConstraint{}
	case *tree.ParenExpr:
		return tableSchemaConstraintForSource(typed.Expr, qualifier, allowUnqualified)
	case *tree.ComparisonExpr:
		if typed.Op != tree.EQUAL {
			return subscriptionSchemaConstraint{}
		}
		if value, ok := tableSchemaConstraintValue(
			typed.Left, typed.Right, qualifier, allowUnqualified,
		); ok {
			return subscriptionSchemaConstraint{value: value}
		}
		if value, ok := tableSchemaConstraintValue(
			typed.Right, typed.Left, qualifier, allowUnqualified,
		); ok {
			return subscriptionSchemaConstraint{value: value}
		}
	}
	// NOT/XOR and unrelated predicates do not prove a schema, but an exact
	// constraint in a separate top-level AND conjunct remains sufficient.
	return subscriptionSchemaConstraint{}
}

func sameSubscriptionSchemaConstraint(left, right tree.Expr) bool {
	if left == nil || right == nil {
		return false
	}
	switch left := left.(type) {
	case *tree.NumVal:
		right, ok := right.(*tree.NumVal)
		return ok && left.ValType == tree.P_char && right.ValType == tree.P_char &&
			strings.EqualFold(left.String(), right.String())
	case *tree.ParamExpr:
		right, ok := right.(*tree.ParamExpr)
		return ok && left.Offset == right.Offset
	default:
		return false
	}
}

func tableSchemaConstraintValue(
	column tree.Expr,
	value tree.Expr,
	qualifier string,
	allowUnqualified bool,
) (tree.Expr, bool) {
	name, ok := column.(*tree.UnresolvedName)
	if !ok || !strings.EqualFold(name.ColName(), "table_schema") ||
		!tableSchemaColumnMatchesSource(name, qualifier, allowUnqualified) {
		return nil, false
	}
	switch value := value.(type) {
	case *tree.NumVal:
		if value.ValType == tree.P_char {
			return value, true
		}
	case *tree.ParamExpr:
		return value, true
	}
	return nil, false
}

func tableSchemaColumnMatchesSource(
	name *tree.UnresolvedName,
	qualifier string,
	allowUnqualified bool,
) bool {
	tableName := name.TblName()
	if tableName == "" {
		return allowUnqualified
	}
	if !strings.EqualFold(tableName, qualifier) {
		return false
	}
	databaseName := name.DbName()
	return databaseName == "" || strings.EqualFold(databaseName, INFORMATION_SCHEMA)
}

func (builder *QueryBuilder) subscriptionSchemaConstraintValue(
	constraint subscriptionSchemaConstraint,
) (string, bool) {
	switch value := constraint.value.(type) {
	case *tree.NumVal:
		return value.String(), value.ValType == tree.P_char
	case *tree.ParamExpr:
		proc := builder.compCtx.GetProcess()
		if proc == nil {
			return "", false
		}
		// Parser parameter offsets are one-based, while the process vector is
		// zero-based (the same conversion is performed when ParamRef values are
		// evaluated by the execution engine).
		paramIndex := value.Offset - 1
		params := proc.GetPrepareParams()
		if params == nil || paramIndex < 0 || paramIndex >= params.Length() ||
			params.IsNull(uint64(paramIndex)) {
			return "", false
		}
		return params.GetStringAt(paramIndex), true
	default:
		return "", false
	}
}

// PreparedStatementHasDynamicSubscriptionMetadata reports prepared SELECTs
// whose STATISTICS publisher is selected by a TABLE_SCHEMA parameter. Those
// statements must be rebound after execute-time parameters are installed;
// fixing the publisher at PREPARE time makes reuse with another catalog wrong.
func PreparedStatementHasDynamicSubscriptionMetadata(stmt tree.Statement) bool {
	return statementTreeHasDynamicSubscriptionMetadata(reflect.ValueOf(stmt))
}

// statementTreeHasDynamicSubscriptionMetadata walks the complete parser tree.
// SELECTs can be nested in CTEs, unions, derived tables, and scalar predicates;
// reflection is used for the same reason as selectTreeHasExportParam: the
// parser's expression visitor deliberately leaves Subquery.Accept unimplemented.
func statementTreeHasDynamicSubscriptionMetadata(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		if value.CanInterface() {
			if clause, ok := value.Interface().(*tree.SelectClause); ok &&
				selectClauseHasDynamicSubscriptionMetadata(clause) {
				return true
			}
		}
		return statementTreeHasDynamicSubscriptionMetadata(value.Elem())
	}

	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if statementTreeHasDynamicSubscriptionMetadata(value.Field(i)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if statementTreeHasDynamicSubscriptionMetadata(value.Index(i)) {
				return true
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if statementTreeHasDynamicSubscriptionMetadata(iterator.Key()) ||
				statementTreeHasDynamicSubscriptionMetadata(iterator.Value()) {
				return true
			}
		}
	}
	return false
}

func selectClauseHasDynamicSubscriptionMetadata(clause *tree.SelectClause) bool {
	if clause == nil || clause.From == nil {
		return false
	}
	var sources []subscriptionStatisticsSource
	tableCount := 0
	for _, table := range clause.From.Tables {
		collectSubscriptionStatisticsSources(table, "", &sources, &tableCount)
	}
	allowUnqualified := len(sources) == 1 && tableCount == 1
	for _, source := range sources {
		constraint := tableSchemaConstraintForSource(
			whereExpr(clause), source.qualifier, allowUnqualified,
		)
		if _, dynamic := constraint.value.(*tree.ParamExpr); dynamic {
			return true
		}
	}
	return false
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
