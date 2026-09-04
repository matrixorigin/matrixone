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
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	informationSchemaStatistics = "statistics"
	// maxSubscriptionStatisticsPublisherBranches is the validated admission
	// envelope for the current UNION-based representation: 64 visible
	// subscriptions across four logical STATISTICS occurrences. The limit is
	// statement-wide and counts only publisher views; the local view is always
	// retained. Exceeding it fails planning instead of truncating metadata.
	maxSubscriptionStatisticsPublisherBranches = 256
	// maxSubscriptionStatisticsPublicationTableEntries bounds the total number
	// of explicit publication table literals expanded by one statement. It is
	// intentionally statement-wide because the same visible subscriptions are
	// expanded again for every logical STATISTICS occurrence.
	maxSubscriptionStatisticsPublicationTableEntries = 4096
	// maxSubscriptionStatisticsPublicationTableLiteralBytes bounds the encoded
	// string-literal payload that accompanies the explicit table entries.
	maxSubscriptionStatisticsPublicationTableLiteralBytes = 1 << 20
)

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
	if err := builder.checkPlanningCanceled(); err != nil {
		return 0, err
	}
	subscriptions, err := builder.visibleSubscriptionMetadata(snapshot)
	if err != nil {
		return 0, err
	}
	if err := builder.checkPlanningCanceled(); err != nil {
		return 0, err
	}

	nodes := make([]int32, 0, len(subscriptions)+1)
	localNode, err := builder.bindView(ctx, tableDef, snapshot, obj, schema, table, nil)
	if err != nil {
		return 0, err
	}
	nodes = append(nodes, localNode)
	for _, subscription := range subscriptions {
		if err := builder.checkPlanningCanceled(); err != nil {
			return 0, err
		}
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
) ([]*SubscriptionMetadata, error) {
	provider, ok := builder.compCtx.(SubscriptionMetadataProvider)
	if !ok {
		return nil, nil
	}

	remaining := maxSubscriptionStatisticsPublisherBranches -
		builder.subscriptionStatisticsPublisherBranches
	if remaining < 0 {
		remaining = 0
	}
	snapshotKey, err := subscriptionMetadataSnapshotKey(snapshot)
	if err != nil {
		return nil, err
	}
	visible, cached := builder.subscriptionStatisticsMetadata[snapshotKey]
	if cached {
		if err := builder.reserveSubscriptionStatisticsBudget(visible); err != nil {
			return nil, err
		}
		return visible, nil
	}

	subscriptions, err := provider.GetSubscriptionMetadata(snapshot, remaining)
	if err != nil {
		return nil, err
	}
	if err := builder.checkPlanningCanceled(); err != nil {
		return nil, err
	}

	capacity := len(subscriptions)
	if capacity > remaining {
		capacity = remaining
	}
	lowerCaseTableNames := builder.compCtx.GetLowerCaseTableNames()
	seen := make(map[string]struct{}, capacity)
	visible = make([]*SubscriptionMetadata, 0, capacity)
	for _, subscription := range subscriptions {
		if err := builder.checkPlanningCanceled(); err != nil {
			return nil, err
		}
		if subscription == nil ||
			!validSubscriptionPublicationScope(subscription.Meta) ||
			(!subscription.AllTablesVisible && len(subscription.VisibleTableIDs) == 0) {
			continue
		}
		nameKey := subscriptionMetadataNameKey(subscription.Meta.SubName, lowerCaseTableNames)
		if _, duplicate := seen[nameKey]; duplicate {
			continue
		}
		if len(visible) == remaining {
			return nil, subscriptionStatisticsBudgetError(builder.GetContext())
		}
		seen[nameKey] = struct{}{}
		visible = append(visible, subscription)
	}
	sort.SliceStable(visible, func(i, j int) bool {
		leftName := visible[i].Meta.SubName
		rightName := visible[j].Meta.SubName
		leftKey := subscriptionMetadataNameKey(leftName, lowerCaseTableNames)
		rightKey := subscriptionMetadataNameKey(rightName, lowerCaseTableNames)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		// Make the selected representative deterministic when names compare equal
		// under a case-insensitive mode.
		return leftName < rightName
	})
	if builder.subscriptionStatisticsMetadata == nil {
		builder.subscriptionStatisticsMetadata = make(map[string][]*SubscriptionMetadata)
	}
	if err := builder.reserveSubscriptionStatisticsBudget(visible); err != nil {
		return nil, err
	}
	builder.subscriptionStatisticsMetadata[snapshotKey] = visible
	return visible, nil
}

// reserveSubscriptionStatisticsBudget admits one logical STATISTICS
// occurrence before any publisher view is bound. Alongside publisher branches,
// it accounts for explicit publication-table literals, which each become a
// plan expression in the publisher mo_tables filter.
func (builder *QueryBuilder) reserveSubscriptionStatisticsBudget(
	subscriptions []*SubscriptionMetadata,
) error {
	entries := 0
	literalBytes := 0
	for _, subscription := range subscriptions {
		if err := builder.checkPlanningCanceled(); err != nil {
			return err
		}
		if subscription == nil || subscription.Meta == nil {
			continue
		}
		scope, err := subscriptionPublicationTableScope(
			subscription.Meta.Tables, builder.compCtx.GetLowerCaseTableNames(), builder.checkPlanningCanceled,
		)
		if err != nil {
			return err
		}
		entries += len(scope.tableNames)
		literalBytes += scope.literalBytes
	}
	if builder.subscriptionStatisticsPublisherBranches+len(subscriptions) >
		maxSubscriptionStatisticsPublisherBranches {
		return subscriptionStatisticsBudgetError(builder.GetContext())
	}
	if builder.subscriptionStatisticsPublicationTableEntries+entries >
		maxSubscriptionStatisticsPublicationTableEntries {
		return subscriptionStatisticsPublicationTableEntriesBudgetError(builder.GetContext())
	}
	if builder.subscriptionStatisticsPublicationTableLiteralBytes+literalBytes >
		maxSubscriptionStatisticsPublicationTableLiteralBytes {
		return subscriptionStatisticsPublicationTableBytesBudgetError(builder.GetContext())
	}
	builder.subscriptionStatisticsPublisherBranches += len(subscriptions)
	builder.subscriptionStatisticsPublicationTableEntries += entries
	builder.subscriptionStatisticsPublicationTableLiteralBytes += literalBytes
	return nil
}

func subscriptionMetadataSnapshotKey(snapshot *Snapshot) (string, error) {
	if !IsSnapshotValid(snapshot) {
		return "", nil
	}
	data, err := snapshot.Marshal()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func subscriptionStatisticsBudgetError(ctx context.Context) error {
	return moerr.NewInvalidInputf(
		ctx,
		"information_schema.statistics publisher expansion exceeds planning budget of %d branches; reduce visible subscriptions or STATISTICS occurrences",
		maxSubscriptionStatisticsPublisherBranches,
	)
}

func subscriptionStatisticsPublicationTableEntriesBudgetError(ctx context.Context) error {
	return moerr.NewInvalidInputf(
		ctx,
		"information_schema.statistics publication table expansion exceeds planning budget of %d table entries; reduce published tables or STATISTICS occurrences",
		maxSubscriptionStatisticsPublicationTableEntries,
	)
}

func subscriptionStatisticsPublicationTableBytesBudgetError(ctx context.Context) error {
	return moerr.NewInvalidInputf(
		ctx,
		"information_schema.statistics publication table expansion exceeds planning budget of %d encoded table-name bytes; reduce published tables or STATISTICS occurrences",
		maxSubscriptionStatisticsPublicationTableLiteralBytes,
	)
}

// subscriptionMetadataNameKey follows the server's database-identifier
// comparison rules. In mode 0 differently-cased subscription schema names are
// distinct databases; modes 1 and 2 resolve them case-insensitively.
func subscriptionMetadataNameKey(name string, lowerCaseTableNames int64) string {
	return tree.NewCStr(name, lowerCaseTableNames).Compare()
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

func isSubscriptionStatisticsView(schema, table string, subscription *SubscriptionMetadata) bool {
	return subscription != nil && subscription.Meta != nil &&
		strings.EqualFold(schema, INFORMATION_SCHEMA) &&
		strings.EqualFold(table, informationSchemaStatistics)
}

// rewriteSubscriptionStatisticsAccount makes the persisted view agree with
// the publisher identity attached to its catalog scans. The rewrite is limited
// to the built-in STATISTICS view.
//
// The canonical information_schema views use __mo_visible_tables to enforce
// the current session's RBAC permissions. A subscription branch must not reuse
// that predicate: subscriber role IDs are local to the subscriber account and
// have no meaning in the publisher account. Subscriber RBAC is computed before
// this branch is bound; buildTable then intersects that scope with publication
// database/table filters on the publisher mo_tables scan. The CTE therefore
// needs only the publisher account guard here.
func rewriteSubscriptionStatisticsAccount(stmt *tree.Select, accountID uint32) bool {
	clause := selectClauseOf(stmt)
	if clause != nil && clause.Where != nil && clause.Where.Expr != nil {
		visitor := currentAccountIDVisitor{accountID: accountID}
		clause.Where.Expr, _ = clause.Where.Expr.Accept(visitor)
	}

	if stmt == nil || stmt.With == nil {
		return true
	}
	for _, cte := range stmt.With.CTEs {
		if cte == nil || cte.Name == nil ||
			!strings.EqualFold(string(cte.Name.Alias), "__mo_visible_tables") {
			continue
		}
		cteSelect, ok := cte.Stmt.(*tree.Select)
		if !ok {
			return false
		}
		cteClause := selectClauseOf(cteSelect)
		if cteClause == nil {
			return false
		}
		publisherAccount := tree.NewComparisonExpr(
			tree.EQUAL,
			tree.NewUnresolvedName(tree.NewCStr("tbl", 1), tree.NewCStr("account_id", 1)),
			tree.NewNumVal(
				uint64(accountID), strconv.FormatUint(uint64(accountID), 10), false, tree.P_uint64,
			),
		)
		cteClause.Where = &tree.Where{Type: tree.AstWhere, Expr: publisherAccount}
		return true
	}
	return true
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

func subscriptionMoTablesFilter(subscription *SubscriptionMetadata) tree.Expr {
	filter, _ := subscriptionMoTablesFilterWithCheck(subscription, 1, nil)
	return filter
}

func subscriptionMoTablesFilterWithCheck(
	subscription *SubscriptionMetadata,
	lowerCaseTableNames int64,
	checkCanceled func() error,
) (tree.Expr, error) {
	if subscription == nil {
		return nil, nil
	}
	if !validSubscriptionPublicationScope(subscription.Meta) {
		return falseSubscriptionMetadataFilter(), nil
	}
	meta := subscription.Meta
	var filter tree.Expr = tree.NewComparisonExpr(
		tree.EQUAL,
		tree.NewUnresolvedColName("reldatabase"),
		tree.NewNumVal(meta.DbName, meta.DbName, false, tree.P_char),
	)
	if meta.Tables != "*" {
		scope, err := subscriptionPublicationTableScope(
			meta.Tables, lowerCaseTableNames, checkCanceled,
		)
		if err != nil {
			return nil, err
		}
		if len(scope.tableNames) == 0 {
			return falseSubscriptionMetadataFilter(), nil
		}
		values := make(tree.Exprs, 0, len(scope.tableNames))
		for _, tableName := range scope.tableNames {
			values = append(values, tree.NewNumVal(tableName, tableName, false, tree.P_char))
		}
		filter = tree.NewAndExpr(filter, tree.NewComparisonExpr(
			tree.IN,
			tree.NewUnresolvedColName("relname"),
			tree.NewTuple(values),
		))
	}

	if subscription.AllTablesVisible {
		return filter, nil
	}
	visibleIDs := append([]uint64(nil), subscription.VisibleTableIDs...)
	sort.Slice(visibleIDs, func(i, j int) bool { return visibleIDs[i] < visibleIDs[j] })
	values := make(tree.Exprs, 0, len(visibleIDs))
	var previous uint64
	for i, tableID := range visibleIDs {
		if tableID == 0 || (i > 0 && tableID == previous) {
			continue
		}
		previous = tableID
		values = append(values, tree.NewNumVal(
			tableID, strconv.FormatUint(tableID, 10), false, tree.P_uint64,
		))
	}
	if len(values) == 0 {
		return falseSubscriptionMetadataFilter(), nil
	}
	return tree.NewAndExpr(filter, tree.NewComparisonExpr(
		tree.IN,
		tree.NewUnresolvedColName("rel_logical_id"),
		tree.NewTuple(values),
	)), nil
}

type subscriptionPublicationScope struct {
	tableNames   []string
	literalBytes int
}

// subscriptionPublicationTableScope normalizes persisted explicit publication
// lists before they are expanded into AST literals. Old catalog rows can still
// contain duplicate entries, so this must not rely solely on CREATE/ALTER
// normalization. The literal-byte cost includes SQL quoting and escaping.
func subscriptionPublicationTableScope(
	tables string,
	lowerCaseTableNames int64,
	checkCanceled func() error,
) (subscriptionPublicationScope, error) {
	if tables == "*" {
		return subscriptionPublicationScope{}, nil
	}
	scope := subscriptionPublicationScope{}
	seen := make(map[string]struct{})
	for _, tableName := range strings.Split(tables, ",") {
		if checkCanceled != nil {
			if err := checkCanceled(); err != nil {
				return subscriptionPublicationScope{}, err
			}
		}
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			continue
		}
		nameKey := subscriptionMetadataNameKey(tableName, lowerCaseTableNames)
		if _, duplicate := seen[nameKey]; duplicate {
			continue
		}
		seen[nameKey] = struct{}{}
		scope.tableNames = append(scope.tableNames, tableName)
		// Two quote bytes plus one escape byte for each embedded quote or
		// backslash mirrors the SQL-literal payload without a renderer.
		scope.literalBytes += len(tableName) + 2 + strings.Count(tableName, "'") +
			strings.Count(tableName, "\\")
	}
	sort.Strings(scope.tableNames)
	return scope, nil
}

// validSubscriptionPublicationScope validates the catalog fields that become
// publisher-side authorization predicates. Empty database/table scope must
// never degrade into an unfiltered cross-account catalog scan.
func validSubscriptionPublicationScope(meta *SubscriptionMeta) bool {
	if meta == nil || meta.SubName == "" || meta.DbName == "" || meta.Tables == "" {
		return false
	}
	if meta.Tables == "*" {
		return true
	}
	scope, err := subscriptionPublicationTableScope(meta.Tables, 1, nil)
	return err == nil && len(scope.tableNames) > 0
}

func falseSubscriptionMetadataFilter() tree.Expr {
	const impossibleScope = "__mo_invalid_subscription_scope__"
	left := tree.NewUnresolvedColName("reldatabase")
	right := tree.NewNumVal(impossibleScope, impossibleScope, false, tree.P_char)
	return tree.NewAndExpr(
		tree.NewComparisonExpr(tree.EQUAL, left, right),
		tree.NewComparisonExpr(
			tree.NOT_EQUAL,
			tree.NewUnresolvedColName("reldatabase"),
			tree.NewNumVal(impossibleScope, impossibleScope, false, tree.P_char),
		),
	)
}

// currentSubscriptionMoTablesFilter keeps direct subscription-table binding
// and account-wide metadata binding separate. Direct statements have already
// passed normal frontend privilege checks and need only publication scope;
// STATISTICS branches additionally carry subscriber-local table visibility.
func (builder *QueryBuilder) currentSubscriptionMoTablesFilter() (tree.Expr, error) {
	if builder.queryingSubscriptionMetadata != nil {
		return subscriptionMoTablesFilterWithCheck(
			builder.queryingSubscriptionMetadata, builder.compCtx.GetLowerCaseTableNames(), builder.checkPlanningCanceled,
		)
	}
	meta := builder.compCtx.GetQueryingSubscription()
	if meta == nil {
		return nil, nil
	}
	return subscriptionMoTablesFilterWithCheck(&SubscriptionMetadata{
		Meta:             meta,
		AllTablesVisible: true,
	}, builder.compCtx.GetLowerCaseTableNames(), builder.checkPlanningCanceled)
}
