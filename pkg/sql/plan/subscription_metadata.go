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
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/subscriptionmetadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	informationSchemaStatistics = "statistics"
	// informationSchemaStatisticsNdvFunction names the published distinct-value
	// estimate inside the persisted STATISTICS body. A publisher branch appends
	// the in-band publisher account config to each call so the estimate
	// resolves through the publisher identity without any per-row internal
	// subscription lookup.
	informationSchemaStatisticsNdvFunction = "mo_table_col_ndv"
	// informationSchemaStatisticsNullCntFunction names the published NULL-row
	// population used beside the distinct-value estimate by CARDINALITY's
	// NULL value group. A publisher branch bakes the same publisher account
	// config into it, because the subscriber session cannot open the
	// publisher's database by name at evaluation time.
	informationSchemaStatisticsNullCntFunction = "mo_table_col_nullcnt"
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
	for _, node := range query.GetNodes() {
		if node == nil {
			continue
		}
		for _, step := range node.GetViewPath() {
			if step != nil && strings.EqualFold(step.DatabaseName, INFORMATION_SCHEMA) && strings.EqualFold(step.ViewName, informationSchemaStatistics) {
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
		if subscription == nil {
			continue
		}
		validScope, scopeErr := validSubscriptionPublicationScopeWithCheck(
			subscription.Meta, lowerCaseTableNames, builder.checkPlanningCanceled,
		)
		if scopeErr != nil {
			return nil, scopeErr
		}
		if !validScope ||
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
		unionCtx.cteDatabase = firstCtx.cteDatabase
		unionCtx.viewPath = append([]*planpb.ViewStep(nil), firstCtx.viewPath...)
		unionCtx.restoreViewMySQLSpecialTypes = firstCtx.restoreViewMySQLSpecialTypes
		unionCtx.headings = append([]string{}, firstCtx.headings...)
		unionCtx.projectTag = unionTag
		unionCtx.projects = outputProjects
		unionCtx.results = outputProjects
		for i := range outputProjects {
			unionCtx.setOutputColumnProvenance(
				int32(i), firstCtx.outputColumnProvenanceForProject(int32(i)))
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

// rewriteSubscriptionStatisticsNdvArguments bakes the publisher branch's
// publication identity into every statistics-estimator call of the persisted
// STATISTICS body. The branch keeps scanning the publisher's catalog rows, so
// tbl.reldatabase already carries the publisher's physical database name, but
// the volatile estimate is evaluated later with the subscriber session
// identity, which cannot open the publisher's database by name. Appending the
// publisher account id as an in-band config argument lets the executor attach
// that account before resolving the already-published database and relation
// directly through the engine, so a publisher branch performs no per-row
// subscription metadata lookup at execution time. The branch representation
// is a UNION: its publisher arms run as parallel scopes, and resolving the
// account through the session's shared internal executor from more than one
// arm interleaves internal result sets on one channel and corrupts them, so
// the resolved identity must be carried in the plan itself.
//
// The walk reaches the projections, WHERE, HAVING and GROUP BY of the body's
// select clauses, which is where the repository's STATISTICS body can legally
// compute the estimate. The persisted visibility CTEs and FROM sources are
// deliberately left alone: they contain EXISTS subqueries, tree.Subquery.Accept
// is not implemented, and the estimate never depends on their expressions.
//
// Account id zero is the sys account. Publications created by the sys session
// (CREATE PUBLICATION under dump, every `from sys publication` subscription)
// publish with mo_subs.pub_account_id = 0, and those branches scan sys catalog
// rows whose reldatabase still names the physical published database, so the
// account must be baked for them exactly as for any foreign publisher (#30910).
func rewriteSubscriptionStatisticsNdvArguments(stmt *tree.Select, pubMeta *planpb.SubscriptionMeta) {
	if stmt == nil || pubMeta == nil {
		return
	}
	rewriteStatisticsNdvProjectionSelect(
		stmt, statisticsNdvArgumentRewriter{publicationAccountID: pubMeta.GetAccountId()})
}

func rewriteStatisticsNdvProjectionSelect(stmt *tree.Select, visitor statisticsNdvArgumentRewriter) {
	if stmt == nil {
		return
	}
	rewriteStatisticsNdvProjectionStatement(stmt.Select, visitor)
}

func rewriteStatisticsNdvProjectionStatement(
	selectStatement tree.SelectStatement,
	visitor statisticsNdvArgumentRewriter,
) {
	switch selectStatement := selectStatement.(type) {
	case *tree.SelectClause:
		rewriteStatisticsNdvProjectionClause(selectStatement, visitor)
	case *tree.Select:
		rewriteStatisticsNdvProjectionSelect(selectStatement, visitor)
	case *tree.ParenSelect:
		rewriteStatisticsNdvProjectionStatement(selectStatement.Select, visitor)
	case *tree.UnionClause:
		rewriteStatisticsNdvProjectionStatement(selectStatement.Left, visitor)
		rewriteStatisticsNdvProjectionStatement(selectStatement.Right, visitor)
	}
}

func rewriteStatisticsNdvProjectionClause(clause *tree.SelectClause, visitor statisticsNdvArgumentRewriter) {
	for i := range clause.Exprs {
		if clause.Exprs[i].Expr != nil {
			clause.Exprs[i].Expr, _ = clause.Exprs[i].Expr.Accept(visitor)
		}
	}
	if clause.GroupBy != nil {
		for groupIndex := range clause.GroupBy.GroupByExprsList {
			rewriteStatisticsNdvExprs(clause.GroupBy.GroupByExprsList[groupIndex], visitor)
		}
		rewriteStatisticsNdvExprs(clause.GroupBy.GroupingSet, visitor)
	}
	if clause.Where != nil && clause.Where.Expr != nil {
		clause.Where.Expr, _ = clause.Where.Expr.Accept(visitor)
	}
	if clause.Having != nil && clause.Having.Expr != nil {
		clause.Having.Expr, _ = clause.Having.Expr.Accept(visitor)
	}
}

func rewriteStatisticsNdvExprs(exprs tree.Exprs, visitor statisticsNdvArgumentRewriter) {
	for i := range exprs {
		if exprs[i] != nil {
			exprs[i], _ = exprs[i].Accept(visitor)
		}
	}
}

// statisticsPublicationAccountConfigPrefix introduces the in-band publisher
// account marker appended by the rewriter. The format is private to the
// builtins' publication arms; nothing user-visible may depend on it.
const statisticsPublicationAccountConfigPrefix = "pubacct="

// statisticsPublicationAccountConfig renders the in-band config argument that
// names the publisher account owning a branch row's statistics.
func statisticsPublicationAccountConfig(accountID int32) string {
	return statisticsPublicationAccountConfigPrefix +
		strconv.FormatInt(int64(accountID), 10)
}

// statisticsNdvConfigArity and statisticsNullCntConfigArity name the plain
// direct arities the persisted STATISTICS body keeps for the two estimators:
// mo_table_col_ndv's fourth argument is the index's algo_params and
// mo_table_col_nullcnt's third argument is the column. The publisher-account
// config argument sits exactly one position beyond them.
const (
	statisticsNdvConfigArity     = 4
	statisticsNullCntConfigArity = 3
)

// isUserSuppliedPublicationConfigLiteral reports whether one estimator
// argument is a plain string literal carrying the in-band publication-config
// marker. The config channel is compiler-owned, so text reaches it only as
// the bake's P_char literal or as a literal typed verbatim by the user; the
// numeric suffix mirrors the estimator's parser (publicationAccountIDFromConfig)
// so a decodable marker never masquerades as index metadata.
func isUserSuppliedPublicationConfigLiteral(arg tree.Expr) bool {
	literal, ok := arg.(*tree.NumVal)
	if !ok || literal.ValType != tree.P_char {
		return false
	}
	rest, prefixed := strings.CutPrefix(literal.String(), statisticsPublicationAccountConfigPrefix)
	if !prefixed || rest == "" {
		return false
	}
	if _, err := strconv.ParseUint(rest, 10, 32); err != nil {
		return false
	}
	return true
}

// userSuppliedStatisticsConfigStripped removes the user-reachable
// publication-config argument(s) from a bound estimator call and returns the
// arguments to bind instead. The config argument is a compiler-owned channel:
// the only authorized producer is
// rewriteSubscriptionStatisticsNdvArguments, which appends the publisher
// identity to a publisher branch while
// QueryBuilder.queryingSubscriptionMetadata scopes that branch's binding.
// A config argument reaching binding from anywhere else - direct SQL, view
// bodies, subqueries, prepared or default expressions - would let user text
// select another account's statistics, so outside that scope the removal
// holds at every reachable shape (#30910 tenant isolation):
//
//   - a call of exactly the baked arity (statisticsNdvConfigArity+1 /
//     statisticsNullCntConfigArity+1) carries user-produced text in the
//     config slot, whatever its form - literal, column reference or prepared
//     placeholder - so the trailing argument is removed wholesale and the
//     remaining plain direct arguments keep their own overload surface;
//   - inside the plain direct surface, a config-shaped string literal is
//     removed only from mo_table_col_ndv's index/algo-parameter slot, the
//     one plain argument that is a parameter rather than an object name:
//     there it would otherwise render a false unavailable estimate instead
//     of the plain call's published value. The database, table and column
//     arguments spell object names, so a literal that merely equals the
//     marker text - SHOW INDEX supplies db and table as literals, so SHOW
//     INDEX FROM d.`pubacct=0` must keep binding - stays bound as the name
//     it asks for, and mo_table_col_nullcnt, whose plain surface ends at
//     the identity arguments, keeps only the trailing rule above.
//
// A nil builder binds on the base binder path outside any publisher branch,
// so it strips as well. Calls beyond the baked arity are left untouched: no
// overload exists for them, so binding fails closed. Non-config literals and
// marker text the estimator cannot decode are left to normal binding for
// the same reason - they select no account and can only bind the plain
// overload surface.
func userSuppliedStatisticsConfigStripped(
	builder *QueryBuilder,
	funcName string,
	args tree.Exprs,
) tree.Exprs {
	visible := builder != nil && builder.queryingSubscriptionMetadata != nil
	plainArgs := -1
	switch {
	case visible:
		return args
	case strings.EqualFold(funcName, informationSchemaStatisticsNdvFunction):
		plainArgs = statisticsNdvConfigArity
	case strings.EqualFold(funcName, informationSchemaStatisticsNullCntFunction):
		plainArgs = statisticsNullCntConfigArity
	default:
		return args
	}

	stripped := args
	if len(stripped) == plainArgs+1 {
		stripped = stripped[:len(stripped)-1]
	}
	if len(stripped) > plainArgs {
		return stripped
	}
	// The only plain direct-surface position a config literal can occupy
	// without displacing an object name is mo_table_col_ndv's algo-parameter
	// slot: a decodable literal there is removed so the call binds to the
	// identical plain call. Every earlier position is the db/table/column
	// identity and keeps its spelled value (a legal identifier may equal
	// the marker text), and mo_table_col_nullcnt has no plain parameter
	// beyond its identity arguments, so only the trailing rule removes
	// its config-shaped text.
	if plainArgs == statisticsNdvConfigArity &&
		len(stripped) == statisticsNdvConfigArity &&
		isUserSuppliedPublicationConfigLiteral(stripped[statisticsNdvConfigArity-1]) {
		stripped = stripped[:statisticsNdvConfigArity-1]
	}
	return stripped
}

type statisticsNdvArgumentRewriter struct {
	publicationAccountID int32
}

func (v statisticsNdvArgumentRewriter) Enter(expr tree.Expr) (tree.Expr, bool) {
	function, ok := expr.(*tree.FuncExpr)
	if !ok || function.FuncName == nil {
		return expr, false
	}
	// CARDINALITY derives its estimate from both statistics builtins, so the
	// publisher branch must bake its identity into each one it publishes: the
	// distinct-value estimate (the overload ends with the index's algo_params)
	// and the NULL-row population.
	name := function.FuncName.Origin()
	isNdv := strings.EqualFold(name, informationSchemaStatisticsNdvFunction)
	isNullCnt := strings.EqualFold(name, informationSchemaStatisticsNullCntFunction)
	if !isNdv && !isNullCnt {
		return expr, false
	}
	configuredArgs := statisticsNdvConfigArity
	if isNullCnt {
		configuredArgs = statisticsNullCntConfigArity
	}
	// The persisted body carries either publication-config-bearing calls or
	// the plain direct shapes; the config argument is appended exactly once,
	// so a re-visited tree never grows a second config.
	if len(function.Exprs) < configuredArgs || len(function.Exprs) > configuredArgs {
		return function, true
	}
	function.Exprs = append(function.Exprs, tree.NewNumVal(
		statisticsPublicationAccountConfig(v.publicationAccountID),
		statisticsPublicationAccountConfig(v.publicationAccountID),
		false, tree.P_char,
	))
	return function, true
}

func (v statisticsNdvArgumentRewriter) Exit(expr tree.Expr) (tree.Expr, bool) {
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
	valid, _ := validSubscriptionPublicationScopeWithCheck(meta, 1, nil)
	return valid
}

func validSubscriptionPublicationScopeWithCheck(
	meta *SubscriptionMeta,
	lowerCaseTableNames int64,
	checkCanceled func() error,
) (bool, error) {
	if meta == nil || meta.SubName == "" || meta.DbName == "" || meta.Tables == "" {
		return false, nil
	}
	if meta.Tables == "*" {
		return true, nil
	}
	scope, err := subscriptionPublicationTableScope(
		meta.Tables, lowerCaseTableNames, checkCanceled,
	)
	return err == nil && len(scope.tableNames) > 0, err
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

const (
	subscriptionTablesFunctionName  = "mo_subscription_tables"
	subscriptionColumnsFunctionName = "mo_subscription_columns"
)

var (
	subscriptionTablesColDefs  []*planpb.ColDef
	subscriptionColumnsColDefs []*planpb.ColDef
)

func init() {
	tableNames, tableTypes := subscriptionmetadata.TablesSchema()
	subscriptionTablesColDefs = subscriptionMetadataColDefs(tableNames, tableTypes)
	columnNames, columnTypes := subscriptionmetadata.ColumnsSchema()
	subscriptionColumnsColDefs = subscriptionMetadataColDefs(columnNames, columnTypes)
}

func subscriptionMetadataColDefs(names []string, columnTypes []types.Type) []*planpb.ColDef {
	cols := make([]*planpb.ColDef, len(names))
	for i := range names {
		cols[i] = &planpb.ColDef{Name: names[i], Typ: makePlan2Type(&columnTypes[i])}
	}
	return cols
}

func requireSubscriptionMetadataProtocol(ctx context.Context, proc *process.Process) error {
	if proc == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(
			ctx,
			"subscription information-schema metadata requires all CNs to support protocol version 46",
		)
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < defines.MORPCVersion46 {
		return moerr.NewNotSupported(
			ctx,
			"subscription information-schema metadata requires all CNs to support protocol version 46",
		)
	}
	return nil
}

func requireSubscriptionMetadataView(
	ctx context.Context,
	bindCtx *BindContext,
	persistedViewTarget string,
	functionName string,
) error {
	for current := bindCtx; current != nil; current = current.parent {
		if len(current.viewPath) > 0 {
			step := current.viewPath[len(current.viewPath)-1]
			if step != nil && subscriptionMetadataOwnerAllowed(objectkey.Encode(step.DatabaseName, step.ViewName), functionName) {
				return nil
			}
		}
	}
	if persistedViewTarget != "" &&
		subscriptionMetadataOwnerAllowed(persistedViewTarget, functionName) {
		return nil
	}
	return moerr.NewNotSupportedf(ctx,
		"%s is private to information_schema metadata views", functionName)
}

func subscriptionMetadataOwnerAllowed(ownerKey string, functionName string) bool {
	database, view := objectkey.Decode(ownerKey)
	if !strings.EqualFold(database, "information_schema") {
		return false
	}
	switch functionName {
	case subscriptionTablesFunctionName:
		return strings.EqualFold(view, "tables") || strings.EqualFold(view, "columns")
	case subscriptionColumnsFunctionName, ViewColumnsFunctionName, SubscriptionViewColumnsFunctionName:
		return strings.EqualFold(view, "columns")
	default:
		return false
	}
}

func (builder *QueryBuilder) buildSubscriptionMetadata(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
	functionName string,
	cols []*planpb.ColDef,
) (int32, error) {
	if len(tbl.Func.Exprs) != 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(),
			functionName+" function has invalid input args length", len(tbl.Func.Exprs))
	}
	if err := requireSubscriptionMetadataProtocol(builder.GetContext(), builder.compCtx.GetProcess()); err != nil {
		return 0, err
	}
	if err := requireSubscriptionMetadataView(
		builder.GetContext(), ctx, builder.persistedViewTarget, functionName); err != nil {
		return 0, err
	}

	node := &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		Stats:    &planpb.Stats{},
		TableDef: &planpb.TableDef{
			TableType: "func_table",
			TblFunc: &planpb.TableFunction{
				Name: functionName,
			},
			Cols: DeepCopyColDefList(cols),
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
		ScanSnapshot:    DeepCopySnapshot(ctx.snapshot),
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) buildSubscriptionTables(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	return builder.buildSubscriptionMetadata(
		tbl, ctx, exprs, children, subscriptionTablesFunctionName, subscriptionTablesColDefs)
}

func (builder *QueryBuilder) buildSubscriptionColumns(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	return builder.buildSubscriptionMetadata(
		tbl, ctx, exprs, children, subscriptionColumnsFunctionName, subscriptionColumnsColDefs)
}
