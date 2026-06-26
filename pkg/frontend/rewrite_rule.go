// Copyright 2024 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	hintPrefix = "/*+ "
	hintSuffix = " */"
)

// rewriteHintPayload is the JSON structure embedded in the hint comment.
type rewriteHintPayload struct {
	Rewrites map[string]string `json:"rewrites"`
}

// formatRewriteHint serializes a rules map into the /*+ {"rewrites": {...}} */ hint format.
func formatRewriteHint(ctx context.Context, rules map[string]string) (string, error) {
	payload := rewriteHintPayload{Rewrites: rules}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return "", moerr.NewInternalErrorf(ctx, "failed to serialize rewrite rules: %v", err)
	}
	// Encode appends a trailing newline, trim it
	return hintPrefix + strings.TrimSpace(buf.String()) + hintSuffix, nil
}

// parseRewriteHint parses a hint string back into a rules map.
func parseRewriteHint(ctx context.Context, hint string) (map[string]string, error) {
	if !strings.HasPrefix(hint, hintPrefix) || !strings.HasSuffix(hint, hintSuffix) {
		return nil, moerr.NewInternalErrorf(ctx, "invalid rewrite hint format")
	}
	jsonStr := hint[len(hintPrefix) : len(hint)-len(hintSuffix)]
	var payload rewriteHintPayload
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to parse rewrite hint: %v", err)
	}
	return payload.Rewrites, nil
}

// rewriteSQL checks the session's role rewrite rule cache and the
// remap_rewrites session variable and, if rules exist, prepends a hint comment
// to the SQL string.
// If ruleCache is nil (not yet loaded), it lazily loads rules via loadRuleCache.
// Session-variable rules are merged on top of the role rules (a session rule
// overrides a role rule for the same table key).
// If the combined rule set is empty, the original SQL is returned unchanged.
// This function only injects hints when enable_remap_hint is true.
// Rule cache load failures are returned to the caller so access-control rewrites
// do not silently fall back to the unmodified SQL.
func rewriteSQL(ctx context.Context, ses *Session, sql string) (string, error) {
	// Check if enable_remap_hint is enabled using cached value
	if !ses.rewriteEnabled.Load() {
		return sql, nil
	}

	// Check cache with read lock first
	ses.ruleCacheMu.RLock()
	cache := ses.ruleCache
	ses.ruleCacheMu.RUnlock()

	if cache == nil {
		// Load rules if cache is empty
		rules, err := loadRuleCache(ctx, ses)
		if err != nil {
			ses.Error(ctx, "failed to load rewrite rule cache", logutil.ErrorField(err))
			return sql, err
		}

		// Update cache with write lock and double-check
		ses.ruleCacheMu.Lock()
		if ses.ruleCache == nil {
			ses.ruleCache = rules
			cache = rules
		} else {
			cache = ses.ruleCache
		}
		ses.ruleCacheMu.Unlock()
	}

	// Merge session-variable rewrites on top of the role rules.
	sessionRules, err := getSessionRewriteRules(ctx, ses)
	if err != nil {
		return sql, err
	}

	if len(cache) == 0 && len(sessionRules) == 0 {
		// No persistent or session rules. Leave any inline /*+ ... */ hint to be
		// handled natively by the parser.
		return sql, nil
	}

	// Precedence: role rules < session variable < inline hint. The inline hint
	// is part of this single statement's text, so its overrides are naturally
	// effective for this one query only. We merge the inline rules into the
	// single hint we emit because the parser only honors the first leading
	// hint; emitting a second hint of our own would otherwise mask the inline
	// one.
	inlineRules, err := extractInlineRewrites(ctx, sql)
	if err != nil {
		return sql, err
	}

	combined := make(map[string]string, len(cache)+len(sessionRules)+len(inlineRules))
	for k, v := range cache {
		combined[k] = v
	}
	for k, v := range sessionRules {
		combined[k] = v
	}
	for k, v := range inlineRules {
		combined[k] = v
	}

	hint, err := formatRewriteHint(ctx, combined)
	if err != nil {
		return sql, err
	}

	return hint + " " + sql, nil
}

// extractInlineRewrites returns the table-rewrite rules from a leading
// /*+ {...} */ (or /*!+ {...} */) hint on sql, if present. It only treats a
// hint whose content is a JSON object (starting with '{') as a rewrite hint;
// other leading hints yield no rules. The original sql is left untouched — the
// rules are merged into the single hint rewriteSQL prepends.
func extractInlineRewrites(ctx context.Context, sql string) (map[string]string, error) {
	content, ok := leadingHintContent(sql)
	if !ok {
		return nil, nil
	}
	content = strings.TrimSpace(content)
	if content == "" || content[0] != '{' {
		return nil, nil
	}
	return parseSessionRewrites(ctx, content)
}

// leadingHintContent extracts the inner content of the first leading
// /*+ ... */ or /*!+ ... */ comment in sql. It returns ("", false) when sql
// does not start with such a hint.
func leadingHintContent(sql string) (string, bool) {
	s := strings.TrimLeft(sql, " \t\r\n")
	if !strings.HasPrefix(s, "/*+") && !strings.HasPrefix(s, "/*!+") {
		return "", false
	}
	start := 3
	if strings.HasPrefix(s, "/*!+") {
		start = 4
	}
	end := strings.Index(s[start:], "*/")
	if end < 0 {
		return "", false
	}
	return s[start : start+end], true
}

// getSessionRewriteRules reads the remap_rewrites session variable and returns
// its table-rewrite rules. An empty/unset variable yields no rules. The value
// is validated at SET time (validateRemapRewrites), so a parse error here is
// unexpected and is treated as "no session rules" rather than failing every
// query.
func getSessionRewriteRules(ctx context.Context, ses *Session) (map[string]string, error) {
	v, err := ses.GetSessionSysVar("remap_rewrites")
	if err != nil {
		// remap_rewrites is always registered; a lookup error should not block
		// the query, so fall back to "no session rules".
		return nil, nil
	}
	raw, ok := v.(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	rules, err := parseSessionRewrites(ctx, raw)
	if err != nil {
		ses.Error(ctx, "failed to parse remap_rewrites; ignoring session rewrites",
			logutil.ErrorField(err))
		return nil, nil
	}
	return rules, nil
}

// validateRemapRewrites validates a remap_rewrites session-variable value: it
// must be a string holding a valid rewrites payload (parseable JSON) where every
// table key is non-empty and every rule value is a SELECT-like statement (the
// same constraint as role rules). It is called at SET time so an invalid value
// is rejected immediately and never stored.
func validateRemapRewrites(ctx context.Context, val interface{}) error {
	raw, ok := val.(string)
	if !ok {
		return moerr.NewInvalidInputf(ctx, "remap_rewrites must be a string, got %T", val)
	}
	rules, err := parseSessionRewrites(ctx, raw)
	if err != nil {
		return err
	}
	for key, rule := range rules {
		if strings.TrimSpace(key) == "" {
			return moerr.NewInvalidInput(ctx, "remap_rewrites: table key must not be empty")
		}
		if err := validateRewriteRuleSQL(ctx, rule); err != nil {
			return err
		}
	}
	return nil
}

// parseSessionRewrites parses the remap_rewrites value. It accepts both the
// wrapped form {"rewrites": {"db.t": "select ..."}} (same payload as the inline
// hint) and the bare form {"db.t": "select ..."}.
func parseSessionRewrites(ctx context.Context, value string) (map[string]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	var payload rewriteHintPayload
	if err := json.Unmarshal([]byte(value), &payload); err == nil && payload.Rewrites != nil {
		return payload.Rewrites, nil
	}

	var rules map[string]string
	if err := json.Unmarshal([]byte(value), &rules); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid remap_rewrites value %q: %v", value, err)
	}
	return rules, nil
}

// loadRuleCache queries the mo_catalog.mo_role_rule table for all rewrite rules
// associated with the current user's active roles and returns them as a map.
func loadRuleCache(ctx context.Context, ses *Session) (map[string]string, error) {
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		return map[string]string{}, nil
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	roleIDs, err := loadActiveRoleIDsForRuleCache(ctx, bh, tenant)
	if err != nil {
		return nil, err
	}
	if len(roleIDs) == 0 {
		return map[string]string{}, nil
	}

	sql := getSqlForRoleRulesOfRoleIDs(roleIDs)

	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	rules := make(map[string]string)
	if !execResultArrayHasData(erArray) {
		return rules, nil
	}

	rolePriority := make(map[int64]int, len(roleIDs))
	for i, roleID := range roleIDs {
		rolePriority[roleID] = i
	}

	ruleRows := make([]rewriteRuleRow, 0, erArray[0].GetRowCount())
	rowCount := erArray[0].GetRowCount()
	for i := uint64(0); i < rowCount; i++ {
		roleID, err := erArray[0].GetInt64(ctx, i, 0)
		if err != nil {
			return nil, err
		}
		ruleName, err := erArray[0].GetString(ctx, i, 1)
		if err != nil {
			return nil, err
		}
		rule, err := erArray[0].GetString(ctx, i, 2)
		if err != nil {
			return nil, err
		}
		ruleRows = append(ruleRows, rewriteRuleRow{
			roleID:   roleID,
			ruleName: ruleName,
			rule:     rule,
		})
	}

	// Rule conflict priority follows active-role discovery order:
	// primary role, directly granted secondary roles by grant time, then inherited
	// roles in breadth-first grant-time order. Later rows override incompatible
	// earlier rows for the same rule_name.
	sort.SliceStable(ruleRows, func(i, j int) bool {
		iPriority, iOK := rolePriority[ruleRows[i].roleID]
		jPriority, jOK := rolePriority[ruleRows[j].roleID]
		if !iOK {
			iPriority = len(rolePriority)
		}
		if !jOK {
			jPriority = len(rolePriority)
		}
		if iPriority != jPriority {
			return iPriority < jPriority
		}
		return ruleRows[i].ruleName < ruleRows[j].ruleName
	})

	for _, row := range ruleRows {
		if existingRule, ok := rules[row.ruleName]; ok {
			mergedRule, err := mergeRewriteRules(ctx, existingRule, row.rule)
			if err != nil {
				return nil, err
			}
			rules[row.ruleName] = mergedRule
		} else {
			rules[row.ruleName] = row.rule
		}
	}

	return rules, nil
}

type rewriteRuleRow struct {
	roleID   int64
	ruleName string
	rule     string
}

func loadActiveRoleIDsForRuleCache(ctx context.Context, bh BackgroundExec, tenant *TenantInfo) ([]int64, error) {
	roleSet := make(map[int64]struct{})
	roleQueue := make([]int64, 0, 1)

	addRoleID := func(roleID int64) {
		if _, ok := roleSet[roleID]; ok {
			return
		}
		roleSet[roleID] = struct{}{}
		roleQueue = append(roleQueue, roleID)
	}

	addRoleID(int64(tenant.GetDefaultRoleID()))

	if tenant.GetUseSecondaryRole() {
		sql := getSqlForRoleIDsOfUserForRuleCache(int(tenant.GetUserID()))
		bh.ClearExecResultSet()
		if err := bh.Exec(ctx, sql); err != nil {
			return nil, err
		}

		erArray, err := getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}

		if execResultArrayHasData(erArray) {
			rowCount := erArray[0].GetRowCount()
			for i := uint64(0); i < rowCount; i++ {
				roleID, err := erArray[0].GetInt64(ctx, i, 0)
				if err != nil {
					return nil, err
				}
				addRoleID(roleID)
			}
		}
	}

	for i := 0; i < len(roleQueue); i++ {
		sql := getSqlForInheritedRoleIDsForRuleCache(roleQueue[i])
		bh.ClearExecResultSet()
		if err := bh.Exec(ctx, sql); err != nil {
			return nil, err
		}

		erArray, err := getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}

		if execResultArrayHasData(erArray) {
			rowCount := erArray[0].GetRowCount()
			for j := uint64(0); j < rowCount; j++ {
				roleID, err := erArray[0].GetInt64(ctx, j, 0)
				if err != nil {
					return nil, err
				}
				addRoleID(roleID)
			}
		}
	}

	return roleQueue, nil
}

func getSqlForRoleIDsOfUserForRuleCache(userID int) string {
	return fmt.Sprintf("select role_id,with_grant_option from mo_catalog.mo_user_grant where user_id = %d order by granted_time asc, role_id asc;", userID)
}

func getSqlForInheritedRoleIDsForRuleCache(roleID int64) string {
	return fmt.Sprintf("select granted_id,with_grant_option from mo_catalog.mo_role_grant where grantee_id = %d order by granted_time asc, granted_id asc;", roleID)
}

func getSqlForRoleRulesOfRoleIDs(roleIDs []int64) string {
	var builder strings.Builder
	for i, roleID := range roleIDs {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatInt(roleID, 10))
	}
	return fmt.Sprintf("select role_id, rule_name, `rule` from %s.%s where role_id in (%s) order by role_id, rule_name",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, builder.String())
}

func mergeRewriteRules(ctx context.Context, leftRule, rightRule string) (string, error) {
	leftRule = trimRewriteRuleForMerge(leftRule)
	rightRule = trimRewriteRuleForMerge(rightRule)

	if leftRule == rightRule {
		return leftRule, nil
	}

	mergedRule, ok, err := mergeRewriteRulesSafely(ctx, leftRule, rightRule)
	if err != nil {
		return "", err
	}
	if !ok {
		return rightRule, nil
	}
	return mergedRule, nil
}

func trimRewriteRuleForMerge(rule string) string {
	rule = strings.TrimSpace(rule)
	for strings.HasSuffix(rule, ";") {
		rule = strings.TrimSpace(strings.TrimSuffix(rule, ";"))
	}
	return rule
}

type rewriteRuleMergeShape struct {
	stmt       *tree.Select
	clause     *tree.SelectClause
	selectList string
	table      string
}

// Rewrite rules from active roles are a source-row visibility union: if any
// role allows a base row, that row should remain visible. For simple
// single-table SELECTs with matching select lists and source tables, OR-ing the
// filters expresses that row union directly. This is intentionally not a
// UNION DISTINCT equivalent for partial projections; two visible base rows that
// project to the same values should both remain visible.
func mergeRewriteRulesSafely(ctx context.Context, leftRule, rightRule string) (string, bool, error) {
	leftShape, ok, err := rewriteRuleMergeShapeForRule(ctx, leftRule)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	rightShape, ok, err := rewriteRuleMergeShapeForRule(ctx, rightRule)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	if leftShape.selectList != rightShape.selectList || leftShape.table != rightShape.table {
		return "", false, nil
	}

	merged := *leftShape.stmt
	mergedClause := *leftShape.clause
	mergedClause.Where = mergeRewriteRuleWhere(leftShape.clause.Where, rightShape.clause.Where)
	merged.Select = &mergedClause

	return tree.String(&merged, dialect.MYSQL), true, nil
}

func rewriteRuleMergeShapeForRule(ctx context.Context, rule string) (*rewriteRuleMergeShape, bool, error) {
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rule, 1)
	if err != nil {
		return nil, false, moerr.NewInternalErrorf(ctx, "failed to parse rewrite rule %q while merging rewrite rules: %v", rule, err)
	}
	shape, ok := rewriteRuleMergeShapeForStatement(stmt)
	return shape, ok, nil
}

func rewriteRuleMergeShapeForStatement(stmt tree.Statement) (*rewriteRuleMergeShape, bool) {
	switch s := stmt.(type) {
	case *tree.Select:
		return rewriteRuleMergeShapeForSelect(s)
	case *tree.ParenSelect:
		if s.Select == nil {
			return nil, false
		}
		return rewriteRuleMergeShapeForStatement(s.Select)
	default:
		return nil, false
	}
}

func rewriteRuleMergeShapeForSelect(stmt *tree.Select) (*rewriteRuleMergeShape, bool) {
	if stmt == nil || len(stmt.OrderBy) > 0 || stmt.Limit != nil || stmt.With != nil ||
		stmt.TimeWindow != nil || stmt.RankOption != nil || stmt.Ep != nil || stmt.SelectLockInfo != nil {
		return nil, false
	}

	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || !rewriteRuleSelectClauseIsMergeable(clause) {
		return nil, false
	}

	table, ok := rewriteRuleSingleTableSource(clause.From)
	if !ok {
		return nil, false
	}

	return &rewriteRuleMergeShape{
		stmt:       stmt,
		clause:     clause,
		selectList: normalizeRewriteSQL(tree.String(&clause.Exprs, dialect.MYSQL)),
		table:      table,
	}, true
}

func rewriteRuleSelectClauseIsMergeable(clause *tree.SelectClause) bool {
	return clause != nil &&
		!clause.Distinct &&
		clause.Option == 0 &&
		clause.GroupBy == nil &&
		clause.Having == nil &&
		rewriteRuleSelectExprsAreMergeable(clause.Exprs)
}

func rewriteRuleSelectExprsAreMergeable(exprs tree.SelectExprs) bool {
	for _, expr := range exprs {
		if rewriteRuleSelectExprIsStar(expr.Expr) && expr.As != nil && !expr.As.Empty() {
			return false
		}
		if !rewriteExprIsMergeSafe(expr.Expr) {
			return false
		}
	}
	return true
}

func rewriteRuleSelectExprIsStar(expr tree.Expr) bool {
	switch e := expr.(type) {
	case tree.UnqualifiedStar:
		return true
	case *tree.UnresolvedName:
		return e.Star
	default:
		return false
	}
}

func rewriteRuleSingleTableSource(from *tree.From) (string, bool) {
	if from == nil || len(from.Tables) != 1 {
		return "", false
	}

	tableExpr, ok := rewriteRuleSingleTableExpr(from.Tables[0])
	if !ok {
		return "", false
	}

	return normalizeRewriteSQL(tree.String(tableExpr, dialect.MYSQL)), true
}

func rewriteRuleSingleTableExpr(expr tree.TableExpr) (*tree.AliasedTableExpr, bool) {
	if joinExpr, ok := expr.(*tree.JoinTableExpr); ok {
		// The MySQL parser wraps a lone table reference as a CROSS JoinTableExpr.
		if joinExpr.Right != nil || joinExpr.Cond != nil || joinExpr.Option != "" {
			return nil, false
		}
		expr = joinExpr.Left
	}

	tableExpr, ok := expr.(*tree.AliasedTableExpr)
	if !ok || tableExpr.As.Cols != nil || len(tableExpr.IndexHints) > 0 {
		return nil, false
	}

	tableName, ok := tableExpr.Expr.(*tree.TableName)
	if !ok || tableName.AtTsExpr != nil {
		return nil, false
	}

	return tableExpr, true
}

func mergeRewriteRuleWhere(left, right *tree.Where) *tree.Where {
	// Mergeability is decided from the top-level SELECT shape only. Predicate
	// internals, including subqueries, stay opaque and are only OR-ed together.
	switch {
	case left == nil || left.Expr == nil:
		return nil
	case right == nil || right.Expr == nil:
		return nil
	default:
		return &tree.Where{
			Type: tree.AstWhere,
			Expr: tree.NewOrExpr(
				tree.NewParentExpr(left.Expr),
				tree.NewParentExpr(right.Expr),
			),
		}
	}
}

func normalizeRewriteSQL(sql string) string {
	return strings.ToLower(strings.TrimSpace(sql))
}

func rewriteExprIsMergeSafe(expr tree.Expr) bool {
	if expr == nil {
		return true
	}

	switch e := expr.(type) {
	case *tree.UnresolvedName, tree.UnqualifiedStar, *tree.NumVal, *tree.StrVal:
		return true
	case *tree.BinaryExpr:
		return rewriteExprIsMergeSafe(e.Left) && rewriteExprIsMergeSafe(e.Right)
	case *tree.UnaryExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.ComparisonExpr:
		return rewriteExprIsMergeSafe(e.Left) &&
			rewriteExprIsMergeSafe(e.Right) &&
			rewriteExprIsMergeSafe(e.Escape)
	case *tree.AndExpr:
		return rewriteExprIsMergeSafe(e.Left) && rewriteExprIsMergeSafe(e.Right)
	case *tree.XorExpr:
		return rewriteExprIsMergeSafe(e.Left) && rewriteExprIsMergeSafe(e.Right)
	case *tree.OrExpr:
		return rewriteExprIsMergeSafe(e.Left) && rewriteExprIsMergeSafe(e.Right)
	case *tree.NotExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsNullExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsNotNullExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsUnknownExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsNotUnknownExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsTrueExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsNotTrueExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsFalseExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.IsNotFalseExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.ParenExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.FuncExpr:
		name := rewriteFuncExprName(e)
		if name == "" || e.Type == tree.FUNC_TYPE_TABLE ||
			e.WindowSpec != nil ||
			function.GetFunctionIsAggregateByName(name) ||
			function.GetFunctionIsWinFunByName(name) {
			return false
		}
		return rewriteExprsAreMergeSafe(e.Exprs) && rewriteOrderByIsMergeSafe(e.OrderBy)
	case *tree.SerialExtractExpr:
		return rewriteExprIsMergeSafe(e.SerialExpr) && rewriteExprIsMergeSafe(e.IndexExpr)
	case *tree.CastExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.BitCastExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	case *tree.Tuple:
		return rewriteExprsAreMergeSafe(e.Exprs)
	case *tree.RangeCond:
		return rewriteExprIsMergeSafe(e.Left) &&
			rewriteExprIsMergeSafe(e.From) &&
			rewriteExprIsMergeSafe(e.To)
	case *tree.CaseExpr:
		if !rewriteExprIsMergeSafe(e.Expr) || !rewriteExprIsMergeSafe(e.Else) {
			return false
		}
		for _, when := range e.Whens {
			if when == nil {
				continue
			}
			if !rewriteExprIsMergeSafe(when.Cond) || !rewriteExprIsMergeSafe(when.Val) {
				return false
			}
		}
		return true
	case *tree.IntervalExpr:
		return rewriteExprIsMergeSafe(e.Expr)
	default:
		return false
	}
}

func rewriteExprsAreMergeSafe(exprs tree.Exprs) bool {
	for _, expr := range exprs {
		if !rewriteExprIsMergeSafe(expr) {
			return false
		}
	}
	return true
}

func rewriteOrderByIsMergeSafe(orderBy tree.OrderBy) bool {
	for _, order := range orderBy {
		if order == nil {
			continue
		}
		if !rewriteExprIsMergeSafe(order.Expr) {
			return false
		}
	}
	return true
}

func rewriteFuncExprName(fn *tree.FuncExpr) string {
	if fn.FuncName != nil {
		return strings.ToLower(fn.FuncName.Origin())
	}
	if name, ok := fn.Func.FunctionReference.(*tree.UnresolvedName); ok {
		return strings.ToLower(name.ColName())
	}
	return ""
}

func validateRewriteRuleSQL(ctx context.Context, rule string) error {
	if strings.TrimSpace(rule) == "" {
		return moerr.NewInvalidInput(ctx, "rewrite rule SQL is empty")
	}

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rule, 1)
	if err != nil {
		return moerr.NewInvalidInputf(ctx, "invalid rewrite rule SQL %q: %v", rule, err)
	}

	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
		return nil
	default:
		return moerr.NewInvalidInputf(ctx, "invalid rewrite rule SQL %q: only accept SELECT-like statements as rewrites", rule)
	}
}

// escapeSQLString escapes a string for safe use in SQL literals using writeEscapedSQLString.
func escapeSQLString(s string) string {
	var buf bytes.Buffer
	writeEscapedSQLString(&buf, []byte(s))
	return buf.String()
}

// handleAlterRoleAddRule verifies role existence, then inserts or updates a mo_role_rule record.
func handleAlterRoleAddRule(ses *Session, execCtx *ExecCtx, stmt *tree.AlterRoleAddRule) error {
	ctx := execCtx.reqCtx

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Begin transaction
	err := bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// Step 1: Verify role exists and get role_id
	sqlStr, err := getSqlForRoleIdOfRole(ctx, stmt.RoleName)
	if err != nil {
		return err
	}
	vr, err := verifyRoleFunc(ctx, bh, sqlStr, stmt.RoleName, roleType)
	if err != nil {
		return err
	}
	if vr == nil {
		return moerr.NewInternalErrorf(ctx, "there is no role %s", stmt.RoleName)
	}
	roleID := vr.id

	if err = validateRewriteRuleSQL(ctx, stmt.RuleSQL); err != nil {
		return err
	}

	// Derive rule_name from db.tbl
	ruleName := stmt.DbName + "." + stmt.TblName

	// Delete existing rule (if any), then insert the new one
	deleteSQL := fmt.Sprintf("delete from %s.%s where role_id = %d and rule_name = %s",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapeSQLString(ruleName))
	if err = bh.Exec(ctx, deleteSQL); err != nil {
		return err
	}

	insertSQL := fmt.Sprintf("insert into %s.%s (role_id, rule_name, `rule`) values (%d, %s, %s)",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID,
		escapeSQLString(ruleName), escapeSQLString(stmt.RuleSQL))
	if err = bh.Exec(ctx, insertSQL); err != nil {
		return err
	}

	// Invalidate current session's rule cache after successful rule modification
	// Note: This only affects the current session. Other sessions using the same role
	// will need to reconnect or execute SET ROLE to refresh their cache.
	// TODO: Implement cross-session cache invalidation for better consistency.
	ses.ruleCacheMu.Lock()
	ses.ruleCache = nil
	ses.ruleCacheMu.Unlock()

	return err
}

// handleAlterRoleDropRule verifies role and rule existence, then deletes the mo_role_rule record.
func handleAlterRoleDropRule(ses *Session, execCtx *ExecCtx, stmt *tree.AlterRoleDropRule) error {
	ctx := execCtx.reqCtx

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Begin transaction
	err := bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// Step 1: Verify role exists and get role_id
	sqlStr, err := getSqlForRoleIdOfRole(ctx, stmt.RoleName)
	if err != nil {
		return err
	}
	vr, err := verifyRoleFunc(ctx, bh, sqlStr, stmt.RoleName, roleType)
	if err != nil {
		return err
	}
	if vr == nil {
		return moerr.NewInternalErrorf(ctx, "there is no role %s", stmt.RoleName)
	}
	roleID := vr.id

	// Derive rule_name from db.tbl
	ruleName := stmt.DbName + "." + stmt.TblName

	// Step 2: Check if rule_name exists for this role_id
	checkSQL := fmt.Sprintf("select `rule` from %s.%s where role_id = %d and rule_name = %s",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapeSQLString(ruleName))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalErrorf(ctx, "rule '%s' does not exist for role '%s'", ruleName, stmt.RoleName)
	}

	// Step 3: Delete the matching record
	deleteSQL := fmt.Sprintf("delete from %s.%s where role_id = %d and rule_name = %s",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapeSQLString(ruleName))
	if err = bh.Exec(ctx, deleteSQL); err != nil {
		return err
	}

	// Invalidate current session's rule cache after successful rule modification
	// Note: This only affects the current session. Other sessions using the same role
	// will need to reconnect or execute SET ROLE to refresh their cache.
	// TODO: Implement cross-session cache invalidation for better consistency.
	ses.ruleCacheMu.Lock()
	ses.ruleCache = nil
	ses.ruleCacheMu.Unlock()

	return err
}

// handleShowRules queries and returns all rewrite rules for the specified role.
func handleShowRules(ses *Session, execCtx *ExecCtx, stmt *tree.ShowRules) error {
	ctx := execCtx.reqCtx

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Begin transaction
	err := bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// Step 1: Verify role exists and get role_id
	sqlStr, err := getSqlForRoleIdOfRole(ctx, stmt.RoleName)
	if err != nil {
		return err
	}
	vr, err := verifyRoleFunc(ctx, bh, sqlStr, stmt.RoleName, roleType)
	if err != nil {
		return err
	}
	if vr == nil {
		return moerr.NewInternalErrorf(ctx, "there is no role %s", stmt.RoleName)
	}
	roleID := vr.id

	// Step 2: Query mo_role_rule for all rules of this role_id
	querySQL := fmt.Sprintf("select rule_name, `rule` from %s.%s where role_id = %d",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID)

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, querySQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	// Step 3: Build result set with columns: rule_name, rule
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("rule_name")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("rule")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	if execResultArrayHasData(erArray) {
		rowCount := erArray[0].GetRowCount()
		for i := uint64(0); i < rowCount; i++ {
			ruleName, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return err
			}
			rule, err := erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return err
			}
			row := make([]interface{}, 2)
			row[0] = ruleName
			row[1] = rule
			mrs.AddRow(row)
		}
	}

	return trySaveQueryResult(ctx, ses, mrs)
}
