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

// rewriteSQL checks the session's role rewrite rule cache and, if rules exist,
// prepends a hint comment to the SQL string.
// If ruleCache is nil (not yet loaded), it lazily loads rules via loadRuleCache.
// If the rule set is empty, the original SQL is returned unchanged.
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

	if len(cache) == 0 {
		return sql, nil
	}

	hint, err := formatRewriteHint(ctx, cache)
	if err != nil {
		return sql, err
	}

	return hint + " " + sql, nil
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
	leftRule = trimRewriteRuleForUnion(leftRule)
	rightRule = trimRewriteRuleForUnion(rightRule)

	leftColumns, ok, err := rewriteRuleOutputColumns(ctx, leftRule)
	if err != nil {
		return "", err
	}
	if !ok {
		return rightRule, nil
	}
	rightColumns, ok, err := rewriteRuleOutputColumns(ctx, rightRule)
	if err != nil {
		return "", err
	}
	if !ok {
		return rightRule, nil
	}
	if !sameRewriteOutputColumns(leftColumns, rightColumns) {
		return rightRule, nil
	}
	return fmt.Sprintf("(%s) union distinct (%s)", leftRule, rightRule), nil
}

func trimRewriteRuleForUnion(rule string) string {
	rule = strings.TrimSpace(rule)
	for strings.HasSuffix(rule, ";") {
		rule = strings.TrimSpace(strings.TrimSuffix(rule, ";"))
	}
	return rule
}

type rewriteRuleOutputColumn struct {
	name string
	expr string
}

func rewriteRuleOutputColumns(ctx context.Context, rule string) ([]rewriteRuleOutputColumn, bool, error) {
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rule, 1)
	if err != nil {
		return nil, false, moerr.NewInternalErrorf(ctx, "failed to parse rewrite rule %q while merging rewrite rules: %v", rule, err)
	}
	columns, ok := outputColumnsFromRewriteStatement(stmt)
	return columns, ok, nil
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

func outputColumnsFromRewriteStatement(stmt tree.Statement) ([]rewriteRuleOutputColumn, bool) {
	switch s := stmt.(type) {
	case *tree.Select:
		if len(s.OrderBy) > 0 || s.Limit != nil {
			return nil, false
		}
		return outputColumnsFromRewriteSelectStatement(s.Select)
	case *tree.ParenSelect:
		if s.Select == nil {
			return nil, false
		}
		return outputColumnsFromRewriteStatement(s.Select)
	default:
		return nil, false
	}
}

func outputColumnsFromRewriteSelectStatement(stmt tree.SelectStatement) ([]rewriteRuleOutputColumn, bool) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		if !mergeableRewriteSelectClause(s) {
			return nil, false
		}
		columns := make([]rewriteRuleOutputColumn, 0, len(s.Exprs))
		for _, expr := range s.Exprs {
			column, ok := outputColumnFromRewriteSelectExpr(expr)
			if !ok {
				return nil, false
			}
			columns = append(columns, column)
		}
		return columns, true
	case *tree.UnionClause:
		return outputColumnsFromRewriteSelectStatement(s.Left)
	case *tree.ParenSelect:
		if s.Select == nil {
			return nil, false
		}
		return outputColumnsFromRewriteStatement(s.Select)
	case *tree.Select:
		if len(s.OrderBy) > 0 || s.Limit != nil {
			return nil, false
		}
		return outputColumnsFromRewriteSelectStatement(s.Select)
	default:
		return nil, false
	}
}

func mergeableRewriteSelectClause(stmt *tree.SelectClause) bool {
	if stmt.Distinct || stmt.Option&(tree.QuerySpecOptionDistinct|tree.QuerySpecOptionDistinctRow) != 0 {
		return false
	}
	if stmt.GroupBy != nil || stmt.Having != nil {
		return false
	}
	for _, expr := range stmt.Exprs {
		if !rewriteExprIsMergeSafe(expr.Expr) {
			return false
		}
	}
	return true
}

func outputColumnFromRewriteSelectExpr(expr tree.SelectExpr) (rewriteRuleOutputColumn, bool) {
	if expr.As != nil && !expr.As.Empty() {
		return rewriteRuleOutputColumn{
			name: normalizeRewriteOutputColumn(expr.As.Compare()),
			expr: normalizeRewriteOutputExpr(expr.Expr),
		}, true
	}

	switch e := expr.Expr.(type) {
	case tree.UnqualifiedStar:
		return rewriteRuleOutputColumn{}, false
	case *tree.UnresolvedName:
		if e.Star {
			return rewriteRuleOutputColumn{}, false
		}
		return rewriteRuleOutputColumn{
			name: normalizeRewriteOutputColumn(e.ColName()),
			expr: normalizeRewriteOutputColumn(tree.String(expr.Expr, dialect.MYSQL)),
		}, true
	default:
		exprText := normalizeRewriteOutputExpr(expr.Expr)
		return rewriteRuleOutputColumn{
			name: normalizeRewriteOutputColumn(tree.String(expr.Expr, dialect.MYSQL)),
			expr: exprText,
		}, true
	}
}

func normalizeRewriteOutputColumn(column string) string {
	return strings.ToLower(strings.TrimSpace(column))
}

func normalizeRewriteOutputExpr(expr tree.Expr) string {
	if _, ok := expr.(*tree.UnresolvedName); ok {
		return normalizeRewriteOutputColumn(tree.String(expr, dialect.MYSQL))
	}
	return strings.TrimSpace(tree.String(expr, dialect.MYSQL))
}

func sameRewriteOutputColumns(leftColumns, rightColumns []rewriteRuleOutputColumn) bool {
	if len(leftColumns) != len(rightColumns) {
		return false
	}
	for i := range leftColumns {
		if leftColumns[i] != rightColumns[i] {
			return false
		}
	}
	return true
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
