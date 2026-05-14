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
			// Log the error for debugging
			ses.Error(ctx, "failed to load rewrite rule cache", logutil.ErrorField(err))
			return sql, nil
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
		return sql, nil
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

	rowCount := erArray[0].GetRowCount()
	for i := uint64(0); i < rowCount; i++ {
		ruleName, err := erArray[0].GetString(ctx, i, 0)
		if err != nil {
			return nil, err
		}
		rule, err := erArray[0].GetString(ctx, i, 1)
		if err != nil {
			return nil, err
		}
		if existingRule, ok := rules[ruleName]; ok {
			rules[ruleName] = mergeRewriteRules(ctx, existingRule, rule)
		} else {
			rules[ruleName] = rule
		}
	}

	return rules, nil
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
		sql := getSqlForRoleIdOfUserId(int(tenant.GetUserID()))
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
		sql := getSqlForInheritedRoleIdOfRoleId(roleQueue[i])
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

	roleIDs := make([]int64, 0, len(roleSet))
	for roleID := range roleSet {
		roleIDs = append(roleIDs, roleID)
	}
	sort.Slice(roleIDs, func(i, j int) bool {
		return roleIDs[i] < roleIDs[j]
	})
	return roleIDs, nil
}

func getSqlForRoleRulesOfRoleIDs(roleIDs []int64) string {
	var builder strings.Builder
	for i, roleID := range roleIDs {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatInt(roleID, 10))
	}
	return fmt.Sprintf("select rule_name, `rule` from %s.%s where role_id in (%s) order by role_id, rule_name",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, builder.String())
}

func mergeRewriteRules(ctx context.Context, leftRule, rightRule string) string {
	leftRule = trimRewriteRuleForUnion(leftRule)
	rightRule = trimRewriteRuleForUnion(rightRule)

	leftColumns, ok := rewriteRuleOutputColumns(ctx, leftRule)
	if !ok {
		return rightRule
	}
	rightColumns, ok := rewriteRuleOutputColumns(ctx, rightRule)
	if !ok {
		return rightRule
	}
	if !sameRewriteOutputColumns(leftColumns, rightColumns) {
		return rightRule
	}
	return fmt.Sprintf("(%s) union distinct (%s)", leftRule, rightRule)
}

func trimRewriteRuleForUnion(rule string) string {
	rule = strings.TrimSpace(rule)
	for strings.HasSuffix(rule, ";") {
		rule = strings.TrimSpace(strings.TrimSuffix(rule, ";"))
	}
	return rule
}

func rewriteRuleOutputColumns(ctx context.Context, rule string) ([]string, bool) {
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, rule, 1)
	if err != nil {
		return nil, false
	}
	return outputColumnsFromRewriteStatement(stmt)
}

func outputColumnsFromRewriteStatement(stmt tree.Statement) ([]string, bool) {
	switch s := stmt.(type) {
	case *tree.Select:
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

func outputColumnsFromRewriteSelectStatement(stmt tree.SelectStatement) ([]string, bool) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		columns := make([]string, 0, len(s.Exprs))
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
		return outputColumnsFromRewriteSelectStatement(s.Select)
	default:
		return nil, false
	}
}

func outputColumnFromRewriteSelectExpr(expr tree.SelectExpr) (string, bool) {
	if expr.As != nil && !expr.As.Empty() {
		return expr.As.Compare(), true
	}

	switch e := expr.Expr.(type) {
	case tree.UnqualifiedStar:
		return "", false
	case *tree.UnresolvedName:
		if e.Star {
			return "", false
		}
		return e.ColName(), true
	default:
		return strings.ToLower(tree.String(expr.Expr, dialect.MYSQL)), true
	}
}

func sameRewriteOutputColumns(leftColumns, rightColumns []string) bool {
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
