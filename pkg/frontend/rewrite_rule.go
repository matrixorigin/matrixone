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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
func formatRewriteHint(rules map[string]string) (string, error) {
	payload := rewriteHintPayload{Rewrites: rules}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return "", fmt.Errorf("internal error: failed to serialize rewrite rules: %w", err)
	}
	// Encode appends a trailing newline, trim it
	return hintPrefix + strings.TrimSpace(buf.String()) + hintSuffix, nil
}

// parseRewriteHint parses a hint string back into a rules map.
func parseRewriteHint(hint string) (map[string]string, error) {
	if !strings.HasPrefix(hint, hintPrefix) || !strings.HasSuffix(hint, hintSuffix) {
		return nil, fmt.Errorf("invalid rewrite hint format")
	}
	jsonStr := hint[len(hintPrefix) : len(hint)-len(hintSuffix)]
	var payload rewriteHintPayload
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		return nil, fmt.Errorf("failed to parse rewrite hint: %w", err)
	}
	return payload.Rewrites, nil
}

// rewriteSQL checks the session's role rewrite rule cache and, if rules exist,
// prepends a hint comment to the SQL string.
// If ruleCache is nil (not yet loaded), it lazily loads rules via loadRuleCache.
// If the rule set is empty, the original SQL is returned unchanged.
func rewriteSQL(ctx context.Context, ses *Session, sql string) (string, error) {
	if ses.ruleCache == nil {
		rules, err := loadRuleCache(ctx, ses)
		if err != nil {
			return sql, nil
		}
		ses.ruleCache = rules
	}

	if len(ses.ruleCache) == 0 {
		return sql, nil
	}

	hint, err := formatRewriteHint(ses.ruleCache)
	if err != nil {
		return sql, nil
	}

	return hint + " " + sql, nil
}

// loadRuleCache queries the mo_catalog.mo_role_rule table for all rewrite rules
// associated with the current user's active role and returns them as a map.
func loadRuleCache(ctx context.Context, ses *Session) (map[string]string, error) {
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		return map[string]string{}, nil
	}
	roleID := tenant.GetDefaultRoleID()

	sql := fmt.Sprintf("select rule_name, `rule` from %s.%s where role_id = %d",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

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
		rules[ruleName] = rule
	}

	return rules, nil
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

	// Step 2: Check if rule_name already exists for this role_id
	checkSQL := fmt.Sprintf("select `rule` from %s.%s where role_id = %d and rule_name = '%s'",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, strings.ReplaceAll(stmt.RuleName, "'", "''"))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	escapedRuleName := strings.ReplaceAll(stmt.RuleName, "'", "''")
	escapedRule := strings.ReplaceAll(stmt.RuleSQL, "'", "''")

	if execResultArrayHasData(erArray) {
		// Rule exists: UPDATE
		updateSQL := fmt.Sprintf("update %s.%s set `rule` = '%s' where role_id = %d and rule_name = '%s'",
			catalog.MO_CATALOG, catalog.MO_ROLE_RULE, escapedRule, roleID, escapedRuleName)
		if err = bh.Exec(ctx, updateSQL); err != nil {
			return err
		}
	} else {
		// Rule does not exist: INSERT
		insertSQL := fmt.Sprintf("insert into %s.%s (role_id, rule_name, `rule`) values (%d, '%s', '%s')",
			catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapedRuleName, escapedRule)
		if err = bh.Exec(ctx, insertSQL); err != nil {
			return err
		}
	}

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

	// Step 2: Check if rule_name exists for this role_id
	escapedRuleName := strings.ReplaceAll(stmt.RuleName, "'", "''")
	checkSQL := fmt.Sprintf("select `rule` from %s.%s where role_id = %d and rule_name = '%s'",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapedRuleName)

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalErrorf(ctx, "rule '%s' does not exist for role '%s'", stmt.RuleName, stmt.RoleName)
	}

	// Step 3: Delete the matching record
	deleteSQL := fmt.Sprintf("delete from %s.%s where role_id = %d and rule_name = '%s'",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, escapedRuleName)
	if err = bh.Exec(ctx, deleteSQL); err != nil {
		return err
	}

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
