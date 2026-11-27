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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	hintPrefix = "/*+ "
	hintSuffix = " */"
	// nonStringRewriteMsg is the stable substring of the error raised when a
	// rewrite value is not a single SQL string (an array or object). It lets
	// extractInlineRewrites distinguish that user-input rejection from other
	// parse errors it should defer to the parser.
	nonStringRewriteMsg = "must be a single SQL string"
)

// rewriteHintPayload is the JSON structure embedded in the hint comment.
type rewriteHintPayload struct {
	Rewrites map[string]string `json:"rewrites"`
	// RemapDb maps a source database name to a target database name. It is
	// applied before the table Rewrites.
	RemapDb map[string]string `json:"remapdb"`
}

// rewritePolicySnapshot captures optional rewrites at the request boundary.
// Each staged statement uses its current SQL mode and active role catalog.
type rewritePolicySnapshot struct {
	enabled        bool
	sessionEnabled bool
	roleRules      *roleRuleCache
	sessionRules   map[string]string
	sessionRemapDb map[string]string
}

func cloneRewriteMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (policy *rewritePolicySnapshot) hasRoleRules() bool {
	return policy != nil && policy.roleRules != nil && len(policy.roleRules.first) > 0
}

var captureRoleRuleCache = func(ctx context.Context, ses *Session) (*roleRuleCache, error) {
	if ses.GetTenantInfo() == nil {
		return &roleRuleCache{}, nil
	}
	snapshot, err := readStatementAuthorizationSnapshot(ctx, ses, true)
	if err != nil {
		return nil, err
	}
	if snapshot.IsEmpty() && ses.GetFromRealUser() {
		return nil, moerr.NewInternalError(ctx, "authorization snapshot is unavailable for role rules")
	}
	storage := getPu(ses.GetService()).StorageEngine
	reader, _ := storage.(engine.TableContentReader)
	principal := currentAuthorizationPrincipal(ses)

	ses.ruleCacheMu.Lock()
	cache := ses.ruleCache
	if cache != nil && cache.proof.valid(ctx, principal, reader, snapshot, ses.ruleCacheObserved[:]) {
		ses.ruleCacheMu.Unlock()
		return cache, nil
	}
	ses.ruleCacheMu.Unlock()

	rules, err := loadRoleRuleCacheAtSnapshot(ctx, ses, snapshot, storage, reader)
	if err != nil {
		ses.Error(ctx, "failed to load rewrite rule cache", logutil.ErrorField(err))
		return nil, err
	}
	ses.ruleCacheMu.Lock()
	ses.ruleCache = rules
	ses.ruleCacheMu.Unlock()
	return rules, nil
}

func captureRewritePolicy(ctx context.Context, ses *Session) (*rewritePolicySnapshot, error) {
	policy := &rewritePolicySnapshot{sessionEnabled: ses.rewriteEnabled.Load()}
	if policy.sessionEnabled {
		policy.sessionRules, policy.sessionRemapDb = getSessionRewriteRules(ctx, ses)
		policy.sessionRules = cloneRewriteMap(policy.sessionRules)
		policy.sessionRemapDb = cloneRewriteMap(policy.sessionRemapDb)
	}
	return policy.withCurrentRoleRules(ctx, ses)
}

func (policy *rewritePolicySnapshot) withCurrentRoleRules(ctx context.Context, ses *Session) (*rewritePolicySnapshot, error) {
	cache, err := captureRoleRuleCache(ctx, ses)
	if err != nil {
		return nil, err
	}
	next := *policy
	next.roleRules = cache
	next.enabled = next.sessionEnabled || next.hasRoleRules()
	return &next, nil
}

// validateRemapDb checks a remapdb map: every source/target is a valid database
// identifier, and the set of source databases is disjoint from the set of
// target databases. The disjointness rule forbids chaining/ambiguity such as
// {"x":"y","y":"z"} (y is both a target and a source) and self-maps {"x":"x"}.
func validateRemapDb(ctx context.Context, remapDb map[string]string) error {
	if len(remapDb) == 0 {
		return nil
	}
	sources := make(map[string]struct{}, len(remapDb))
	for src, dst := range remapDb {
		if !isValidDbIdentifier(src) || !isValidDbIdentifier(dst) {
			return moerr.NewInvalidInputf(ctx, "remapdb names must be valid identifiers, got %q -> %q", src, dst)
		}
		if parsers.IsSystemDatabase(src) || parsers.IsSystemDatabase(dst) {
			return moerr.NewInvalidInputf(ctx, "remapdb must not remap a system database, got %q -> %q", src, dst)
		}
		sources[src] = struct{}{}
	}
	for _, dst := range remapDb {
		if _, ok := sources[dst]; ok {
			return moerr.NewInvalidInputf(ctx, "remapdb: database %q must not be both a source and a destination (chaining is not allowed)", dst)
		}
	}
	return nil
}

// isValidDbIdentifier reports whether s is a usable (unquoted) database name:
// non-empty and composed only of letters, digits, underscore or '$'. remapdb
// only substitutes a name, so both sides must be plain identifiers.
func isValidDbIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r == '_' || r == '$' ||
			(r >= '0' && r <= '9') ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') {
			continue
		}
		return false
	}
	return true
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

// formatRewriteHintChains serializes per-table rewrite chains into the
// /*+ {"rewrites": {...}} */ hint format. A chain with a single layer is
// emitted as a plain string (keeping the common case identical to
// formatRewriteHint); a chain with multiple layers is emitted as an ordered
// JSON array (innermost first, outermost last). Empty chains are skipped.
func formatRewriteHintChains(ctx context.Context, chains map[string][]string, remapDb map[string]string) (string, error) {
	rewrites := make(map[string]interface{}, len(chains))
	for k, chain := range chains {
		switch len(chain) {
		case 0:
			continue
		case 1:
			rewrites[k] = chain[0]
		default:
			rewrites[k] = chain
		}
	}
	payload := map[string]interface{}{"rewrites": rewrites}
	if len(remapDb) > 0 {
		payload["remapdb"] = remapDb
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return "", moerr.NewInternalErrorf(ctx, "failed to serialize rewrite rules: %v", err)
	}
	// Encode appends a trailing newline, trim it
	// A rule can contain a SQL comment terminator inside a literal. Escape
	// the slash in JSON so it survives the outer SQL hint comment.
	return hintPrefix + strings.ReplaceAll(strings.TrimSpace(buf.String()), "*/", `*\/`) + hintSuffix, nil
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

// rewriteSQL materializes the effective rewrite policy independently on every
// statement in a request. Splitting uses the parser's top-level statement
// boundaries, so semicolons in strings, comments, and compound bodies do not
// create fragments. Blank and comment-only fragments remain untouched to
// preserve alignment with the parser's statement list.
// If ruleCache is nil (not yet loaded), it lazily loads rules via loadRuleCache.
// Rules for the same table key are layered, not overridden: role -> session ->
// inline are emitted as an ordered chain (stacked views), so a session/inline
// rule narrows the role-rewritten relation rather than replacing it. See the
// chain-building comment below for details.
// If the combined rule set is empty, the original SQL is returned unchanged.
// The session switch controls optional rewrites. Active role rules are mandatory.
// Rule cache load failures are returned to the caller so access-control rewrites
// do not silently fall back to the unmodified SQL.
func rewriteSQL(ctx context.Context, ses *Session, sql string) (string, error) {
	policy, err := captureRewritePolicy(ctx, ses)
	if err != nil {
		return sql, err
	}
	return policy.rewrite(ctx, sql, sessionSQLModeForParser(ses))
}

func (policy *rewritePolicySnapshot) rewrite(ctx context.Context, sql string, sqlMode string) (string, error) {
	if policy == nil || !policy.enabled {
		return sql, nil
	}

	fragments, err := parsers.SplitSqlByStatementWithSQLMode(ctx, sql, sqlMode)
	if err != nil {
		if _, ok := err.(*moerr.Error); !ok {
			err = moerr.NewParseError(ctx, err.Error())
		}
		return sql, err
	}
	roleRules, ruleErr := policy.roleRules.forSQLMode(ctx, sqlMode)
	rewrite := func(fragment string) (string, error) {
		if ruleErr != nil && !isRoleRuleRecoveryStatement(ctx, fragment, sqlMode) {
			return fragment, ruleErr
		}
		return rewriteSingleSQL(ctx, fragment, roleRules, policy.sessionRules, policy.sessionRemapDb, policy.sessionEnabled)
	}
	if len(fragments) == 1 {
		if !parsers.FragmentHasStatement(sql) {
			return sql, nil
		}
		return rewrite(sql)
	}

	for i, fragment := range fragments {
		if !parsers.FragmentHasStatement(fragment) {
			continue
		}
		rewritten, err := rewrite(fragment)
		if err != nil {
			return sql, err
		}
		fragments[i] = rewritten
	}
	rewritten := strings.Join(fragments, ";")
	if strings.HasSuffix(strings.TrimSpace(sql), ";") {
		rewritten += ";"
	}
	return rewritten, nil
}

// A catalog load error permits only standalone controls that do not read
// protected rows. Their ordinary authorization and handlers still apply.
func isStandaloneRoleRuleControl(ctx context.Context, sql, sqlMode string) bool {
	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, sql, 1, sqlMode)
	if err != nil {
		return false
	}
	defer stmt.Free()
	switch stmt.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction, *tree.Kill:
		return true
	}
	return false
}

// Recovery can end a transaction, reset policy-dependent session settings or
// drop a table. These operations still undergo normal object authorization.
func isRoleRuleRecoveryStatement(ctx context.Context, sql, sqlMode string) bool {
	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, sql, 1, sqlMode)
	if err != nil {
		return false
	}
	defer stmt.Free()
	switch s := stmt.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction, *tree.Kill, *tree.DropTable,
		*tree.AlterRoleAddRule, *tree.AlterRoleDropRule:
		return true
	case *tree.SetRole:
		return s.SecondaryRole && s.SecondaryRoleType == tree.SecondaryRoleTypeNone
	case *tree.SetVar:
		// General SET can contain a subquery or additional assignments.
		if len(s.Assignments) == 1 {
			return isStandaloneSQLModeAssignment(s.Assignments[0])
		}
	}
	return false
}

func isStandaloneSQLModeAssignment(a *tree.VarAssignmentExpr) bool {
	if !a.System || a.Global || !strings.EqualFold(a.Name, "sql_mode") || a.Reserved != nil {
		return false
	}
	if value, ok := a.Value.(*tree.NumVal); ok {
		return value.ValType == tree.P_char
	}
	_, ok := a.Value.(*tree.DefaultVal)
	return ok
}

// rewriteSingleSQL merges a policy snapshot with one statement's own leading
// inline JSON hint. The wrapper above loads/captures role and session policy
// once per request and passes the same snapshot to every statement.
func rewriteSingleSQL(
	ctx context.Context,
	sql string,
	cache map[string]string,
	sessionRules map[string]string,
	sessionRemapDb map[string]string,
	sessionEnabled bool,
) (string, error) {
	// Inline /*+ {...} */ rewrites and database remaps. A user-provided rewrite
	// value must be a single SQL string; arrays/objects are rejected here. The
	// array form exists only internally, to carry the role->session->inline
	// stacked-view chain to the planner. Validate the inline hint up front so an
	// invalid value is caught even when there is no role/session config to merge.
	var inlineRules, inlineRemapDb map[string]string
	if sessionEnabled {
		var err error
		inlineRules, inlineRemapDb, err = extractInlineRewrites(ctx, sql)
		if err != nil {
			return sql, err
		}
	}

	if len(cache) == 0 && len(sessionRules) == 0 && len(sessionRemapDb) == 0 &&
		len(inlineRules) == 0 && len(inlineRemapDb) == 0 {
		// Nothing to apply. Leave any non-rewrite inline hint for the parser.
		return sql, nil
	}

	// Layering: a table's rewrites are applied as stacked views in the order
	// role rule -> session variable -> inline hint. Each layer is a view over
	// the previous one (a reference to the table inside a session/inline rule
	// resolves to the role-rewritten relation, etc.), so later layers narrow or
	// transform the earlier ones rather than replacing them. The inline hint is
	// part of this single statement's text, so its layer is effective for this
	// one query only. All layers are emitted as a single hint because the
	// parser only honors the first leading hint; a second hint of our own would
	// otherwise mask the inline one. Each source contributes one string layer;
	// the multi-element array only appears in the merged hint we emit below.
	//
	// remapdb (database-name substitution) is applied before the table rewrites
	// and does not stack: a database maps to one target, so the inline hint
	// overrides the session variable for the same source database.
	chains := make(map[string][]string, len(cache)+len(sessionRules)+len(inlineRules))
	for k, v := range cache {
		chains[k] = append(chains[k], v)
	}
	for k, v := range sessionRules {
		chains[k] = append(chains[k], v)
	}
	for k, v := range inlineRules {
		chains[k] = append(chains[k], v)
	}

	remapDb := make(map[string]string, len(sessionRemapDb)+len(inlineRemapDb))
	for k, v := range sessionRemapDb {
		remapDb[k] = v
	}
	for k, v := range inlineRemapDb {
		remapDb[k] = v
	}

	hint, err := formatRewriteHintChains(ctx, chains, remapDb)
	if err != nil {
		return sql, err
	}

	return hint + " " + sql, nil
}

// rewriteSQLFromMaterializedPolicy applies the policy already captured on the
// outer statement to SQL nested in PREPARE ... FROM 'sql'/@var, then layers the
// nested SQL's own inline hint on top. It intentionally does not read session
// or role state again: earlier statements in the same COM_QUERY may already
// have changed that state after the request was parsed.
func rewriteSQLFromMaterializedPolicy(ctx context.Context, outerSQL, innerSQL string, sessionEnabled bool) (string, error) {
	chains := make(map[string][]string)
	remapDb := make(map[string]string)
	if content, ok := leadingHintContent(outerSQL); ok {
		content = strings.TrimSpace(content)
		if content != "" && content[0] == '{' {
			outerChains, outerRemapDb, err := parsers.DecodeRewriteHint(ctx, content)
			if err != nil {
				return innerSQL, err
			}
			for key, chain := range outerChains {
				chains[key] = append(chains[key], chain...)
			}
			for src, dst := range outerRemapDb {
				remapDb[src] = dst
			}
		}
	}

	if sessionEnabled {
		inlineRules, inlineRemapDb, err := extractInlineRewrites(ctx, innerSQL)
		if err != nil {
			return innerSQL, err
		}
		for key, rule := range inlineRules {
			chains[key] = append(chains[key], rule)
		}
		for src, dst := range inlineRemapDb {
			remapDb[src] = dst
		}
	}
	if len(chains) == 0 && len(remapDb) == 0 {
		return innerSQL, nil
	}

	hint, err := formatRewriteHintChains(ctx, chains, remapDb)
	if err != nil {
		return innerSQL, err
	}
	return hint + " " + innerSQL, nil
}

// addStatementRewriteHints preserves the normal privilege path while requiring
// a rewrite-capable execution path for statements that can read table data.
func addStatementRewriteHints(ctx context.Context, stmts []tree.Statement, sql, sqlMode string, input *UserInput) error {
	if input == nil || !input.rewritePolicy.hasRoleRules() {
		return parsers.AddRewriteHintsWithSQLMode(ctx, stmts, sql, sqlMode)
	}
	targets := make([]tree.Statement, len(stmts))
	var appliesRules bool
	for i, stmt := range stmts {
		target := authorizationStatement(stmt)
		rewrite, supported := roleRuleStatementSupport(target)
		if !supported {
			return moerr.NewNotSupported(ctx, "statement cannot enforce active role rules")
		}
		appliesRules = appliesRules || rewrite
		targets[i] = target
	}
	var roleRules map[string]string
	lower := int64(1)
	var provenFoldedTargets map[string]uint64
	if appliesRules {
		var err error
		cache := input.rewritePolicy.roleRules
		lower = cache.lowerCaseTableNames
		provenFoldedTargets = cache.provenFoldedTargets
		roleRules, err = cache.forSQLMode(ctx, sqlMode)
		if err != nil {
			return err
		}
	}
	return parsers.AddRewriteHintsWithRoleRules(ctx, targets, sql, sqlMode, roleRules, lower, provenFoldedTargets)
}

// roleRuleStatementSupport classifies statements by whether planning binds the
// active rules. PREPARE, EXECUTE and SET validate their nested statements at
// their respective planning boundaries. Metadata SHOW is limited to row-local
// expressions because its synthesized queries do not carry role rules.
func roleRuleStatementSupport(stmt tree.Statement) (rewrite, supported bool) {
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.Insert:
		return true, true
	case *tree.Update:
		if stmt.With != nil || stmt.MultiTable || stmt.From != nil || stmt.HasReturning() ||
			!roleRuleWriteRowLocal(stmt.Where, stmt.OrderBy, stmt.Limit) {
			return false, false
		}
		for _, assignment := range stmt.Exprs {
			if !rowLocalExpr(assignment.Expr, true) {
				return false, false
			}
		}
		return true, true
	case *tree.Delete:
		if stmt.With != nil || stmt.TableRefs != nil || stmt.HasReturning() ||
			!roleRuleWriteRowLocal(stmt.Where, stmt.OrderBy, stmt.Limit) {
			return false, false
		}
		return true, true
	case *tree.DumpTable, *tree.MoDump, *tree.BackupStart:
		// Physical export does not pass the exported rows through the binder.
		return false, false
	case *tree.CreateTable:
		return stmt.AsSource != nil, true
	case *tree.CreateView, *tree.AlterView, *tree.CloneTable, *tree.CloneDatabase, *tree.CallStmt,
		*tree.ExecuteSQLTask, *tree.ShowTableValues, *tree.DataBranchCreateTable,
		*tree.DataBranchCreateDatabase, *tree.DataBranchDiff, *tree.DataBranchMerge,
		*tree.DataBranchPick, *tree.RestoreSnapShot, *tree.RestorePitr:
		return false, false
	case *tree.ShowColumns:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowDatabases:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowTarget:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowTableStatus:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowTables:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowCollation:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowVariables:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowStatus:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowFunctionOrProcedureStatus:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowIcebergCatalogs:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowIcebergNamespaces:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowIcebergTables:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowMongoDBConnections:
		return false, roleRuleMetadataFilterSupported(stmt.Where, stmt.Like)
	case *tree.ShowIndex:
		return false, roleRuleMetadataFilterSupported(stmt.Where, nil)
	case *tree.ShowSequences:
		return false, roleRuleMetadataFilterSupported(stmt.Where, nil)
	case *tree.ShowAccounts:
		return false, roleRuleMetadataFilterSupported(nil, stmt.Like)
	case *tree.ShowPublications:
		return false, roleRuleMetadataFilterSupported(nil, stmt.Like)
	case *tree.ShowSubscriptions:
		return false, roleRuleMetadataFilterSupported(nil, stmt.Like)
	case *tree.ShowRolesStmt:
		return false, roleRuleMetadataFilterSupported(nil, stmt.Like)
	case *tree.ShowStages:
		return false, roleRuleMetadataFilterSupported(nil, stmt.Like)
	default:
		kind := stmt.GetQueryType()
		return false, kind != tree.QueryTypeDML && kind != tree.QueryTypeDQL
	}
}

func roleRuleWriteRowLocal(where *tree.Where, order tree.OrderBy, limit *tree.Limit) bool {
	if where != nil && !rowLocalExpr(where.Expr, true) {
		return false
	}
	for _, term := range order {
		if !rowLocalExpr(term.Expr, true) {
			return false
		}
	}
	return limit == nil || (rowLocalExpr(limit.Offset, true) && rowLocalExpr(limit.Count, true))
}

func roleRuleMetadataFilterSupported(where *tree.Where, like *tree.ComparisonExpr) bool {
	return (where == nil || showMetadataRowLocalExpr(where.Expr)) &&
		(like == nil || showMetadataRowLocalExpr(like))
}

// applyRoleRulesToSetExpression covers the SELECT synthesized by SET, which
// enters planning with an AST in place of materialized SQL.
func applyRoleRulesToSetExpression(execCtx *ExecCtx, ses *Session, stmt *tree.Select, expr tree.Expr, preparedExpression bool) error {
	switch expr.(type) {
	case *tree.NumVal, *tree.DefaultVal:
		// Literals also allow SQL mode recovery when a rule cannot be parsed.
		return nil
	}
	var cache *roleRuleCache
	if preparedExpression && execCtx.prepareStmt != nil {
		cache = execCtx.prepareStmt.roleRules
	} else if execCtx.input != nil && execCtx.input.rewritePolicy != nil {
		cache = execCtx.input.rewritePolicy.roleRules
	}
	if cache == nil || len(cache.first) == 0 {
		return nil
	}

	ctx := execCtx.reqCtx
	sqlMode := sessionSQLModeForParser(ses)
	rules, err := cache.forSQLMode(ctx, sqlMode)
	if err != nil {
		return err
	}
	chains := make(map[string][]string, len(rules))
	for key, rule := range rules {
		chains[key] = []string{rule}
	}
	hint, err := formatRewriteHintChains(ctx, chains, nil)
	if err != nil {
		return err
	}
	return parsers.AddRewriteHintsWithRoleRules(ctx, []tree.Statement{stmt}, hint+" select 1", sqlMode, rules, cache.lowerCaseTableNames, cache.provenFoldedTargets)
}

func (prepareStmt *PrepareStmt) checkRoleRulePolicy(ctx context.Context, ses *Session) error {
	current, err := captureRoleRuleCache(ctx, ses)
	if err != nil {
		return err
	}
	if prepareStmt.roleRules == nil {
		if len(current.first) > 0 {
			return moerr.NewInvalidInput(ctx, "prepared statement is missing its active role rules; prepare it again")
		}
		return nil
	}
	if len(prepareStmt.roleRules.first) == 0 && len(current.first) == 0 {
		return nil
	}
	mode := sessionSQLModeForParser(ses)
	if current.lowerCaseTableNames == prepareStmt.roleRules.lowerCaseTableNames &&
		mysql.ParseSQLModeFlags(mode) == mysql.ParseSQLModeFlags(prepareStmt.schedulingSQLMode) {
		if current == prepareStmt.roleRules {
			return nil
		}
		oldRules, err := prepareStmt.roleRules.forSQLMode(ctx, mode)
		if err != nil {
			return err
		}
		activeRules, err := current.forSQLMode(ctx, mode)
		if err != nil {
			return err
		}
		if maps.Equal(oldRules, activeRules) && maps.Equal(prepareStmt.roleRules.provenFoldedTargets, current.provenFoldedTargets) {
			return nil
		}
	}
	return moerr.NewInvalidInput(ctx, "active role rule policy changed; prepare the statement again")
}

// extractInlineRewrites returns the per-table rewrite rules and database-remap
// entries from a leading /*+ {...} */ (or /*!+ {...} */) hint on sql, if present.
// Each rewrite value must be a single SQL string: a user-written inline hint may
// not use the array (chain) form — that form is reserved for the merged hint
// rewriteSQL emits internally. An array/object value is rejected with an error.
// The original sql is left untouched; the rules are merged into the single hint
// rewriteSQL prepends.
func extractInlineRewrites(ctx context.Context, sql string) (rewrites map[string]string, remapDb map[string]string, err error) {
	content, ok := leadingHintContent(sql)
	if !ok {
		return nil, nil, nil
	}
	content = strings.TrimSpace(content)
	if content == "" || content[0] != '{' {
		return nil, nil, nil
	}
	// parseSessionRewrites is string-only (stringRewrites rejects arrays/objects).
	rewrites, remapDb, err = parseSessionRewrites(ctx, content)
	if err != nil {
		// Surface the array/object rejection (the user-input rule we enforce here);
		// leave every other parse problem (e.g. malformed JSON) for the parser's
		// AddRewriteHints, so the error matches the no-config path.
		if strings.Contains(err.Error(), nonStringRewriteMsg) {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	return rewrites, remapDb, nil
}

// extractInlineRemapDb returns only the remapdb entries from a leading
// /*+ {...} */ hint, decoding the remapdb field alone and ignoring rewrites.
// This is what the executor needs to apply database remapping, and it stays
// correct even when the merged hint carries layered (array-form) rewrites that
// parseSessionRewrites would reject.
func extractInlineRemapDb(sql string) map[string]string {
	content, ok := leadingHintContent(sql)
	if !ok {
		return nil
	}
	content = strings.TrimSpace(content)
	if content == "" || content[0] != '{' {
		return nil
	}
	var payload struct {
		RemapDb map[string]string `json:"remapdb"`
	}
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return nil
	}
	return payload.RemapDb
}

// extractRemapDbByStatement returns the effective remap carried by every real
// statement-bearing fragment. rewriteSQL materializes the role/session/inline
// merge into each fragment independently, so this preserves those boundaries
// through parsing and execution.
func extractRemapDbByStatement(ctx context.Context, sql string) ([]map[string]string, error) {
	return extractRemapDbByStatementWithSQLMode(ctx, sql, "")
}

func extractRemapDbByStatementWithSQLMode(ctx context.Context, sql string, sqlMode string) ([]map[string]string, error) {
	fragments, err := parsers.SplitSqlByStatementWithSQLMode(ctx, sql, sqlMode)
	if err != nil {
		return nil, err
	}
	remaps := make([]map[string]string, 0, len(fragments))
	for _, fragment := range fragments {
		if parsers.FragmentHasStatement(fragment) {
			remaps = append(remaps, extractInlineRemapDb(fragment))
		}
	}
	if len(remaps) == 0 {
		return []map[string]string{nil}, nil
	}
	return remaps, nil
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
// its table-rewrite rules and database-remap entries. An empty/unset variable
// yields nothing. The value is validated at SET time (validateRemapRewrites), so
// a parse error here is unexpected and is treated as "no session config" rather
// than failing every query.
func getSessionRewriteRules(ctx context.Context, ses *Session) (rewrites map[string]string, remapDb map[string]string) {
	v, err := ses.GetSessionSysVar("remap_rewrites")
	if err != nil {
		// remap_rewrites is always registered; a lookup error should not block
		// the query, so fall back to "no session config".
		return nil, nil
	}
	raw, ok := v.(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	rewrites, remapDb, err = parseSessionRewrites(ctx, raw)
	if err != nil {
		ses.Error(ctx, "failed to parse remap_rewrites; ignoring session rewrites",
			logutil.ErrorField(err))
		return nil, nil
	}
	return rewrites, remapDb
}

// validateRewriteTableKey ensures a rewrite rule's table key is qualified as
// database.table (both parts present). An unqualified key such as "t" is
// rejected: it can never match a table reference, so the rule would silently do
// nothing (issue #25188). This mirrors the parser's check on inline-hint keys.
func validateRewriteTableKey(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return moerr.NewInvalidInput(ctx, "remap_rewrites: table key must not be empty")
	}
	// Same qualification rule the parser enforces on inline-hint keys, so a value
	// accepted at SET time never fails later when the hint is re-parsed.
	if _, _, ok := parsers.SplitRewriteKey(key); !ok {
		return moerr.NewInvalidInputf(ctx, "remap_rewrites: rewrite table %q must be qualified as database.table", key)
	}
	return nil
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
	rules, remapDb, err := parseSessionRewrites(ctx, raw)
	if err != nil {
		return err
	}
	for key, rule := range rules {
		if err := validateRewriteTableKey(ctx, key); err != nil {
			return err
		}
		if err := validateRewriteRuleSQL(ctx, rule, 1, ""); err != nil {
			return err
		}
	}
	if err := validateRemapDb(ctx, remapDb); err != nil {
		return err
	}
	return nil
}

// parseSessionRewrites parses a remap_rewrites value or an inline hint payload
// into its table-rewrite rules and database-remap entries. It accepts both the
// wrapped form {"rewrites": {...}, "remapdb": {...}} and the bare form
// {"db.t": "select ..."} (rewrites only, no remapdb).
func parseSessionRewrites(ctx context.Context, value string) (rewrites map[string]string, remapDb map[string]string, err error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil, nil
	}

	// A rewrite value must be a single SQL string: a table name maps to exactly
	// one query. The chained-array form is internal to rewriteSQL/the parser and
	// is never accepted here.
	var wrapped struct {
		Rewrites map[string]json.RawMessage `json:"rewrites"`
		RemapDb  map[string]string          `json:"remapdb"`
	}
	if err := json.Unmarshal([]byte(value), &wrapped); err == nil &&
		(wrapped.Rewrites != nil || wrapped.RemapDb != nil) {
		rules, derr := stringRewrites(ctx, value, wrapped.Rewrites)
		if derr != nil {
			return nil, nil, derr
		}
		return rules, wrapped.RemapDb, nil
	}

	var bare map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &bare); err != nil {
		return nil, nil, moerr.NewInvalidInputf(ctx, "invalid remap_rewrites value %q: %v", value, err)
	}
	rules, derr := stringRewrites(ctx, value, bare)
	if derr != nil {
		return nil, nil, derr
	}
	return rules, nil, nil
}

// stringRewrites requires every rewrite value to be a single SQL string. An
// array (a layered chain) or any other JSON type is rejected: a table maps to
// one query, not a list.
func stringRewrites(ctx context.Context, value string, raw map[string]json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for k, rv := range raw {
		var s string
		if err := json.Unmarshal(rv, &s); err != nil {
			return nil, moerr.NewInvalidInputf(ctx, "invalid remap_rewrites value %q: rewrite for %q %s, not an array or object", value, k, nonStringRewriteMsg)
		}
		out[k] = s
	}
	return out, nil
}

// Catalog text is immutable after publication. Each request keeps the captured
// generation, but each statement interprets it under its own parser mode.
type roleRuleCache struct {
	lowerCaseTableNames int64
	caseCollision       bool
	provenFoldedTargets map[string]uint64
	proof               roleRuleCacheProof
	first               map[string]string
	later               map[string][]string // competing rules only, in active-role priority order
	hasNonUTF8          bool
	last                atomic.Pointer[mergedRoleRules]
}

type mergedRoleRules struct {
	mode  mysql.SQLModeFlags
	rules map[string]string
}

func (c *roleRuleCache) forSQLMode(ctx context.Context, sqlMode string) (map[string]string, error) {
	if c == nil {
		return nil, nil
	}
	if c.caseCollision {
		return nil, moerr.NewInvalidInput(ctx, "active role rules contain ambiguous case spellings")
	}
	if len(c.later) == 0 && !c.hasNonUTF8 {
		return c.first, nil
	}
	mode := mysql.ParseSQLModeFlags(sqlMode)
	if last := c.last.Load(); last != nil && last.mode == mode {
		return last.rules, nil
	}
	rules := cloneRewriteMap(c.first)
	for name, later := range c.later {
		for _, rule := range later {
			merged, err := mergeRewriteRules(ctx, c.lowerCaseTableNames, rules[name], rule, sqlMode)
			if err != nil {
				return nil, err
			}
			rules[name] = merged
		}
	}
	for name, rule := range rules {
		hintSQL, err := roleRuleHintSQL(ctx, rule, mode)
		if err != nil {
			return nil, err
		}
		rules[name] = hintSQL
	}
	c.last.Store(&mergedRoleRules{mode: mode, rules: rules})
	return rules, nil
}

// roleRuleHintSQL encodes binary tokens only after rule selection and merging.
// JSON replaces invalid UTF-8, so any remaining invalid byte must fail closed.
func roleRuleHintSQL(ctx context.Context, rule string, mode mysql.SQLModeFlags) (string, error) {
	if utf8.ValidString(rule) {
		return rule, nil
	}
	scanner := mysql.NewScannerWithSQLMode(dialect.MYSQL, rule, mode)
	defer mysql.PutScanner(scanner)
	var out strings.Builder
	var binary bool
	copied := 0
	for {
		token, value := scanner.ScanWithComments()
		if token == 0 || token == mysql.EofChar() || token == mysql.LEX_ERROR {
			break
		}
		if token == mysql.COMMENT {
			continue
		}
		if binary && token == mysql.STRING && !utf8.ValidString(value) {
			out.WriteString(rule[copied:scanner.PrePos])
			// Separate both sides because quotes can delimit SQL tokens
			// without whitespace, but a hexadecimal number cannot.
			fmt.Fprintf(&out, " 0x%x ", value)
			copied = scanner.Pos
		}
		binary = token == mysql.UNDERSCORE_BINARY
	}
	if copied != 0 {
		out.WriteString(rule[copied:])
		if sql := out.String(); utf8.ValidString(sql) {
			return sql, nil
		}
	}
	return "", moerr.NewInvalidInput(ctx, "rewrite rule contains invalid UTF-8 outside binary literals")
}

const roleRuleDependencyCount = 5

type roleRuleCacheProof struct {
	principal    authorizationPrincipal
	snapshot     timestamp.Timestamp
	dependencies [roleRuleDependencyCount]engine.TableContentDependency
	versions     [roleRuleDependencyCount]engine.TableContentVersion
}

func (proof *roleRuleCacheProof) valid(ctx context.Context, principal authorizationPrincipal, reader engine.TableContentReader, snapshot timestamp.Timestamp, observed []engine.TableContentVersion) bool {
	return reader != nil && !proof.snapshot.IsEmpty() && !snapshot.Less(proof.snapshot) &&
		proof.principal == principal &&
		reader.ReadTableContentVersions(ctx, snapshot, proof.dependencies[:], observed) &&
		slices.Equal(observed, proof.versions[:])
}

func captureRoleRuleCacheProof(ctx context.Context, ses *Session, bh BackgroundExec, snapshot timestamp.Timestamp, storage engine.Engine, reader engine.TableContentReader) (proof roleRuleCacheProof) {
	if reader == nil || snapshot.IsEmpty() {
		return proof
	}
	txnSource, ok := bh.(interface{ GetTxnOperator() TxnOperator })
	if !ok {
		return proof
	}
	op := txnSource.GetTxnOperator()
	if op == nil || !op.SnapshotTS().Equal(snapshot) || op.Txn().Isolation != pbtxn.TxnIsolation_SI || !op.TxnOptions().ReadOnly() {
		return proof
	}
	proof.principal = currentAuthorizationPrincipal(ses)
	// The physical rule and role-grant tables provide data versions. The
	// system table catalog also covers changes to their name bindings.
	names := [roleRuleDependencyCount]string{catalog.MO_ROLE_RULE, "mo_user_grant", "mo_role_grant", catalog.MO_TABLES, catalog.MO_DATABASE}
	for i, name := range names {
		account := proof.principal.account
		if name == catalog.MO_TABLES || name == catalog.MO_DATABASE {
			account = sysAccountID
		}
		tableCtx := defines.AttachAccountId(ctx, account)
		db, err := storage.Database(tableCtx, catalog.MO_CATALOG, op)
		if err != nil {
			return roleRuleCacheProof{}
		}
		dbID, err := strconv.ParseUint(db.GetDatabaseId(tableCtx), 10, 64)
		if err != nil {
			return roleRuleCacheProof{}
		}
		relation, err := db.Relation(tableCtx, name, nil)
		if err != nil || relation == nil {
			return roleRuleCacheProof{}
		}
		tableID := relation.GetTableID(tableCtx)
		if err := storage.TryToSubscribeTable(tableCtx, uint64(account), dbID, tableID, catalog.MO_CATALOG, name); err != nil {
			return roleRuleCacheProof{}
		}
		proof.dependencies[i] = engine.TableContentDependency{DatabaseID: dbID, TableID: tableID}
	}
	if reader.ReadTableContentVersions(ctx, snapshot, proof.dependencies[:], proof.versions[:]) {
		proof.snapshot = snapshot
	}
	return proof
}

func loadRoleRuleCacheAtSnapshot(ctx context.Context, ses *Session, snapshot timestamp.Timestamp, storage engine.Engine, reader engine.TableContentReader) (rules *roleRuleCache, err error) {
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true, readSnapshot: snapshot})
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return nil, err
	}
	defer func() { err = finishTxn(ctx, bh, err) }()

	proof := captureRoleRuleCacheProof(ctx, ses, bh, snapshot, storage, reader)
	// The authenticated session retains its own table-name mode. Read it
	// when loading rules so a cache hit does not need the system-variable lock.
	rules, err = readRoleRules(ctx, bh, ses.GetTenantInfo(), parserLowerCaseTableNames(ses))
	if err != nil {
		return nil, err
	}
	if rules.lowerCaseTableNames != 1 {
		if source, ok := bh.(interface{ GetTxnOperator() TxnOperator }); ok {
			if op := source.GetTxnOperator(); op != nil {
				rules.provenFoldedTargets = proveFoldedRoleTargets(ctx, storage, op, rules.first)
			}
		}
	}
	var observed [roleRuleDependencyCount]engine.TableContentVersion
	if proof.valid(ctx, currentAuthorizationPrincipal(ses), reader, snapshot, observed[:]) {
		rules.proof = proof
	}
	return rules, nil
}

// proveFoldedRoleTargets enumerates the directory once per authorization
// generation, at the same catalog snapshot that supplied the role rows.
// A unique exact lowercase relation is sufficient for that spelling only.
func proveFoldedRoleTargets(ctx context.Context, storage engine.Engine, txn TxnOperator, rules map[string]string) map[string]uint64 {
	targets := make(map[string][]string)
	for key := range rules {
		db, table, valid := parsers.SplitRewriteKey(key)
		if valid && key == db+"."+table && key == strings.ToLower(key) && !parsers.RoleRuleAlwaysFolds(db) {
			targets[db] = append(targets[db], table)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	names, err := storage.Databases(ctx, txn)
	if err != nil {
		return nil
	}
	dbMatches := make(map[string]int, len(targets))
	for _, name := range names {
		folded := strings.ToLower(name)
		if _, wanted := targets[folded]; wanted {
			dbMatches[folded]++
		}
	}
	proven := make(map[string]uint64)
	for db, tables := range targets {
		if dbMatches[db] != 1 || !slices.Contains(names, db) {
			continue
		}
		database, err := storage.Database(ctx, db, txn)
		if err != nil || database == nil || database.IsSubscription(ctx) {
			continue
		}
		relations, err := database.Relations(ctx)
		if err != nil {
			continue
		}
		counts := make(map[string]int, len(relations))
		for _, relation := range relations {
			counts[strings.ToLower(relation)]++
		}
		for _, table := range tables {
			if counts[table] != 1 || !slices.Contains(relations, table) {
				continue
			}
			relation, err := database.Relation(ctx, table, nil)
			if err == nil && relation != nil {
				proven[db+"."+table] = relation.GetTableID(ctx)
			}
		}
	}
	return proven
}

func readRoleRules(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, lower int64) (*roleRuleCache, error) {
	rules := &roleRuleCache{first: make(map[string]string), lowerCaseTableNames: lower}

	roleIDs, err := loadActiveRoleIDsForRuleCache(ctx, bh, tenant)
	if err != nil {
		return nil, err
	}
	if len(roleIDs) == 0 {
		return rules, nil
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
		rules.hasNonUTF8 = rules.hasNonUTF8 || !utf8.ValidString(rule)
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
	slices.SortStableFunc(ruleRows, func(a, b rewriteRuleRow) int {
		aPriority, aOK := rolePriority[a.roleID]
		bPriority, bOK := rolePriority[b.roleID]
		if !aOK {
			aPriority = len(rolePriority)
		}
		if !bOK {
			bPriority = len(rolePriority)
		}
		if aPriority != bPriority {
			return cmp.Compare(aPriority, bPriority)
		}
		return cmp.Compare(a.ruleName, b.ruleName)
	})

	rawSpellings := make(map[string]string, len(ruleRows))
	for _, row := range ruleRows {
		if db, table, ok := parsers.SplitRewriteKey(row.ruleName); ok {
			key := parsers.RoleRuleKey(db, table, lower)
			if prior, exists := rawSpellings[key]; exists && prior != row.ruleName {
				rules.caseCollision = true
			}
			rawSpellings[key] = row.ruleName
			row.ruleName = key
		}
		if _, ok := rules.first[row.ruleName]; !ok {
			rules.first[row.ruleName] = row.rule
			continue
		}
		if rules.later == nil {
			rules.later = make(map[string][]string)
		}
		rules.later[row.ruleName] = append(rules.later[row.ruleName], row.rule)
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

func mergeRewriteRules(ctx context.Context, lower int64, leftRule, rightRule, sqlMode string) (string, error) {
	leftRule = trimRewriteRuleForMerge(leftRule)
	rightRule = trimRewriteRuleForMerge(rightRule)

	if leftRule == rightRule {
		return leftRule, nil
	}

	mergedRule, ok, err := mergeRewriteRulesSafely(ctx, lower, leftRule, rightRule, sqlMode)
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
func mergeRewriteRulesSafely(ctx context.Context, lower int64, leftRule, rightRule, sqlMode string) (string, bool, error) {
	leftShape, ok, err := rewriteRuleMergeShapeForRule(ctx, lower, leftRule, sqlMode)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	rightShape, ok, err := rewriteRuleMergeShapeForRule(ctx, lower, rightRule, sqlMode)
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

	// The merged rule is reparsed as SQL, so literals must retain their string type and value.
	return tree.StringWithOpts(&merged, dialect.MYSQL, rewriteRuleFormatOptions(sqlMode)...), true, nil
}

func rewriteRuleFormatOptions(sqlMode string) []tree.FmtCtxOption {
	opts := []tree.FmtCtxOption{tree.WithSingleQuoteString(), tree.WithQuoteIdentifier(), tree.WithByteExactBinaryLiterals()}
	if mysql.ParseSQLModeFlags(sqlMode).Has(mysql.SQLModeNoBackslashEscapes) {
		opts = append(opts, tree.WithNoBackslashEscape())
	}
	return opts
}

func rewriteRuleMergeShapeForRule(ctx context.Context, lower int64, rule, sqlMode string) (*rewriteRuleMergeShape, bool, error) {
	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, rule, lower, sqlMode)
	if err != nil {
		return nil, false, moerr.NewInternalErrorf(ctx, "failed to parse rewrite rule %q while merging rewrite rules: %v", rule, err)
	}
	shape, ok := rewriteRuleMergeShapeForStatement(stmt, lower, sqlMode)
	return shape, ok, nil
}

func rewriteRuleMergeShapeForStatement(stmt tree.Statement, lower int64, sqlMode string) (*rewriteRuleMergeShape, bool) {
	switch s := stmt.(type) {
	case *tree.Select:
		return rewriteRuleMergeShapeForSelect(s, lower, sqlMode)
	case *tree.ParenSelect:
		if s.Select == nil {
			return nil, false
		}
		return rewriteRuleMergeShapeForStatement(s.Select, lower, sqlMode)
	default:
		return nil, false
	}
}

func rewriteRuleMergeShapeForSelect(stmt *tree.Select, lower int64, sqlMode string) (*rewriteRuleMergeShape, bool) {
	if stmt == nil || len(stmt.OrderBy) > 0 || stmt.Limit != nil || stmt.With != nil ||
		stmt.TimeWindow != nil || stmt.RankOption != nil || stmt.Ep != nil || stmt.SelectLockInfo != nil {
		return nil, false
	}

	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || !rewriteRuleSelectClauseIsMergeable(clause) {
		return nil, false
	}

	table, ok := rewriteRuleSingleTableSource(clause.From, lower)
	if !ok {
		return nil, false
	}

	return &rewriteRuleMergeShape{
		stmt:       stmt,
		clause:     clause,
		selectList: rewriteRuleProjectionKey(&clause.Exprs, lower, sqlMode),
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

func rewriteRuleSingleTableSource(from *tree.From, lower int64) (string, bool) {
	if from == nil || len(from.Tables) != 1 {
		return "", false
	}

	tableExpr, ok := rewriteRuleSingleTableExpr(from.Tables[0])
	if !ok {
		return "", false
	}

	sql := tree.StringWithOpts(tableExpr, dialect.MYSQL, tree.WithQuoteIdentifier())
	if lower != 1 {
		return sql, true
	}
	return normalizeRewriteSQL(sql), true
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

// Preserve literal spelling and type delimiters while folding the surrounding
// SQL as before. In particular, the column `x` is not the string '`x`'.
func rewriteRuleProjectionKey(exprs *tree.SelectExprs, lower int64, sqlMode string) string {
	if lower != 1 {
		return tree.StringWithOpts(exprs, dialect.MYSQL, rewriteRuleFormatOptions(sqlMode)...)
	}
	var literals []tree.StringLiteralPosition
	opts := append(rewriteRuleFormatOptions(sqlMode), tree.WithStringLiteralPositions(&literals))
	sql := tree.StringWithOpts(exprs, dialect.MYSQL, opts...)
	if len(literals) == 0 {
		return normalizeRewriteSQL(sql)
	}

	var key strings.Builder
	key.Grow(len(sql))
	start := 0
	for _, literal := range literals {
		key.WriteString(strings.ToLower(sql[start:literal.Start]))
		key.WriteString(sql[literal.Start:literal.End])
		start = literal.End
	}
	key.WriteString(strings.ToLower(sql[start:]))
	return strings.TrimSpace(key.String())
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

func validateRewriteRuleSQL(ctx context.Context, rule string, lower int64, sqlMode string) error {
	if strings.TrimSpace(rule) == "" {
		return moerr.NewInvalidInput(ctx, "rewrite rule SQL is empty")
	}

	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, rule, lower, sqlMode)
	if err != nil {
		return moerr.NewInvalidInputf(ctx, "invalid rewrite rule SQL %q: %v", rule, err)
	}
	defer stmt.Free()

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

func deleteMatchingRoleRuleKeys(ctx context.Context, bh BackgroundExec, roleID int64, target string, lower int64) (bool, error) {
	sql := fmt.Sprintf("select rule_name from %s.%s where role_id = %d",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return false, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil || !execResultArrayHasData(results) {
		return false, err
	}
	var keys []string
	for i := uint64(0); i < results[0].GetRowCount(); i++ {
		name, err := results[0].GetString(ctx, i, 0)
		if err != nil {
			return false, err
		}
		if name != target {
			db, table, ok := parsers.SplitRewriteKey(name)
			if !ok || parsers.RoleRuleKey(db, table, lower) != target {
				continue
			}
		}
		keys = append(keys, escapeSQLString(name))
	}
	if len(keys) == 0 {
		return false, nil
	}
	sql = fmt.Sprintf("delete from %s.%s where role_id = %d and cast(rule_name as binary) in (%s)",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID, strings.Join(keys, ", "))
	return true, bh.Exec(ctx, sql)
}

func roleRuleDDLKey(ctx context.Context, db, table string, lower int64) (string, error) {
	key := db + "." + table
	parsedDB, parsedTable, ok := parsers.SplitRewriteKey(key)
	if !ok || parsedDB != db || parsedTable != table {
		return "", moerr.NewInvalidInput(ctx, "role rule requires an unambiguous database and table name")
	}
	return parsers.RoleRuleKey(db, table, lower), nil
}

// handleAlterRoleAddRule verifies role existence, then inserts or updates a mo_role_rule record.
func handleAlterRoleAddRule(ses *Session, execCtx *ExecCtx, stmt *tree.AlterRoleAddRule) (err error) {
	ctx := execCtx.reqCtx

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Begin transaction
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
		if err == nil {
			ses.ruleCacheMu.Lock()
			ses.ruleCache = nil
			ses.ruleCacheMu.Unlock()
		}
	}()

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

	lower := parserLowerCaseTableNames(ses)
	if err = validateRewriteRuleSQL(ctx, stmt.RuleSQL, lower, sessionSQLModeForParser(ses)); err != nil {
		return err
	}

	ruleName, err := roleRuleDDLKey(ctx, stmt.DbName, stmt.TblName, lower)
	if err != nil {
		return err
	}
	if _, err = deleteMatchingRoleRuleKeys(ctx, bh, roleID, ruleName, lower); err != nil {
		return err
	}

	insertSQL := fmt.Sprintf("insert into %s.%s (role_id, rule_name, `rule`) values (%d, %s, %s)",
		catalog.MO_CATALOG, catalog.MO_ROLE_RULE, roleID,
		escapeSQLString(ruleName), escapeSQLString(stmt.RuleSQL))
	if err = bh.Exec(ctx, insertSQL); err != nil {
		return err
	}

	return nil
}

// handleAlterRoleDropRule verifies role and rule existence, then deletes the mo_role_rule record.
func handleAlterRoleDropRule(ses *Session, execCtx *ExecCtx, stmt *tree.AlterRoleDropRule) (err error) {
	ctx := execCtx.reqCtx

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Begin transaction
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
		if err == nil {
			ses.ruleCacheMu.Lock()
			ses.ruleCache = nil
			ses.ruleCacheMu.Unlock()
		}
	}()

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

	lower := parserLowerCaseTableNames(ses)
	ruleName, err := roleRuleDDLKey(ctx, stmt.DbName, stmt.TblName, lower)
	if err != nil {
		return err
	}
	deleted, err := deleteMatchingRoleRuleKeys(ctx, bh, roleID, ruleName, lower)
	if err != nil {
		return err
	}
	if !deleted {
		return moerr.NewInternalErrorf(ctx, "rule '%s' does not exist for role '%s'", ruleName, stmt.RoleName)
	}

	return nil
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
