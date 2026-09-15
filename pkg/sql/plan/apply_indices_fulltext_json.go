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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Turning a json_extract comparison into a fulltext2 index probe.
//
// `json_extract_string(j,'$.foo') = 'bar'` becomes
//
//	json_extract_string(j,'$.foo') = 'bar'   AND   <probe on ('foo','bar')>
//
// The original predicate is ALWAYS retained and is what decides the answer. The
// probe only has to be IMPLIED by it: then it can never remove a row the
// original would have kept, and any extra row it lets through is removed by the
// original. A probe that is not implied is a wrong answer, not a slow query, so
// every gate below exists to keep the implication true rather than to widen
// coverage.

// jsonProbe is the index probe implied by one json_extract comparison.
//
// It is a UNION: the document qualifies if it holds any of Terms or any term
// inside any of Ranges. A union is all the contract needs — the probe only has
// to return a superset, since the retained predicate is re-evaluated on every
// row it returns. That is what lets a single comparison probe two different
// value encodings at once (§ jsonRangeProbe).
type jsonProbe struct {
	ColPos int32 // the json column being compared
	Tag    string

	Terms  []string
	Ranges []jsonTermRange
}

// jsonTermRange is an INCLUSIVE term range. Both ends are always inclusive,
// even for a strict > or <: including the boundary term only adds documents
// whose value equals the bound, and the retained predicate removes them. Since
// the probe only owes a superset, carrying exclusivity would buy one term of
// precision at the cost of threading a flag through the whole scan path.
type jsonTermRange struct {
	Lo, Hi string
}

// jsonExtractProbeFromExpr recognizes
//
//	json_extract_string|json_extract_float64(<col>, '<const path>')  <op>  <const>
//
// and returns the probe it implies. ok=false means "no probe" — always a safe
// answer, since the original predicate stands on its own.
func jsonExtractProbeFromExpr(expr *plan.Expr) (jsonProbe, bool) {
	c, ok := jsonExtractComparison(expr)
	if !ok {
		return jsonProbe{}, false
	}
	return c.probe()
}

// jsonComparison is a recognized `json_extract_*(col,path) <op> const`, split out
// so the caller can resolve WHICH index will serve it — and therefore the term
// shape — before the probe is built.
type jsonComparison struct {
	col      int32
	tag      string
	path     string // the raw json path ('$.foo'), for rebuilding the predicate on the tail
	isString bool
	op       string
	lit      *plan.Literal
}

func (c jsonComparison) probe() (jsonProbe, bool) {
	if c.op == "=" {
		return jsonEqualProbe(c.col, c.tag, c.lit, c.isString)
	}
	return jsonRangeProbe(c.col, c.tag, c.lit, c.op, c.isString)
}

func jsonExtractComparison(expr *plan.Expr) (jsonComparison, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return jsonComparison{}, false
	}
	op := fn.Func.ObjName

	// Accept either operand order. `3 < json_extract_float64(...)` is
	// `json_extract_float64(...) > 3`, so the operator flips with the operands.
	extract, konst := fn.Args[0], fn.Args[1]
	if extract.GetF() == nil {
		extract, konst = konst, extract
		var ok bool
		if op, ok = flipComparison(op); !ok {
			return jsonComparison{}, false
		}
	}

	col, tag, path, isString, ok := jsonExtractTarget(extract)
	if !ok {
		return jsonComparison{}, false
	}
	lit := konst.GetLit()
	if lit == nil {
		return jsonComparison{}, false
	}
	return jsonComparison{col: col, tag: tag, path: path, isString: isString, op: op, lit: lit}, true
}

// jsonEqualProbe builds the equality probe.
//
// One encoding per function, because the two extractors are DISJOINT on leaf
// type: json_extract_string returns NULL for every numeric leaf and
// json_extract_float64 returns NULL for every string one (verified against the
// server: json_extract_string('{"v":3.14}','$.v') IS NULL). So
// `json_extract_string(...) = '3.14'` can only be true for the STRING "3.14",
// and probing the float encoding as well would add a term that no qualifying
// document can hold.
func jsonEqualProbe(col int32, tag string, lit *plan.Literal, isString bool) (jsonProbe, bool) {
	if isString {
		s, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return jsonProbe{}, false
		}
		return jsonProbe{
			ColPos: col, Tag: tag,
			Terms: fulltext2.JSONEqualProbeTerms(tag, s.Sval),
		}, true
	}
	f, ok := litAsFloat(lit)
	if !ok {
		return jsonProbe{}, false
	}
	return jsonProbe{
		ColPos: col, Tag: tag,
		Terms: []string{fulltext2.JSONFloatTerm(tag, f)},
	}, true
}

// jsonRangeProbe builds the probe for >, >=, < and <= as a single term RANGE.
//
// The tuple encoding is order-preserving — types.Packer writes a type code then
// an order-preserving body, so terms under one tag sort by value, and the two
// leaf types occupy disjoint stretches (a numeric range can never sweep up a
// string term). So an inequality on a value maps to an inequality on terms, and
// the open end is the tag's own type bound rather than the whole dictionary.
//
// Both ends are INCLUSIVE, which makes a strict > or < a superset by exactly the
// boundary value. That is deliberate: a probe only has to be a NECESSARY
// condition, and the original predicate is retained and re-evaluated above the
// join, so the boundary row is filtered there. It costs one term and removes a
// whole class of off-by-one.
//
// Truncation is superset-safe for the same reason it is for equality. Values are
// cut to maxTermValueBytes before encoding, and truncation is monotone: if
// w < v then trunc(w) <= trunc(v), because either they first differ inside the
// kept prefix (the order survives) or they agree on it (the terms collapse and
// the inclusive bound keeps the row). So a truncated bound never excludes a
// qualifying document.
func jsonRangeProbe(col int32, tag string, lit *plan.Literal, op string, isString bool) (jsonProbe, bool) {
	var bound, loAll, hiAll string
	if isString {
		s, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return jsonProbe{}, false
		}
		bound = fulltext2.JSONStringTerm(tag, s.Sval)
		loAll, hiAll = fulltext2.JSONStringTermBounds(tag)
	} else {
		f, ok := litAsFloat(lit)
		if !ok {
			return jsonProbe{}, false
		}
		// NaN has no position in the encoded order, so no range can bracket it.
		if math.IsNaN(f) {
			return jsonProbe{}, false
		}
		bound = fulltext2.JSONFloatTerm(tag, f)
		loAll, hiAll = fulltext2.JSONNumericTermBounds(tag)
	}

	var r jsonTermRange
	switch op {
	case ">", ">=":
		r = jsonTermRange{Lo: bound, Hi: hiAll}
	case "<", "<=":
		r = jsonTermRange{Lo: loAll, Hi: bound}
	default:
		return jsonProbe{}, false
	}
	return jsonProbe{ColPos: col, Tag: tag, Ranges: []jsonTermRange{r}}, true
}

// jsonExtractTarget matches json_extract_string|json_extract_float64(col, 'path')
// and returns the column position, the path's trailing TAG, the raw path literal, and
// whether the extract is the string flavour.
func jsonExtractTarget(expr *plan.Expr) (col int32, tag, path string, isString, ok bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return 0, "", "", false, false
	}
	switch fn.Func.ObjName {
	case "json_extract_string":
		isString = true
	case "json_extract_float64":
	default:
		return 0, "", "", false, false
	}
	c := fn.Args[0].GetCol()
	if c == nil {
		return 0, "", "", false, false
	}
	lit := fn.Args[1].GetLit()
	if lit == nil {
		return 0, "", "", false, false
	}
	s, isSval := lit.Value.(*plan.Literal_Sval)
	if !isSval {
		return 0, "", "", false, false
	}
	tag, ok = jsonPathTag(s.Sval)
	if !ok {
		return 0, "", "", false, false
	}
	return c.ColPos, tag, s.Sval, isString, true
}

// jsonPathTag returns the trailing object KEY of a literal JSON path, using the
// CANONICAL parser that execution uses.
//
// It must not be a string scan. `$."a.b"` splits on the last '.' as `b"`, while
// the index stores the key `a.b`; the probe built from that wrong key matches
// nothing and, being ANDed in, drops a qualifying row. Escaped and bracketed
// keys fail the same way. bytejson.TerminalKey also rejects any path that is not
// deterministic (`**`, wildcards, `[*]`, ranges), so only a path addressing one
// key is ever optimized.
func jsonPathTag(path string) (string, bool) {
	p, err := bytejson.ParseJsonPath(strings.TrimSpace(path))
	if err != nil {
		return "", false
	}
	return p.TerminalKey()
}

func flipComparison(op string) (string, bool) {
	switch op {
	case "=":
		return "=", true
	case ">":
		return "<", true
	case ">=":
		return "<=", true
	case "<":
		return ">", true
	case "<=":
		return ">=", true
	}
	return "", false
}

// litAsFloat reads a numeric literal. A string literal is NOT coerced: it would
// change which leaves the probe reaches and so could break the implication.
func litAsFloat(lit *plan.Literal) (float64, bool) {
	switch v := lit.Value.(type) {
	case *plan.Literal_I64Val:
		return float64(v.I64Val), true
	case *plan.Literal_U64Val:
		return float64(v.U64Val), true
	case *plan.Literal_Fval:
		return float64(v.Fval), true
	case *plan.Literal_Dval:
		return v.Dval, true
	}
	return 0, false
}

// selfCompletingJSONProbeSupported reports whether the cluster is upgraded enough to run the
// self-completing fulltext2 json index probe (see addJSONFulltextProbes for the mixed-version
// hazard the gate closes). MOProtocolVersion is the service-local rollout gate, raised only once
// every CN understands the probe_tail TableConfig contract and lowered before rollback.
func (builder *QueryBuilder) selfCompletingJSONProbeSupported() bool {
	if builder == nil || builder.compCtx == nil {
		return false
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion74
}

// addJSONFulltextProbes appends an index-probe conjunct to scanNode's filter
// list for every json_extract comparison a json fulltext2 index can serve.
//
// The probe is emitted as an ordinary fulltext_match carrying a binary probe
// payload and the JSONProbeMode mode. That is deliberate: the whole
// scan-to-TVF rewrite (findMatchFullTextIndex, applyJoinFullTextIndices,
// buildFulltext2SearchCfg) then applies unchanged, and the distinct mode is
// what keeps the binary payload away from the pattern parser.
//
// Only TOP-LEVEL conjuncts are considered. A predicate under OR or NOT need not
// hold for a returned row, so a probe derived from it would not be implied.
func (builder *QueryBuilder) addJSONFulltextProbes(scanNode *plan.Node) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return
	}
	if len(scanNode.FilterList) == 0 || len(scanNode.TableDef.Indexes) == 0 {
		return
	}
	// One probe per scan: each is a separate MATCH and the rewrite chains them,
	// but a second probe on the same column buys little and complicates the
	// join shape. Take the first servable comparison.
	for _, f := range scanNode.FilterList {
		if isJSONProbeMatch(f) {
			return // already probed (idempotent across repeated planner passes)
		}
	}
	for _, f := range scanNode.FilterList {
		c, ok := jsonExtractComparison(f)
		if !ok {
			continue
		}
		idxDef := builder.findJSONTupleIndex(scanNode, c.col)
		if idxDef == nil {
			continue
		}
		kind, bar := builder.decideJSONProbe(scanNode, idxDef)
		if kind == jsonProbeSkip {
			continue
		}
		probe, ok := c.probe()
		if !ok {
			continue
		}
		// Mixed-version fence. The json probe emits a fulltext2_search TVF whose TableConfig carries
		// probe_tail/source/bar/predicate for self-completion. That TVF can be serialized into a remote
		// scope (broadcast-join build side) and executed on any selected worker; a CN that predates this
		// contract decodes the config into an older TableConfig, silently drops those fields, and runs
		// only a stale bulk probe -- which then loses rows the tail would have supplied at the mandatory
		// join. MOProtocolVersion is the service-local rollout gate (raised only once every CN
		// understands the contract), so decline the probe until MORPCVersion74 and let the query run as
		// a plain Table Scan on the retained json_extract predicate (correct, just unaccelerated).
		if !builder.selfCompletingJSONProbeSupported() {
			return
		}
		match := builder.makeJSONProbeMatch(scanNode, c.col, probe)
		if match == nil {
			continue
		}
		// An async index (partial) self-completes: record the tail's json predicate + the reconstructed
		// tail SQL. buildFulltext2SearchCfg carries the predicate so the operator's table_changes tail
		// filters the gap to actual matches (evaluated directly on the changed rows, no index), and the
		// join splice publishes the SQL on the node's Stats.Sql for EXPLAIN. The operator binds the
		// generation it actually searched at runtime and unions the tail internally, so no UNION arm is
		// planned. The json comparison also stays in FilterList so the base scan re-checks it on current
		// values (the tail is only a superset).
		if kind == jsonProbePartial {
			// If the tail/fallback predicate cannot be rendered, decline the probe entirely: the
			// operator's fallback would then emit every source pk and the mandatory join would degrade
			// to a full self-join. A plain Table Scan (no probe injected) is correct and cheaper.
			if !builder.recordJSONProbeTail(scanNode, c, bar) {
				continue
			}
		}
		scanNode.FilterList = append(scanNode.FilterList, match)
		return
	}
}

// jsonProbeTailInfo is what a self-completing json probe carries to the operator and to EXPLAIN.
type jsonProbeTailInfo struct {
	// whereSQL is the json predicate rebuilt against the source columns with the public
	// json_extract_string / json_extract_float64 (json_extract_string(`col`, '$.path') <op> <lit>).
	// The operator pushes it into BOTH its table_changes tail and its base-table fallback, which run
	// with applyIndices=1 (via StatementOption.WithOptimizerHints) so their base scan skips the
	// mandatory-filter rewrite and cannot re-trigger the probe and recurse. It filters both to
	// matching rows.
	whereSQL string
	// bar / barLogical are the max source commit as of the read (SourceCommitTS), physical and logical.
	// The operator compares the generation it ACTUALLY searched against this FULL timestamp to choose
	// no-tail (caught up) / tail (behind) / fallback. The logical half must be carried: build_ts is
	// physical-only, so truncating bar to physical would wrongly declare a (P, L>0) bar covered by a
	// generation at physical P and drop that commit's row at the mandatory join.
	bar        int64
	barLogical uint32
	// displaySQL is the tail query shown in EXPLAIN (Verbose) via Stats.Sql so the internally-run tail
	// is visible. Its lower bound is symbolic (<searched generation>) because the operator binds it at
	// runtime; a caught-up run skips it and an incompatible run replaces it with a full pk scan.
	displaySQL string
}

// recordJSONProbeTail marks scanNode's json probe as self-completing and records the tail predicate,
// the max-source-commit bar, and the display SQL, keyed by node id. Presence tells
// buildFulltext2SearchCfg to set TableConfig.ProbeTail. It returns false when the json predicate
// cannot be rendered to SQL: the operator's incompatible-generation FALLBACK is `SELECT pk FROM src
// WHERE <predicate>`, so without a predicate that fallback would emit EVERY source pk and the mandatory
// join would degrade to a full self-join. Rather than risk that, the caller declines the probe and the
// query runs as a plain Table Scan instead (always correct, and a real full scan is cheaper than a
// self-join). In practice this never fires -- a comparison whose term probe rendered also renders here.
func (builder *QueryBuilder) recordJSONProbeTail(scanNode *plan.Node, c jsonComparison, bar types.TS) bool {
	colName := jsonProbeColName(scanNode, c.col)
	if colName == "" {
		return false
	}
	where, ok := jsonComparisonSQL(c, colName) // internal-named: pushed into both tail and fallback
	if !ok || where == "" {
		return false
	}
	if builder.jsonProbeTail == nil {
		builder.jsonProbeTail = make(map[int32]jsonProbeTailInfo)
	}
	builder.jsonProbeTail[scanNode.NodeId] = jsonProbeTailInfo{
		whereSQL:   where,
		bar:        bar.Physical(),
		barLogical: bar.Logical(),
		displaySQL: builder.jsonProbeTailSQL(scanNode, where),
	}
	return true
}

// jsonProbeReadTS is the read point a json probe measures against: the snapshot TS for a
// {snapshot=...}/AS OF read, else the current txn snapshot. Second return is false when there is no
// process/txn to ask (unit contexts).
func (builder *QueryBuilder) jsonProbeReadTS(scanNode *plan.Node) (types.TS, bool) {
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return types.TS{}, false
	}
	txn := proc.GetTxnOperator()
	if txn == nil {
		return types.TS{}, false
	}
	snap := txn.SnapshotTS()
	if ets := sqlexec.NewSqlProcess(proc).ApplyScanSnapshot(scanNode.ScanSnapshot); ets != nil {
		snap = *ets
	}
	return types.TimestampToTS(snap), true
}

func jsonProbeColName(scanNode *plan.Node, col int32) string {
	if scanNode.TableDef == nil || col < 0 || int(col) >= len(scanNode.TableDef.Cols) {
		return ""
	}
	return scanNode.TableDef.Cols[col].Name
}

// jsonComparisonSQL rebuilds the json_extract comparison as SQL text over the source column by NAME,
// so the operator can filter its tail and its fallback directly -- no index. It is the SAME predicate
// the base scan re-checks, so pushing it only shrinks the candidate set. Returns false when the
// literal has no SQL rendering.
//
// It renders the public json_extract_string / json_extract_float64. The fallback scans the BASE
// table, where this predicate would normally re-trigger the mandatory-filter rewrite and recurse;
// that is prevented instead by running the fallback/tail SQL with applyIndices=1 (the fulltext2
// probe sets it via StatementOption.WithOptimizerHints), so its plan skips the index rewrite. No
// byte-identical function twin is needed, so the plan stays free of a version-specific overload.
func jsonComparisonSQL(c jsonComparison, colName string) (string, bool) {
	fn := "json_extract_float64"
	if c.isString {
		fn = "json_extract_string"
	}
	val, ok := jsonLiteralToSQL(c.lit)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%s(%s, %s) %s %s", fn, sqlquote.Ident(colName), sqlquote.String(c.path), c.op, val), true
}

// jsonLiteralToSQL renders the comparison's constant as a SQL literal. Only the literal kinds
// jsonExtractComparison accepts (string for json_extract_string; the numeric kinds litAsFloat reads)
// are handled; anything else returns false.
func jsonLiteralToSQL(lit *plan.Literal) (string, bool) {
	switch v := lit.Value.(type) {
	case *plan.Literal_Sval:
		return sqlquote.String(v.Sval), true
	case *plan.Literal_I64Val:
		return strconv.FormatInt(v.I64Val, 10), true
	case *plan.Literal_U64Val:
		return strconv.FormatUint(v.U64Val, 10), true
	case *plan.Literal_Fval:
		return strconv.FormatFloat(float64(v.Fval), 'g', -1, 64), true
	case *plan.Literal_Dval:
		return strconv.FormatFloat(v.Dval, 'g', -1, 64), true
	}
	return "", false
}

// jsonProbeTailSQL reconstructs, for display, what the fulltext2_search operator runs to self-complete
// an async json probe. The operator picks ONE of three at execution, against the generation it ACTUALLY
// searched, so no single SQL is literally "the" query -- this renders the representative BEHIND tail
// (table_changes inserts after the searched generation up to the read, filtered by whereSQL, projected
// to the pk) and annotates the other two branches so EXPLAIN is not a black box: caught up => no tail;
// newer-than-read or a DDL in the gap => the base-table fallback shown in the trailing comment. The
// lower bound is symbolic (<searched generation>) because it is bound at runtime; the executable forms
// are built by the operator.
func (builder *QueryBuilder) jsonProbeTailSQL(scanNode *plan.Node, whereSQL string) string {
	db, tbl, pk := "", "", ""
	if scanNode.ObjRef != nil {
		db, tbl = scanNode.ObjRef.SchemaName, scanNode.ObjRef.ObjName
	}
	if scanNode.TableDef != nil && scanNode.TableDef.Pkey != nil {
		pk = scanNode.TableDef.Pkey.PkeyColName
	}
	toStr := "<snapshot>"
	if readTS, ok := builder.jsonProbeReadTS(scanNode); ok {
		toStr = fmt.Sprintf("%d-%d", readTS.Physical(), readTS.Logical())
	}
	const tc = "mo_tc" // alias so table_changes' reserved metadata columns bind (matches the operator)
	tail := fmt.Sprintf("SELECT %s.`%s` FROM table_changes('%s', '%s', '<searched generation>', '%s') AS %s WHERE %s.%s = 'insert'",
		tc, pk, db, tbl, toStr, tc, tc, catalog.TableChangesAttrChangeType)
	fallback := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s`", pk, db, tbl)
	if whereSQL != "" {
		tail += " AND (" + whereSQL + ")"
		fallback += " WHERE " + whereSQL
	}
	return tail + " /* self-completes vs searched generation: caught up => no tail; behind => this tail; newer-than-read or DDL-in-gap => " + fallback + " */"
}

// PreparedPlanDependsOnIndexCoverage reports whether a prepared plan carries an injected
// json_extract fulltext2 probe, which must be rebuilt on every EXECUTE. The probe SELF-COMPLETES at
// execution -- it binds its table_changes tail to the generation it actually searched and to the
// current snapshot -- so index freshness itself no longer forces a rebuild (a reused plan tails the
// current generation correctly). What still does is the plan-build-time probe-vs-full-scan decision,
// made from transaction state NO schema version represents: chiefly the transaction-local-write
// guard. The probe and its tail see only COMMITTED rows, so a plan built in a clean txn and reused
// in a txn that has since written uncommitted rows to the source would DROP them (the base scan sees
// them, the probe does not, and the INNER JOIN discards them). Rebuilding re-runs that guard (and
// re-checks that the index is built). A user MATCH also builds a fulltext2_search node but is
// search-semantics (freshness-tolerant); the JSONProbeMode argument distinguishes the injected probe
// and is required.
func PreparedPlanDependsOnIndexCoverage(p *Plan) bool {
	if p == nil {
		return false
	}
	query := p.GetQuery()
	if query == nil {
		return false
	}
	for _, node := range query.GetNodes() {
		if node == nil || node.TableDef == nil || node.TableDef.TblFunc == nil ||
			node.TableDef.TblFunc.Name != fulltext2_search_func_name {
			continue
		}
		args := node.GetTblFuncExprList()
		if len(args) < 3 {
			continue
		}
		if lit := args[2].GetLit(); lit != nil {
			if v, ok := lit.Value.(*plan.Literal_I64Val); ok && v.I64Val == fulltext2.JSONProbeMode {
				return true
			}
		}
	}
	return false
}

// jsonProbeKind is how addJSONFulltextProbes may use a json_extract fulltext2 index for one
// comparison.
type jsonProbeKind int

const (
	// jsonProbeSkip: the index cannot be trusted here -- leave the full scan (fail closed).
	jsonProbeSkip jsonProbeKind = iota
	// jsonProbeCovered: a synchronous index, OR an async index that is CAUGHT UP for the read --
	// emit a mandatory probe with NO tail (the index already reflects every row the read sees).
	jsonProbeCovered
	// jsonProbePartial: an async index that is BEHIND -- emit a mandatory probe the fulltext2_search
	// operator SELF-COMPLETES, binding the generation it searched at runtime and unioning a
	// table_changes tail up to the read snapshot. Covers both current and {snapshot=...} reads.
	jsonProbePartial
)

// decideJSONProbe evaluates idx against scanNode's read and reports how a probe may use it, plus
// (for jsonProbePartial) the plan-time build_ts -- the lower bound shown in the EXPLAIN tail SQL. A
// synchronous index, and an async index that is CAUGHT UP (covered), probe with no tail. An async
// index that is BEHIND self-completes (jsonProbePartial): the operator binds the generation it
// actually searched and unions a table_changes tail up to the read point. Fails closed to
// jsonProbeSkip on any uncertainty (unbuilt index, table_changes cannot serve the table,
// transaction-local writes): a full scan is always correct, an unsound probe is not.
func (builder *QueryBuilder) decideJSONProbe(scanNode *plan.Node, idx *plan.IndexDef) (jsonProbeKind, types.TS) {
	algo := catalog.ToLower(idx.IndexAlgo)
	if !indexplugin.AlwaysAsync(algo, idx.IndexAlgoParams) {
		return jsonProbeCovered, types.TS{}
	}
	if builder == nil || builder.compCtx == nil || scanNode == nil ||
		scanNode.TableDef == nil || scanNode.TableDef.TblId == 0 {
		return jsonProbeSkip, types.TS{}
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return jsonProbeSkip, types.TS{}
	}
	txn := proc.GetTxnOperator()
	if txn == nil {
		return jsonProbeSkip, types.TS{}
	}
	// The effective read TS for a {snapshot=...}/AS OF query (nil for a current read), computed
	// exactly as the search does, so the source-commit bar and the operator's tail target the same
	// read point. ApplyScanSnapshot also binds the snapshot's owning tenant on sp.
	sp := sqlexec.NewSqlProcess(proc)
	scanSnapshotTS := sp.ApplyScanSnapshot(scanNode.ScanSnapshot)

	// proc.Ctx is canceled during planning; use the top context. For a cross-account snapshot the
	// freshness reads (source relation, ISCP log, index metadata) must resolve under the account that
	// OWNS the data, not the reader's -- else they find the wrong account's tables or nothing. sp
	// resolves the effective account (snapshot's owner for a historical read, else the caller's own),
	// so binding it is a no-op for an ordinary current read.
	ctx := proc.GetTopContext()
	if acctID, aerr := sp.EffectiveAccountID(); aerr == nil {
		ctx = defines.AttachAccountId(ctx, acctID)
	}

	// The coverage bar is the max source commit the read must see, read from the source relation's
	// partition state AS OF THE READ: the current txn for a current read, or a txn cloned at the
	// snapshot for a historical one. build_ts and the bar are thus measured at the same read point, so
	// a snapshot whose index had caught up as of S is covered, and one that was behind is completed
	// with a table_changes tail up to S -- exactly like a current read.
	readTxn := txn
	if scanSnapshotTS != nil {
		readTxn = txn.CloneSnapshotOp(*scanSnapshotTS)
	}
	eng := proc.GetSessionInfo().StorageEngine
	if eng == nil {
		return jsonProbeSkip, types.TS{}
	}
	_, _, rel, err := eng.GetRelationById(ctx, readTxn, scanNode.TableDef.TblId)
	if err != nil {
		logutil.Debugf("json index probe: resolve source relation failed for %s: %v", idx.IndexName, err)
		return jsonProbeSkip, types.TS{}
	}
	commitTSProvider, ok := rel.(engine.SourceCommitTSProvider)
	if !ok {
		return jsonProbeSkip, types.TS{}
	}
	// The true max source commit as of the read (empty mustExceed => no early-out). This single value
	// is BOTH the `bar` the operator uses to decide caught-up-vs-behind against the generation it
	// ACTUALLY searched, AND the transaction-local-write guard: SourceCommitTS fails closed on an
	// uncommitted write to the source, which would make ANY probe unsound (the index and the tail see
	// only committed rows). On error, decline to a full scan.
	bar, err := commitTSProvider.SourceCommitTS(ctx, types.TS{})
	if err != nil {
		logutil.Debugf("json index probe: source-commit read failed for %s: %v", idx.IndexName, err)
		return jsonProbeSkip, types.TS{}
	}
	// The operator completes the gap with a table_changes tail. If that TVF cannot serve this table
	// (partitioned, temporary, no explicit pk, or a column colliding with its reserved metadata
	// names), self-completion is impossible -- decline to a full scan.
	if validateTableChangesSource(scanNode.ObjRef, scanNode.TableDef) != nil {
		return jsonProbeSkip, types.TS{}
	}
	// table_changes emits only non-hidden source columns, so a composite (or otherwise hidden)
	// primary key -- whose pk column it drops -- cannot anchor the tail's pk projection. Decline.
	pkPos, ok := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	if !ok || int(pkPos) >= len(scanNode.TableDef.Cols) || scanNode.TableDef.Cols[pkPos].Hidden {
		return jsonProbeSkip, types.TS{}
	}
	// Always self-complete. The generation-dependent choices -- caught up (no tail), behind (tail),
	// newer-than-read or a DDL in the gap (fallback to a full pk scan) -- are ALL made by the operator
	// against the generation it ACTUALLY searched (§10.4), never a plan-time guess. The plan carries
	// only `bar` (this max source commit), which is stable for the statement.
	return jsonProbePartial, bar
}

// findJSONTupleIndex returns the fulltext2 index over exactly colPos whose
// parser is json AND whose persisted include_keys is on — the only index that
// actually holds tuple terms. Probing anything else finds nothing and would
// drop every row.
func (builder *QueryBuilder) findJSONTupleIndex(scanNode *plan.Node, colPos int32) *plan.IndexDef {
	if colPos < 0 || int(colPos) >= len(scanNode.TableDef.Cols) {
		return nil
	}
	colName := scanNode.TableDef.Cols[colPos].Name
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || !idx.TableExist {
			continue
		}
		// Route through the plugin registry rather than branching on the algo
		// name in the SQL layer: an unregistered algo is simply not probeable.
		if _, registered := indexplugin.Get(catalog.ToLower(idx.IndexAlgo)); !registered {
			continue
		}
		// resolve against the STORAGE def, as findMatchFullTextIndex does
		if idx.IndexAlgoTableType != catalog.FullText2Index_TblType_Storage {
			continue
		}

		if len(idx.Parts) != 1 {
			continue
		}
		if !strings.EqualFold(catalog.ResolveAlias(idx.Parts[0]), colName) &&
			!strings.EqualFold(idx.Parts[0], colName) {
			continue
		}
		if fulltext2ParserFromParams(idx.IndexAlgoParams) != fulltext2.ParserJSON {
			continue
		}
		if !jsonIndexIncludeKeys(idx) {
			continue
		}
		return idx
	}
	return nil
}

// jsonIndexIncludeKeys reads the persisted term shape. It must come from the
// index, never be assumed: an index built without tuple terms and probed for
// them silently returns nothing.
func jsonIndexIncludeKeys(idx *plan.IndexDef) bool {
	return jsonIndexParam(idx, catalog.IndexAlgoParamJSONIncludeKeys) != "false"
}

func jsonIndexParam(idx *plan.IndexDef, key string) string {
	if idx == nil || idx.IndexAlgoParams == "" {
		return ""
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(idx.IndexAlgoParams), &m); err != nil {
		return ""
	}
	return m[key]
}

// makeJSONProbeMatch builds `fulltext_match(<payload>, JSONProbeMode, <col>)`.
// The column arg must carry the scan's binding tag, which is how
// findMatchFullTextIndex resolves it back to the index.
func (builder *QueryBuilder) makeJSONProbeMatch(scanNode *plan.Node, colPos int32, p jsonProbe) *plan.Expr {
	ranges := make([][2]string, 0, len(p.Ranges))
	for _, r := range p.Ranges {
		ranges = append(ranges, [2]string{r.Lo, r.Hi})
	}
	payload := fulltext2.EncodeJSONProbePayload(p.Terms, ranges)

	col := scanNode.TableDef.Cols[colPos]
	args := []*plan.Expr{
		// binary: the payload holds 0x00 and pattern metacharacters
		makePlan2StringConstExprWithType(payload, true),
		makePlan2Int64ConstExprWithType(fulltext2.JSONProbeMode),
		{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: scanNode.BindingTags[0],
				ColPos: colPos,
				Name:   col.Name,
			}},
		},
	}
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "fulltext_match", args)
	if err != nil {
		return nil
	}
	return expr
}

// isJSONProbeMatch reports whether expr is a probe this rule already added.
func isJSONProbeMatch(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "fulltext_match" || len(fn.Args) < 2 {
		return false
	}
	lit := fn.Args[1].GetLit()
	if lit == nil {
		return false
	}
	v, ok := lit.Value.(*plan.Literal_I64Val)
	return ok && v.I64Val == fulltext2.JSONProbeMode
}

// dedupFulltextDocIDs puts a GROUP BY on the doc id over a json probe's index
// scan, and returns the new node plus the pk reference to read it through.
//
// The probe's scan walks one term at a time instead of merging them, so it
// yields a document once per matching term. Those repeats cannot reach the INNER
// JOIN below — a repeated pk there multiplies base-table rows — and the fix
// belongs to the aggregate operator rather than to a second de-duplication
// written inside the index: the aggregate already spills, and already handles
// volumes a bespoke in-index structure would have to bound by hand.
//
// Only the doc id is carried up. A probe's score is a constant nobody selects or
// orders by, so the group needs no aggregate at all.
func (builder *QueryBuilder) dedupFulltextDocIDs(ctx *BindContext, ftNodeID int32, pkCol *plan.Expr) (int32, *plan.Expr) {
	groupTag := builder.genNewBindTag()
	aggTag := builder.genNewBindTag()
	nodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{ftNodeID},
		GroupBy:     []*plan.Expr{DeepCopyExpr(pkCol)},
		BindingTags: []int32{groupTag, aggTag},
		SpillMem:    builder.aggSpillMem,
	}, ctx)

	if builder.jsonProbeFtNodes == nil {
		builder.jsonProbeFtNodes = make(map[int32]bool)
	}
	// Record the immediate child, the fulltext2_search SCAN (the id the score-sort/runtime-filter
	// passes hold in ret_filter_node_ids). A self-completing async probe emits its table_changes tail
	// INSIDE that same scan node (no separate UNION arm), so the child is always the scan.
	builder.jsonProbeFtNodes[ftNodeID] = true

	return nodeID, &plan.Expr{
		Typ:  pkCol.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: groupTag, ColPos: 0}},
	}
}
