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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
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
		kind, buildTS := builder.decideJSONProbe(scanNode, idxDef)
		if kind == jsonProbeSkip {
			continue
		}
		probe, ok := c.probe()
		if !ok {
			continue
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
			builder.recordJSONProbeTail(scanNode, c, buildTS)
		}
		scanNode.FilterList = append(scanNode.FilterList, match)
		return
	}
}

// jsonProbeTailInfo is what a self-completing json probe carries to the operator and to EXPLAIN.
type jsonProbeTailInfo struct {
	// whereSQL is the json predicate rebuilt against the tail's columns
	// (json_extract_string(`col`, '$.path') <op> <lit>), appended to the executable table_changes
	// query so the tail returns only matching gap rows. Empty ⇒ push nothing (tail is all gap inserts;
	// the base scan still re-checks, so correctness holds regardless).
	whereSQL string
	// displaySQL is the full reconstructed tail query shown in EXPLAIN (Verbose) via Stats.Sql, so the
	// internally-run tail is visible. Its from bound is the PLAN-TIME build_ts; the operator runs from
	// the generation it actually searched at runtime, differing only within the sub-second reuse window.
	displaySQL string
}

// recordJSONProbeTail marks scanNode's json probe as self-completing and records the tail's predicate
// SQL and its display SQL, keyed by node id. Presence tells buildFulltext2SearchCfg to set
// TableConfig.ProbeTail.
func (builder *QueryBuilder) recordJSONProbeTail(scanNode *plan.Node, c jsonComparison, buildTS types.TS) {
	if builder.jsonProbeTail == nil {
		builder.jsonProbeTail = make(map[int32]jsonProbeTailInfo)
	}
	where := ""
	if colName := jsonProbeColName(scanNode, c.col); colName != "" {
		where, _ = jsonComparisonSQL(c, colName) // best-effort; "" leaves the tail unfiltered (still correct)
	}
	// The EXPLAIN display SQL is shown ONLY when the index is behind the read as of planning
	// (buildTS < readTS) -- i.e. when the tail is expected to actually run. A caught-up index
	// self-completes with an EMPTY tail the operator skips at runtime, so surfacing a table_changes
	// query that will not execute would mislead. ProbeTail + whereSQL are recorded regardless, so a
	// runtime that turns out behind still runs (and filters) the tail; only the display is gated.
	display := ""
	if readTS, ok := builder.jsonProbeReadTS(scanNode); ok && buildTS.LT(&readTS) {
		display = builder.jsonProbeTailSQL(scanNode, buildTS, where)
	}
	builder.jsonProbeTail[scanNode.NodeId] = jsonProbeTailInfo{whereSQL: where, displaySQL: display}
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
// so it can filter table_changes (which exposes the source columns by name) directly -- no index. It
// is the SAME predicate the base scan re-checks, so pushing it only shrinks the tail. Returns false
// when the literal has no SQL rendering (the caller then leaves the tail unfiltered).
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

// jsonProbeTailSQL reconstructs, for display, the table_changes gap query the fulltext2_search
// operator runs to self-complete an async json probe: the inserts committed after the searched
// generation up to the read snapshot, filtered by whereSQL, projected to the pk the join binds. The
// executable form (with the runtime bound) is built by the operator; this is what EXPLAIN shows so
// the internally-run tail is not a black box.
func (builder *QueryBuilder) jsonProbeTailSQL(scanNode *plan.Node, buildTS types.TS, whereSQL string) string {
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
	fromStr := fmt.Sprintf("%d-%d", buildTS.Physical(), buildTS.Logical())
	const tc = "mo_tc" // alias so table_changes' reserved metadata columns bind (matches the operator)
	sql := fmt.Sprintf("SELECT %s.`%s` FROM table_changes('%s', '%s', '%s', '%s') AS %s WHERE %s.%s = 'insert'",
		tc, pk, db, tbl, fromStr, toStr, tc, tc, catalog.TableChangesAttrChangeType)
	if whereSQL != "" {
		sql += " AND (" + whereSQL + ")"
	}
	return sql
}

// PreparedPlanDependsOnIndexCoverage reports whether a prepared plan carries an injected
// json_extract fulltext2 coverage probe. That probe's covered/partial/skip decision is made from
// the async index's freshness at plan-build time -- state no table schema version represents -- so
// reusing the plan after the index falls behind would drop rows committed in the gap: a covered
// probe filters them out, and a partial plan's table_changes window is frozen. Such a plan must be
// rebuilt on every EXECUTE. A user MATCH also builds a fulltext2_search node but is search-semantics
// (freshness-tolerant); the JSONProbeMode argument distinguishes the injected probe and is required.
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
	// jsonProbeCovered: a SYNCHRONOUS index (never behind) -- emit a mandatory probe with no tail.
	jsonProbeCovered
	// jsonProbePartial: an ASYNC index -- emit a mandatory probe the fulltext2_search operator
	// SELF-COMPLETES, binding the generation it searched at runtime and unioning a table_changes tail
	// up to the read snapshot. Covers both current and {snapshot=...} reads.
	jsonProbePartial
)

// The coverage lookup is the only runtime-dependent input to the probe decision, so it is
// indirected here to let unit tests drive the covered/partial/skip matrix without a live index.
var coversSnapshotFn = indexplugin.CoversSnapshot

// decideJSONProbe evaluates idx against scanNode's read and reports how a probe may use it, plus
// (for jsonProbePartial) the plan-time build_ts -- the lower bound shown in the EXPLAIN tail SQL. A
// synchronous index is never behind, so it probes with no tail. An async index always self-completes
// (jsonProbePartial): the operator binds the generation it actually searched and unions a
// table_changes tail up to the read point, so covered-vs-partial need not be decided here. Fails
// closed to jsonProbeSkip on any uncertainty (unbuilt index, table_changes cannot serve the table,
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
	// Resolve the index's hidden tables so the freshness check can read the loaded
	// generation's build_ts (cache, keyed by the storage table) or the durable
	// MAX(build_ts) from the metadata table on a cold cache.
	storeTbl, metaTbl, _ := builder.findFulltext2IndexTables(scanNode, idx)
	dbName := ""
	if scanNode.ObjRef != nil {
		dbName = scanNode.ObjRef.SchemaName
	}
	// The effective read TS for a {snapshot=...}/AS OF query (nil for a current read), computed
	// exactly as the search does, so the freshness check, the coverage bar, and the tail all target
	// the same read point. ApplyScanSnapshot also binds the snapshot's owning tenant on sp.
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
	req := coverage.Request{
		CNUUID:   proc.GetService(),
		Txn:      txn,
		TableID:  scanNode.TableDef.TblId,
		IndexDef: idx,
		Snapshot: types.TimestampToTS(txn.SnapshotTS()),
		// The bar is computed lazily, only once build_ts is known to be a non-empty value it could
		// gate; mustExceed lets the provider stop once the source is known to be behind build_ts. The
		// provider also fails closed on transaction-local writes, so this doubles as the guard that
		// keeps the partial plan from dropping uncommitted rows.
		SourceCommitTS: func(c context.Context, mustExceed types.TS) (types.TS, error) {
			return commitTSProvider.SourceCommitTS(c, mustExceed)
		},
		IndexStorageTable:  storeTbl,
		IndexMetadataDB:    dbName,
		IndexMetadataTable: metaTbl,
		ScanSnapshotTS:     scanSnapshotTS,
	}
	// The `covered` verdict is intentionally ignored. With runtime tail binding the operator ALWAYS
	// self-completes: whether the index was caught up (an empty tail the operator skips at runtime) or
	// behind (the tail fills the gap), the answer is identical, so the plan need not decide
	// covered-vs-partial. coversSnapshotFn is still the way to (a) read buildTS -- the plan-time lower
	// bound shown in the EXPLAIN tail SQL -- and (b) fire the transaction-local-write guard inside
	// SourceCommitTS: a txn with uncommitted writes to the source makes the probe unsound (the index
	// and the tail see only committed rows), and that guard surfaces as an error here, forcing a full
	// scan.
	_, buildTS, err := coversSnapshotFn(ctx, algo, req)
	if err != nil {
		logutil.Debugf("json index probe: coverage check failed for %s: %v", idx.IndexName, err)
		return jsonProbeSkip, types.TS{}
	}
	// An unbuilt / unreadable index (build_ts 0) would turn the tail table_changes(0, S] into a
	// disguised full scan over the table's whole history -- decline to a real full scan instead.
	if buildTS.IsEmpty() {
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
	return jsonProbePartial, buildTS
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
