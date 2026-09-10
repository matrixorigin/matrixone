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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

	col, tag, isString, ok := jsonExtractTarget(extract)
	if !ok {
		return jsonComparison{}, false
	}
	lit := konst.GetLit()
	if lit == nil {
		return jsonComparison{}, false
	}
	return jsonComparison{col: col, tag: tag, isString: isString, op: op, lit: lit}, true
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
// and returns the column position, the path's trailing TAG, and whether the
// extract is the string flavour.
func jsonExtractTarget(expr *plan.Expr) (col int32, tag string, isString, ok bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return 0, "", false, false
	}
	switch fn.Func.ObjName {
	case "json_extract_string":
		isString = true
	case "json_extract_float64":
	default:
		return 0, "", false, false
	}
	c := fn.Args[0].GetCol()
	if c == nil {
		return 0, "", false, false
	}
	lit := fn.Args[1].GetLit()
	if lit == nil {
		return 0, "", false, false
	}
	s, isSval := lit.Value.(*plan.Literal_Sval)
	if !isSval {
		return 0, "", false, false
	}
	tag, ok = jsonPathTag(s.Sval)
	if !ok {
		return 0, "", false, false
	}
	return c.ColPos, tag, isString, true
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
		// A behind index (partial) still injects the bulk probe here; the freshness gap is
		// filled with a table_changes tail when applyJoinFullTextIndices builds the join. The
		// json comparison f stays in FilterList so the base scan re-checks it on current values.
		if kind == jsonProbePartial {
			builder.recordJSONPartialProbe(scanNode, f, buildTS)
		}
		scanNode.FilterList = append(scanNode.FilterList, match)
		return
	}
}

// jsonPartialProbe is the tail-construction input recorded for a behind-index json probe.
type jsonPartialProbe struct {
	buildTS  types.TS   // the searched generation's build_ts: exclusive lower bound of the tail
	jsonPred *plan.Expr // the json_extract comparison, pushed onto table_changes to shrink the tail
}

// recordJSONPartialProbe marks scanNode's json probe as partial so the join builder fills the gap.
func (builder *QueryBuilder) recordJSONPartialProbe(scanNode *plan.Node, jsonPred *plan.Expr, buildTS types.TS) {
	if builder.jsonPartialProbes == nil {
		builder.jsonPartialProbes = make(map[int32]jsonPartialProbe)
	}
	builder.jsonPartialProbes[scanNode.NodeId] = jsonPartialProbe{buildTS: buildTS, jsonPred: DeepCopyExpr(jsonPred)}
}

// jsonProbeKind is how addJSONFulltextProbes may use a json_extract fulltext2 index for one
// comparison.
type jsonProbeKind int

const (
	// jsonProbeSkip: the index cannot be trusted here -- leave the full scan (fail closed).
	jsonProbeSkip jsonProbeKind = iota
	// jsonProbeCovered: the index is current for the read -- emit a mandatory probe.
	jsonProbeCovered
	// jsonProbePartial: a current read whose index is behind -- complete the bulk probe with a
	// table_changes tail from the returned build_ts. json_extract only; snapshots never partial.
	jsonProbePartial
)

// decideJSONProbe evaluates idx against scanNode's read and reports how a probe may use it, plus
// (for jsonProbePartial) the build_ts the searched generation reached -- the lower bound of the
// table_changes tail that fills the freshness gap. A synchronous index always covers. An async
// index is asked; if it does not cover, a current read (scanSnapshot nil) with a known build_ts
// is completed partially, while a historical read declines (snapshots are binary). Fails closed to
// jsonProbeSkip on any uncertainty: a full scan is always correct, an unsound probe is not.
//
// No cost gate guards the partial path: the tail is emitted whenever the index is behind.
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
	// The effective historical read TS for a {snapshot=...}/AS OF query (nil for a
	// current read), computed exactly as the search does, so the freshness check
	// targets the same snapshot-bound index generation the search will load.
	scanSnapshotTS := sqlexec.NewSqlProcess(proc).ApplyScanSnapshot(scanNode.ScanSnapshot)

	// A historical read sees a fixed past state; its coverage bar is the snapshot TS
	// itself (build_ts >= snapshot ⇒ the index processed everything up to that point),
	// so SourceCommitTS -- the current-read "max outstanding source commit" that lets an
	// idle table's watermark catch up -- is neither needed nor meaningful. Only a current
	// read computes it, from the source relation's partition state.
	var sourceCommitTS types.TS
	if scanSnapshotTS == nil {
		eng := proc.GetSessionInfo().StorageEngine
		if eng == nil {
			return jsonProbeSkip, types.TS{}
		}
		_, _, rel, err := eng.GetRelationById(proc.GetTopContext(), txn, scanNode.TableDef.TblId)
		if err != nil {
			logutil.Debugf("json index probe: resolve source relation failed for %s: %v", idx.IndexName, err)
			return jsonProbeSkip, types.TS{}
		}
		commitTSProvider, ok := rel.(engine.SourceCommitTSProvider)
		if !ok {
			return jsonProbeSkip, types.TS{}
		}
		sourceCommitTS, err = commitTSProvider.SourceCommitTS(proc.GetTopContext())
		if err != nil {
			logutil.Debugf("json index probe: source commit timestamp unavailable for %s: %v", idx.IndexName, err)
			return jsonProbeSkip, types.TS{}
		}
	}
	// proc.Ctx is canceled during planning; use the top context.
	ctx := proc.GetTopContext()
	req := coverage.Request{
		CNUUID:             proc.GetService(),
		Txn:                txn,
		TableID:            scanNode.TableDef.TblId,
		IndexDef:           idx,
		Snapshot:           types.TimestampToTS(txn.SnapshotTS()),
		SourceCommitTS:     sourceCommitTS,
		IndexStorageTable:  storeTbl,
		IndexMetadataDB:    dbName,
		IndexMetadataTable: metaTbl,
		ScanSnapshotTS:     scanSnapshotTS,
	}
	covered, err := indexplugin.CoversSnapshot(ctx, algo, req)
	if err != nil {
		logutil.Debugf("json index probe: coverage check failed for %s: %v", idx.IndexName, err)
		return jsonProbeSkip, types.TS{}
	}
	if covered {
		return jsonProbeCovered, types.TS{}
	}
	// Not covered. A historical read cannot be completed with a tail -- snapshots are binary and
	// always have a snapshot-bound generation -- so decline. A current read that is merely behind
	// is completed with a table_changes tail from the generation's build_ts.
	if scanSnapshotTS != nil {
		return jsonProbeSkip, types.TS{}
	}
	buildTS := indexplugin.IndexBuildTS(ctx, algo, req)
	if buildTS.IsEmpty() {
		return jsonProbeSkip, types.TS{}
	}
	// The tail is table_changes over the gap. If that TVF cannot serve this table (partitioned,
	// temporary, no explicit pk, or a column colliding with its reserved metadata names), a partial
	// plan is impossible -- decline to a full scan rather than emit an incomplete bulk-only probe.
	if validateTableChangesSource(scanNode.ObjRef, scanNode.TableDef) != nil {
		return jsonProbeSkip, types.TS{}
	}
	return jsonProbePartial, buildTS
}

// indexCoversSnapshot reports whether idx is current enough to back a mandatory probe.
func (builder *QueryBuilder) indexCoversSnapshot(scanNode *plan.Node, idx *plan.IndexDef) bool {
	kind, _ := builder.decideJSONProbe(scanNode, idx)
	return kind == jsonProbeCovered
}

// buildJSONProbeTail builds the freshness-gap arm of a behind-index json probe: table_changes over
// (buildTS, snapshot] restricted to inserts matching the json predicate, projected to (pk, score) so
// it unions cleanly with the fulltext2_search bulk arm. It returns the project node id, or false if
// the tail cannot be built. The base scan re-checks every WHERE conjunct on current values, so this
// arm need only be a SUPERSET of the gap matches; pushing json_pred here just shrinks it.
func (builder *QueryBuilder) buildJSONProbeTail(ctx *BindContext, scanNode *plan.Node, p jsonPartialProbe, pkType plan.Type) (int32, bool) {
	proc := builder.compCtx.GetProcess()
	if proc == nil || scanNode.ObjRef == nil || scanNode.TableDef.Pkey == nil {
		return 0, false
	}
	txn := proc.GetTxnOperator()
	if txn == nil {
		return 0, false
	}
	// from is EXCLUSIVE in table_changes (it advances by one), and buildTS is the last commit the
	// index reflects, so (buildTS, snapshot] is exactly the gap. to is the read snapshot; it must be
	// <= the statement snapshot, which it is by construction.
	snap := txn.SnapshotTS()
	fromStr := fmt.Sprintf("%d-%d", p.buildTS.Physical(), p.buildTS.Logical())
	toStr := fmt.Sprintf("%d-%d", snap.PhysicalTime, snap.LogicalTime)
	exprs := []*plan.Expr{
		makePlan2StringConstExprWithType(scanNode.ObjRef.SchemaName),
		makePlan2StringConstExprWithType(scanNode.ObjRef.ObjName),
		makePlan2StringConstExprWithType(fromStr),
		makePlan2StringConstExprWithType(toStr),
	}
	tcID, err := builder.buildTableChanges(nil, ctx, exprs, nil)
	if err != nil {
		logutil.Debugf("json partial probe: table_changes build failed for %s: %v", scanNode.ObjRef.ObjName, err)
		return 0, false
	}
	tcNode := builder.qry.Nodes[tcID]
	tcTag := tcNode.BindingTags[0]

	// Locate the pk column (by name) in the table_changes output, which carries all non-hidden
	// source columns after the four change-metadata columns.
	pkPos := int32(-1)
	for i, cd := range tcNode.TableDef.Cols {
		if cd.Name == scanNode.TableDef.Pkey.PkeyColName {
			pkPos = int32(i)
			break
		}
	}
	if pkPos < 0 {
		return 0, false
	}

	// FILTER: change_type = 'insert' (the current state of a gap row; an update is delete+insert,
	// so the insert row carries the new values) AND the json predicate rebased onto these columns.
	changeType := &plan.Expr{
		Typ:  tcNode.TableDef.Cols[0].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tcTag, ColPos: 0}},
	}
	insertOnly, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=",
		[]*plan.Expr{changeType, makePlan2StringConstExprWithType("insert")})
	if err != nil {
		return 0, false
	}
	filters := []*plan.Expr{insertOnly}
	if jp := builder.rebaseToTableChanges(p.jsonPred, scanNode, tcNode, tcTag); jp != nil {
		filters = append(filters, jp)
	}
	filterID := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{tcID},
		FilterList: filters,
	}, ctx)

	// PROJECT (pk, 0::float32) to mirror the fulltext2_search (doc_id, score) layout so the two
	// arms union. The placeholder score is dropped by the group-by dedup above the union.
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{filterID},
		ProjectList: []*plan.Expr{
			{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tcTag, ColPos: pkPos}}},
			{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 0}}}},
		},
		BindingTags: []int32{projTag},
	}, ctx)
	return projID, true
}

// rebaseToTableChanges rewrites, in place, every base-scan ColRef in expr to the matching
// table_changes output column (by name), so a WHERE predicate can be evaluated on the tail. It
// returns nil (do not push) if any referenced column is absent from the table_changes output --
// the base scan still re-checks it, so the tail is merely a larger superset.
func (builder *QueryBuilder) rebaseToTableChanges(expr *plan.Expr, scanNode, tcNode *plan.Node, tcTag int32) *plan.Expr {
	scanTag := scanNode.BindingTags[0]
	pushable := true
	var walk func(e *plan.Expr)
	walk = func(e *plan.Expr) {
		if e == nil || !pushable {
			return
		}
		switch impl := e.Expr.(type) {
		case *plan.Expr_Col:
			if impl.Col.RelPos != scanTag {
				return
			}
			name := impl.Col.Name
			if name == "" && impl.Col.ColPos >= 0 && int(impl.Col.ColPos) < len(scanNode.TableDef.Cols) {
				name = scanNode.TableDef.Cols[impl.Col.ColPos].Name
			}
			newPos := int32(-1)
			for i, cd := range tcNode.TableDef.Cols {
				if cd.Name == name {
					newPos = int32(i)
					break
				}
			}
			if newPos < 0 {
				pushable = false
				return
			}
			impl.Col.RelPos = tcTag
			impl.Col.ColPos = newPos
		case *plan.Expr_F:
			for _, a := range impl.F.Args {
				walk(a)
			}
		case *plan.Expr_List:
			for _, s := range impl.List.List {
				walk(s)
			}
		}
	}
	walk(expr)
	if !pushable {
		return nil
	}
	return expr
}

// unionFtWithTail builds UNION ALL(ft bulk arm, tail arm) on the (pk, score) layout, returning the
// union node id and a pk reference through it. Both children expose col 0 = pk, col 1 = score; the
// union projects them referencing the left (ft) tag, and the executor maps the right arm
// positionally. A group-by dedup above the union then collapses duplicate pks (a row inserted
// <= buildTS and updated in the gap appears in both arms, and the probe repeats a doc per term).
func (builder *QueryBuilder) unionFtWithTail(ctx *BindContext, ftID, ftTag, tailID int32, pkType plan.Type) (int32, *plan.Expr) {
	ftNode := builder.qry.Nodes[ftID]
	tailNode := builder.qry.Nodes[tailID]
	unionTag := builder.genNewBindTag()
	unionID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_UNION_ALL,
		Children: []int32{ftID, tailID},
		ProjectList: []*plan.Expr{
			{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: ftTag, ColPos: 0}}},
			{
				Typ:  setOperationOutputType(plan.Node_UNION_ALL, ftNode.TableDef.Cols[1].Typ, tailNode.ProjectList[1].Typ),
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: ftTag, ColPos: 1}},
			},
		},
		BindingTags: []int32{unionTag},
	}, ctx)
	return unionID, &plan.Expr{
		Typ:  pkType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: unionTag, ColPos: 0}},
	}
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
	// Recorded against the SCAN, not the group: the scan is what the score-sort
	// and runtime-filter passes still hold ids for, and both must know this
	// stream is a probe.
	builder.jsonProbeFtNodes[ftNodeID] = true

	return nodeID, &plan.Expr{
		Typ:  pkCol.Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: groupTag, ColPos: 0}},
	}
}
