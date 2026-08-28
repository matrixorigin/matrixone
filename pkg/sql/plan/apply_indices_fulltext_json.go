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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
//
// withPath says whether the target index carries ancestor paths. It changes the
// term shape, so it must come from the index being probed, never be assumed.
func jsonExtractProbeFromExpr(expr *plan.Expr, withPath bool) (jsonProbe, bool) {
	c, ok := jsonExtractComparison(expr)
	if !ok {
		return jsonProbe{}, false
	}
	return c.probe(withPath)
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

func (c jsonComparison) probe(withPath bool) (jsonProbe, bool) {
	if c.op == "=" {
		return jsonEqualProbe(c.col, c.tag, c.lit, c.isString, withPath)
	}
	return jsonRangeProbe(c.col, c.tag, c.lit, c.op, c.isString, withPath)
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
// For json_extract_string the constant is probed under BOTH encodings when it
// also parses as a number: json_extract_string RENDERS a numeric leaf, so
// `= '3.14'` is true for a document holding the NUMBER 3.14, whose term is the
// float encoding. Probing only the string form would drop that row. The
// document must hold one of the two, so the OR stays implied.
func jsonEqualProbe(col int32, tag string, lit *plan.Literal, isString, withPath bool) (jsonProbe, bool) {
	if isString {
		s, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return jsonProbe{}, false
		}
		return jsonProbe{
			ColPos: col, Tag: tag,
			Terms: fulltext2.JSONEqualProbeTerms(tag, s.Sval, "", withPath),
		}, true
	}
	f, ok := litAsFloat(lit)
	if !ok {
		return jsonProbe{}, false
	}
	return jsonProbe{
		ColPos: col, Tag: tag,
		Terms: []string{fulltext2.JSONFloatTerm(tag, f, "", withPath)},
	}, true
}

// jsonRangeProbe builds the >, >=, < and <= probe.
//
// json_extract_float64 is the easy half: it returns NULL for every non-numeric
// leaf, so a numeric range is exactly implied — one tight range.
//
// json_extract_string is the interesting half. It RENDERS numbers, so a leaf
// satisfying the comparison may be stored under either encoding, and the two
// orders do not agree ("10" < "9" as text). The probe therefore issues TWO
// ranges and unions them:
//
//	string side  — the ordered range, since text order IS the comparison order
//	numeric side — EVERY numeric term under the tag
//
// The numeric side is deliberately NOT tightened. There is no order-preserving
// map from the text comparison onto the float encoding, so any attempt to
// narrow it risks excluding a leaf that satisfies the predicate. Returning all
// of them is a superset, and the retained predicate is re-evaluated on every
// row the probe returns, so the answer stays exact — only the scan is wider.
func jsonRangeProbe(col int32, tag string, lit *plan.Literal, op string, isString, withPath bool) (jsonProbe, bool) {
	if withPath {
		// the ancestor path sorts AFTER the value, so a value range is not a
		// contiguous term range; that needs the path-aware range form
		return jsonProbe{}, false
	}
	p := jsonProbe{ColPos: col, Tag: tag}

	if isString {
		s, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return jsonProbe{}, false
		}
		slo, shi := fulltext2.JSONStringTermBounds(tag)
		r, ok := boundedRange(fulltext2.JSONStringTerm(tag, s.Sval, "", false), op, slo, shi)
		if !ok {
			return jsonProbe{}, false
		}
		nlo, nhi := fulltext2.JSONNumericTermBounds(tag)
		p.Ranges = []jsonTermRange{r, {Lo: nlo, Hi: nhi}}
		return p, true
	}

	f, ok := litAsFloat(lit)
	if !ok {
		return jsonProbe{}, false
	}
	nlo, nhi := fulltext2.JSONNumericTermBounds(tag)
	r, ok := boundedRange(fulltext2.JSONFloatTerm(tag, f, "", false), op, nlo, nhi)
	if !ok {
		return jsonProbe{}, false
	}
	p.Ranges = []jsonTermRange{r}
	return p, true
}

// boundedRange places bound on the side op dictates and pins the open end to
// the type's own extreme, so the range never escapes one encoding.
//
// > and >= produce the same range, as do < and <=: the bound is included either
// way (see jsonTermRange). The strict forms simply return one extra term, which
// the retained predicate drops.
func boundedRange(bound, op, typeLo, typeHi string) (jsonTermRange, bool) {
	r := jsonTermRange{Lo: typeLo, Hi: typeHi}
	switch op {
	case ">", ">=":
		r.Lo = bound
	case "<", "<=":
		r.Hi = bound
	default:
		return jsonTermRange{}, false
	}
	return r, true
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

// jsonPathTag returns the trailing object key of a literal JSON path.
//
// The index term is keyed on that key, so a path whose last step is not one —
// '$', '$[0]' — has no probe. Wildcards are rejected outright: '$.a.*' and
// '$**.b' can resolve to many leaves and the tag is not determined.
//
// A trailing subscript is fine and keeps the enclosing key: '$.a[0]' probes
// tag "a", because an array element is indexed under the key that holds the
// array (see bytejson.Leaf).
func jsonPathTag(path string) (string, bool) {
	p := strings.TrimSpace(path)
	if !strings.HasPrefix(p, "$") {
		return "", false
	}
	if strings.ContainsAny(p, "*") {
		return "", false
	}
	p = p[1:]
	// drop trailing subscripts: the tag is the last KEY above them
	for strings.HasSuffix(p, "]") {
		i := strings.LastIndexByte(p, '[')
		if i < 0 {
			return "", false
		}
		p = p[:i]
	}
	i := strings.LastIndexByte(p, '.')
	if i < 0 {
		return "", false
	}
	tag := p[i+1:]
	if tag == "" || strings.ContainsAny(tag, "[]") {
		return "", false
	}
	return tag, true
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
		probe, ok := c.probe(jsonIndexFullPath(idxDef))
		if !ok {
			continue
		}
		match := builder.makeJSONProbeMatch(scanNode, c.col, probe)
		if match == nil {
			continue
		}
		scanNode.FilterList = append(scanNode.FilterList, match)
		return
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
		// resolve against the STORAGE def, as findMatchFullTextIndex does
		if !catalog.IsFullText2IndexAlgo(idx.IndexAlgo) ||
			idx.IndexAlgoTableType != catalog.FullText2Index_TblType_Storage {
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

// jsonIndexIncludeKeys / jsonIndexFullPath read the persisted term shape. Both
// must come from the index, never be assumed: an index built one way and probed
// the other way silently returns nothing.
func jsonIndexIncludeKeys(idx *plan.IndexDef) bool {
	return jsonIndexParam(idx, catalog.IndexAlgoParamJSONIncludeKeys) != "false"
}

func jsonIndexFullPath(idx *plan.IndexDef) bool {
	return jsonIndexParam(idx, catalog.IndexAlgoParamJSONIncludeFullPath) == "true"
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
