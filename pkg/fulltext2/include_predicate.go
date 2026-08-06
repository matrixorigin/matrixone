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

package fulltext2

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// includeFilterPred is the pushed-down predicate wire form — the SAME JSON shape the vector
// plugins use (pkg/sql/plan filterJSONPred): identical field names (col/op/val/lo/hi/vals),
// the same symbolic op strings (=,!=,<,<=,>,>=,between,in,is_null,is_not_null), the same
// bare-array serialization, and the same pk sentinel col (-1). One shape means a single
// planner predicate-peeler can target both engines. fulltext2 extends it in two ways its Go
// evaluator supports (and the numeric C++ FilterStore cannot): Val/Vals may be JSON STRINGS
// (varchar INCLUDE/pk columns store the actual value), and a "prefix" op (LIKE 'x%'). The
// predicates form a conjunction (WHERE a AND b): a row is admitted iff ALL pass.
type includeFilterPred struct {
	Col  int    `json:"col"`
	Op   string `json:"op"`
	Val  any    `json:"val,omitempty"`  // scalar comparison (=,!=,<,<=,>,>=,prefix)
	Lo   any    `json:"lo,omitempty"`   // between
	Hi   any    `json:"hi,omitempty"`   // between
	Vals []any  `json:"vals,omitempty"` // in
}

// IncludePredPkCol is the "col" sentinel for a predicate on the PRIMARY KEY (doc_id) —
// identical to the vector plugins' pkHostIdSentinelCol (-1). The pk is stored per-doc
// (seg.pk) and typed by the index's PkType, so a pk predicate is evaluated inline exactly
// like an INCLUDE column: an EXACT, JOIN-free alternative to the docfilter second-scan
// pushdown for the comparable pk types (integer family + varchar/char).
const IncludePredPkCol = -1

type includeCmpKind uint8

const (
	incEq includeCmpKind = iota
	incNe
	incLt
	incLe
	incGt
	incGe
	incBetween
	incIn
	incPrefix
	incIsNull
	incIsNotNull
)

// includeOpKinds maps the wire op strings (matching the vector plugins' symbolic ops, plus
// fulltext2's "prefix") to compiled kinds.
var includeOpKinds = map[string]includeCmpKind{
	"=": incEq, "!=": incNe, "<": incLt, "<=": incLe, ">": incGt, ">=": incGe,
	"between": incBetween, "in": incIn,
	"is_null": incIsNull, "is_not_null": incIsNotNull,
	"prefix": incPrefix,
}

// compiledIncludePred is a parsed, type-bound predicate: operands are decoded to the
// column's comparable form (int64 for the integer family, []byte for varchar/char) once at
// compile, so per-candidate evaluation is allocation-free. It is segment-independent (the
// INCLUDE column types are the same across an index's segments); includePredMembership binds
// it to a segment's includeVal.
type compiledIncludePred struct {
	col   int
	kind  includeCmpKind
	isStr bool
	ints  []int64  // integer-column operands
	strs  [][]byte // varchar/char-column operands (also the prefix)
}

// compileIncludePredicates parses the JSON spec and type-binds each predicate against the
// index's INCLUDE column types (from a loaded segment) — or against pkType for a predicate
// on the primary key (Col == IncludePredPkCol). Returns nil for an empty spec.
func compileIncludePredicates(specJSON []byte, includeTypes []int32, pkType int32) ([]compiledIncludePred, error) {
	if len(specJSON) == 0 {
		return nil, nil
	}
	// UseNumber so a JSON integer decodes to json.Number (exact int64), not float64 (which
	// loses precision past 2^53 — real for large pks).
	dec := json.NewDecoder(bytes.NewReader(specJSON))
	dec.UseNumber()
	var preds []includeFilterPred
	if err := dec.Decode(&preds); err != nil {
		return nil, err
	}
	out := make([]compiledIncludePred, 0, len(preds))
	for _, p := range preds {
		kind, ok := includeOpKinds[p.Op]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("fulltext2 include predicate: unknown op %q", p.Op)
		}
		// Resolve the operand type: the pk (Col == IncludePredPkCol) uses pkType; an INCLUDE
		// column uses includeTypes[Col].
		var colType int32
		if p.Col == IncludePredPkCol {
			colType = pkType
		} else if p.Col >= 0 && p.Col < len(includeTypes) {
			colType = includeTypes[p.Col]
		} else {
			return nil, moerr.NewInternalErrorNoCtxf("fulltext2 include predicate: col %d out of range", p.Col)
		}
		if !isComparableIncludeType(colType) {
			return nil, moerr.NewInternalErrorNoCtxf("fulltext2 include predicate: col %d type not comparable", p.Col)
		}
		cp := compiledIncludePred{col: p.Col, kind: kind, isStr: isVarlenaIncludeType(colType)}
		// Gather the op's operands from the shape-appropriate field(s).
		var operands []any
		switch kind {
		case incIsNull, incIsNotNull:
			// no operands
		case incBetween:
			operands = []any{p.Lo, p.Hi}
		case incIn:
			operands = p.Vals
		default:
			operands = []any{p.Val}
		}
		for _, o := range operands {
			if cp.isStr {
				s, ok := includeOperandString(o)
				if !ok {
					return nil, moerr.NewInternalErrorNoCtxf("fulltext2 include predicate: col %d expects a string operand", p.Col)
				}
				cp.strs = append(cp.strs, []byte(s))
			} else {
				n, ok := includeOperandInt64(o)
				if !ok {
					return nil, moerr.NewInternalErrorNoCtxf("fulltext2 include predicate: col %d expects an integer operand", p.Col)
				}
				cp.ints = append(cp.ints, n)
			}
		}
		out = append(out, cp)
	}
	return out, nil
}

// includeOperandString extracts a string operand from a decoded JSON value.
func includeOperandString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// includeOperandInt64 extracts an int64 operand from a decoded JSON value (json.Number with
// UseNumber, or float64/string defensively).
func includeOperandInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n, true
		}
		if f, err := x.Float64(); err == nil { // e.g. "1.0"
			return int64(f), true
		}
		return 0, false
	case float64:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

// isVarlenaIncludeType reports whether an INCLUDE column type is compared as bytes (varchar/
// char) rather than as an integer.
func isVarlenaIncludeType(t int32) bool {
	switch types.T(t) {
	case types.T_varchar, types.T_char:
		return true
	default:
		return false
	}
}

// isComparableIncludeType reports whether the inline evaluator can compare a value of this
// type — the integer family (normalized to int64) or varchar/char (byte-compare). A pk of an
// unsupported type (uuid/decimal/...) must fall back to the docfilter pushdown, so the
// planner never emits an inline pk predicate on it.
func isComparableIncludeType(t int32) bool {
	if isVarlenaIncludeType(t) {
		return true
	}
	switch types.T(t) {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	default:
		return false
	}
}

// test evaluates the predicate against one candidate's stored INCLUDE value with SQL
// 3-valued logic: a value comparison against NULL is UNKNOWN → the row is NOT admitted;
// only isnull/isnotnull inspect the NULL flag directly.
func (p *compiledIncludePred) test(v any, isNull bool) bool {
	switch p.kind {
	case incIsNull:
		return isNull
	case incIsNotNull:
		return !isNull
	}
	if isNull {
		return false // NULL vs value ⇒ UNKNOWN ⇒ excluded
	}
	if p.isStr {
		b := includeBytes(v)
		switch p.kind {
		case incEq:
			return bytes.Equal(b, p.strs[0])
		case incNe:
			return !bytes.Equal(b, p.strs[0])
		case incLt:
			return bytes.Compare(b, p.strs[0]) < 0
		case incLe:
			return bytes.Compare(b, p.strs[0]) <= 0
		case incGt:
			return bytes.Compare(b, p.strs[0]) > 0
		case incGe:
			return bytes.Compare(b, p.strs[0]) >= 0
		case incBetween:
			return bytes.Compare(b, p.strs[0]) >= 0 && bytes.Compare(b, p.strs[1]) <= 0
		case incPrefix:
			return bytes.HasPrefix(b, p.strs[0])
		case incIn:
			for _, s := range p.strs {
				if bytes.Equal(b, s) {
					return true
				}
			}
			return false
		}
		return false
	}
	n, ok := includeInt64(v)
	if !ok {
		return false
	}
	switch p.kind {
	case incEq:
		return n == p.ints[0]
	case incNe:
		return n != p.ints[0]
	case incLt:
		return n < p.ints[0]
	case incLe:
		return n <= p.ints[0]
	case incGt:
		return n > p.ints[0]
	case incGe:
		return n >= p.ints[0]
	case incBetween:
		return n >= p.ints[0] && n <= p.ints[1]
	case incIn:
		for _, x := range p.ints {
			if n == x {
				return true
			}
		}
		return false
	case incPrefix:
		return false // prefix is a string-only op; the planner never emits it on an int column
	}
	return false
}

// includeBytes coerces a stored varchar/char INCLUDE value to bytes (decodePk yields []byte).
func includeBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}

// includeInt64 normalizes a stored integer INCLUDE value (any int/uint width, as decodePk
// returns it) to int64 for comparison. A uint64 above math.MaxInt64 wraps — a documented
// v1 limitation; such huge unsigned filter values are not expected.
func includeInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	default:
		return 0, false
	}
}

// includePredMembership admits an ord iff every compiled INCLUDE predicate passes against
// its stored value in seg — the in-index prefilter, ANDed into the search's allow-set
// (mkAllow) alongside the docfilter/liveness memberships.
type includePredMembership struct {
	seg   *Segment
	preds []compiledIncludePred
}

func (m *includePredMembership) Contains(ord int64) bool {
	for i := range m.preds {
		p := &m.preds[i]
		var v any
		var isNull bool
		if p.col == IncludePredPkCol {
			// The primary key is stored per-doc and is never NULL.
			v = m.seg.pk(ord)
		} else {
			var err error
			v, isNull, err = m.seg.includeVal(ord, p.col)
			if err != nil {
				return false // corrupt value ⇒ exclude (safe: a covering fallback JOIN re-checks)
			}
		}
		if !p.test(v, isNull) {
			return false
		}
	}
	return true
}
