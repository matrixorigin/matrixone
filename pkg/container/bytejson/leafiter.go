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

package bytejson

import "iter"

// LeafKind is the JSON type of a scalar leaf, kept as a small enum so callers
// can encode the value under its own type instead of stringifying it.
//
// The set is chosen to cover every leaf a json_extract_* function can return a
// non-NULL value for, because an unindexed-but-extractable leaf makes an index
// probe drop rows. TpCodeLiteral (true/false/null), Blob, Opaque and Bit are
// still outside it — see the walker's default branch.
type LeafKind uint8

const (
	LeafString LeafKind = iota
	LeafInt64
	LeafUint64
	LeafFloat64
	// LeafDecimal is reachable as BOTH a number and a string (see the walker),
	// so its Str holds the decimal text and the encoder emits both forms.
	LeafDecimal
)

// Leaf is one scalar value of a JSON document together with the path that
// reached it, split at the last object key.
//
// Tag is the nearest enclosing object key. Array elements inherit the key of
// the object member holding the array, so {"a":[1,2]} yields Tag "a" twice:
// a user asks for '$.a[0]', and a probe on ("a", 1) stays a necessary
// condition for that predicate (the wrong index is a false positive, which the
// retained predicate removes). Tag is empty only for a document that is itself
// a scalar.
//
// AncestorPath is everything above Tag, dot-joined ("a.b"), and is empty for a
// top-level member. Array subscripts are NOT part of it: the elements of
// {"a":[{"b":1},{"b":2}]} both report Tag "b" and AncestorPath "a", so a probe
// derived from '$.a[0].b' also matches the '$.a[1].b' document. That is a false
// positive, which the retained predicate removes; keeping subscripts would make
// the term depend on element order and buy nothing the predicate does not
// already decide.
//
// Str/I64/U64/F64 carry the value; exactly one is meaningful, selected by Kind.
// Str aliases the document buffer and is only valid until the next iteration
// step — copy it if it must outlive that.
type Leaf struct {
	Tag          []byte
	AncestorPath []byte
	Kind         LeafKind
	Str          []byte
	I64          int64
	U64          uint64
	F64          float64
}

// TokenizeLeaves yields every scalar leaf of the document with its Tag and
// AncestorPath. It is the structure-aware counterpart of TokenizeValue, which
// reports values only and so cannot distinguish {"b":"X"} from {"c":"X"}.
//
// withPath controls whether AncestorPath is materialized. Building it costs a
// per-leaf append into a reused buffer, so a caller that does not need the path
// (the common leaf-only index) passes false and pays nothing.
func (bj ByteJson) TokenizeLeaves(withPath bool) iter.Seq[Leaf] {
	return func(yield func(Leaf) bool) {
		w := leafWalker{yield: yield, withPath: withPath}
		w.walk(bj, nil, 0)
	}
}

type leafWalker struct {
	yield    func(Leaf) bool
	withPath bool
	// path is the dotted ancestor path built in place; children append and
	// truncate back, so one buffer serves the whole walk.
	path []byte
}

// walk descends bj. tag is the nearest enclosing object key. pathLen is the
// length of w.path that is the ancestor path *of tag* — children restore to it.
func (w *leafWalker) walk(bj ByteJson, tag []byte, pathLen int) bool {
	switch bj.Type {
	case TpCodeObject:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			key := bj.getObjectKey(i)
			childPathLen := pathLen
			if w.withPath {
				// the ancestor path of key is (ancestors of tag) + tag
				w.path = w.path[:pathLen]
				if len(tag) > 0 {
					if pathLen > 0 {
						w.path = append(w.path, '.')
					}
					w.path = append(w.path, tag...)
				}
				childPathLen = len(w.path)
			}
			if !w.walk(bj.getObjectVal(i), key, childPathLen) {
				return false
			}
		}
		return true

	case TpCodeArray:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			// Array elements keep the enclosing key as their tag and its
			// ancestor path unchanged: the subscript is BELOW the tag, so it is
			// part of neither (see Leaf.AncestorPath).
			if !w.walk(bj.getArrayElem(i), tag, pathLen) {
				return false
			}
		}
		return true

	// Date/Time/Datetime store their text and json_extract_string renders exactly
	// those bytes (CompareByteJson groups them with TpCodeString for the same
	// reason), so they index as strings. Leaving them out would make
	// json_extract_string(...) = '2024-01-02' miss a document that satisfies it.
	case TpCodeString, TpCodeDate, TpCodeTime, TpCodeDatetime:
		return w.emit(Leaf{Tag: tag, Kind: LeafString, Str: bj.GetString()}, pathLen)
	case TpCodeInt64:
		return w.emit(Leaf{Tag: tag, Kind: LeafInt64, I64: bj.GetInt64()}, pathLen)
	case TpCodeUint64:
		return w.emit(Leaf{Tag: tag, Kind: LeafUint64, U64: bj.GetUint64()}, pathLen)
	case TpCodeFloat64:
		return w.emit(Leaf{Tag: tag, Kind: LeafFloat64, F64: bj.GetFloat64()}, pathLen)
	case TpCodeDecimal:
		// json_extract_float64 returns a decimal leaf as a number while
		// json_extract_string renders its text, so it is reachable from both and
		// must be indexed under both. Str carries the text; the encoder also
		// emits the numeric form.
		return w.emit(Leaf{Tag: tag, Kind: LeafDecimal, Str: bj.GetString()}, pathLen)
	default:
		// TpCodeLiteral (true/false/null), Blob, Opaque and Bit are not indexed.
		// json_extract_string can still render some of those, so a probe must not
		// be synthesized for a column that may hold them — see the caller's gate.
		return true
	}
}

func (w *leafWalker) emit(l Leaf, pathLen int) bool {
	if w.withPath {
		l.AncestorPath = w.path[:pathLen]
	}
	return w.yield(l)
}
