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
	// LeafDecimal carries its decimal TEXT in Str; the encoder parses that into
	// the numeric encoding, which is the only form a probe can reach.
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
// Str/I64/U64/F64 carry the value; exactly one is meaningful, selected by Kind.
// Str aliases the document buffer and is only valid until the next iteration
// step — copy it if it must outlive that.
type Leaf struct {
	Tag  []byte
	Kind LeafKind
	Str  []byte
	I64  int64
	U64  uint64
	F64  float64
}

// TokenizeLeaves yields every scalar leaf of the document with its Tag. It is
// the structure-aware counterpart of TokenizeValue, which reports values only
// and so cannot distinguish {"b":"X"} from {"c":"X"}.
func (bj ByteJson) TokenizeLeaves() iter.Seq[Leaf] {
	return func(yield func(Leaf) bool) {
		w := leafWalker{yield: yield}
		w.walk(bj, nil)
	}
}

type leafWalker struct {
	yield func(Leaf) bool
}

// walk descends bj. tag is the nearest enclosing object key.
func (w *leafWalker) walk(bj ByteJson, tag []byte) bool {
	switch bj.Type {
	case TpCodeObject:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			if !w.walk(bj.getObjectVal(i), bj.getObjectKey(i)) {
				return false
			}
		}
		return true

	case TpCodeArray:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			// Array elements keep the enclosing key as their tag: the subscript
			// is BELOW the tag, so {"a":[1,2]} yields Tag "a" twice.
			if !w.walk(bj.getArrayElem(i), tag) {
				return false
			}
		}
		return true

	// Date/Time/Datetime store their text and json_extract_string renders exactly
	// those bytes (CompareByteJson groups them with TpCodeString for the same
	// reason), so they index as strings. Leaving them out would make
	// json_extract_string(...) = '2024-01-02' miss a document that satisfies it.
	case TpCodeString, TpCodeDate, TpCodeTime, TpCodeDatetime:
		return w.yield(Leaf{Tag: tag, Kind: LeafString, Str: bj.GetString()})
	case TpCodeInt64:
		return w.yield(Leaf{Tag: tag, Kind: LeafInt64, I64: bj.GetInt64()})
	case TpCodeUint64:
		return w.yield(Leaf{Tag: tag, Kind: LeafUint64, U64: bj.GetUint64()})
	case TpCodeFloat64:
		return w.yield(Leaf{Tag: tag, Kind: LeafFloat64, F64: bj.GetFloat64()})
	case TpCodeDecimal:
		// Numeric to both extractors: json_extract_float64 returns it as a
		// number, json_extract_string returns NULL for it. Str carries the
		// decimal text; the encoder parses it into the numeric form.
		return w.yield(Leaf{Tag: tag, Kind: LeafDecimal, Str: bj.GetString()})
	default:
		// TpCodeLiteral (true/false/null), Blob, Opaque and Bit are not indexed.
		// json_extract_string can still render some of those, so a probe must not
		// be synthesized for a column that may hold them — see the caller's gate.
		return true
	}
}
