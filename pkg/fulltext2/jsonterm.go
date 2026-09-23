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
	"encoding/binary"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// JSON tuple terms.
//
// A leaf whose nearest object key is z and whose value is V is indexed as the
// order-preserving tuple ( z , V ). types.Packer is byte-order preserving, so
// lexicographic order over terms IS value order within a type — which is what
// turns `json_extract_float64(j,'$.a.b') > 3.14` into a term-range scan.
//
// Tag first, then value, so prefix(tag) is every value under that key
// (range-scannable) and (tag, value) is an exact lookup.
//
// The term deliberately does NOT carry the ancestor path: a probe is
// path-agnostic, matching that key/value wherever it appears. A predicate on a
// specific path therefore gets a superset, which the retained predicate
// narrows.
//
// The terms are RAW packed bytes, not hex: they contain 0x00 and bytes that are
// BOOLEAN-mode pattern syntax, so they must never be routed through a pattern
// parser or a SQL string literal. The probe carries them as a varbinary
// function argument instead.

// maxTermBytes bounds a whole packed term. It is bytejson.MAX_TOKEN_SIZE so a
// tuple term and a value token share one limit. It is only a backstop now that
// the value element is capped separately.
const maxTermBytes = bytejson.MAX_TOKEN_SIZE

// maxTermValueBytes caps the VALUE element of a tuple term.
//
// Capping the value rather than the finished term is what keeps the term
// well-formed: an over-long value would otherwise consume the whole budget and
// crowd out the ancestor path, leaving a term that is not the tuple it claims
// to be. With the value bounded, tag and path always survive.
//
// Both the build side and the probe side truncate through truncValue, so the
// two agree. Equality on an over-long value therefore degrades to equality on
// its first maxTermValueBytes bytes — a PREFIX match. That returns every
// document sharing the prefix, a superset of the true equality, and the
// retained predicate removes the extras. Ranges keep the same property because
// their bounds are inclusive: a value greater than the bound either differs
// inside the prefix (so its term sorts above) or shares it (so its term equals
// the bound and the inclusive end keeps it).
//
// Truncation is by BYTES and may split a UTF-8 rune. That is harmless: terms
// are compared as bytes, and both sides cut at the same offset.
const maxTermValueBytes = 100

// truncValue bounds a string value element. The ONE place either side cuts.
func truncValue(v []byte) []byte {
	if len(v) > maxTermValueBytes {
		return v[:maxTermValueBytes]
	}
	return v
}

// JSONTermOptions is the per-index term shape, persisted in IndexAlgoParams.
// Both the CREATE build and the CDC build must read the same options, or the
// two halves of one index disagree and rows silently stop matching.
type JSONTermOptions struct {
	// IncludeKeys indexes the (tag, value) tuple. Default true. When false the
	// index keeps the historical value-only tokenization.
	IncludeKeys bool
}

// DefaultJSONTermOptions is what a plain `WITH PARSER json` means for a NEW
// index. Indexes built before these options existed must NOT be read as this —
// they hold value-only terms and are recorded as IncludeKeys=false.
func DefaultJSONTermOptions() JSONTermOptions {
	return JSONTermOptions{IncludeKeys: true}
}

// JSONTupleTerms returns the tuple terms of one JSON document.
//
// It is the single encoder shared by the CREATE and CDC build paths. Those two
// paths tokenizing independently is a known source of silently unsearchable
// rows, so neither is allowed its own copy of this logic.
func JSONTupleTerms(bj bytejson.ByteJson, opt JSONTermOptions) []string {
	if !opt.IncludeKeys {
		return nil
	}
	p := types.NewPacker()
	defer p.Close()

	var terms []string
	emit := func(l bytejson.Leaf, value func(*types.Packer)) {
		p.Reset()
		p.EncodeStringType(l.Tag)
		value(p)
		terms = append(terms, truncateTerm(p.Bytes()))
	}
	for l := range bj.TokenizeLeaves() {
		if l.Kind == bytejson.LeafDecimal {
			// A decimal is NUMERIC to both extractors: json_extract_float64
			// returns it as a number and json_extract_string returns NULL for it,
			// so only the numeric form is ever probed. An unparsable decimal text
			// yields no term rather than a wrong one.
			if f, err := strconv.ParseFloat(string(l.Str), 64); err == nil {
				emit(l, func(p *types.Packer) { p.EncodeFloat64(f) })
			}
			continue
		}
		emit(l, func(p *types.Packer) { packLeafValue(p, l) })
	}
	return terms
}

// packLeafValue writes the leaf's value. Stringifying a number here would
// destroy the ordering that makes range scans possible.
//
// EVERY number is encoded as float64, whatever integer width bytejson happened
// to parse it into. JSON has a single number type: {"b":3} and {"b":3.0} are the
// same value and must produce the same term, and a probe cannot know which
// internal width a document used. Encoding ints as ints would make {"b":3}
// unreachable from any numeric probe — a dropped row.
//
// Integers beyond 2^53 lose precision in the term. That is safe in one
// direction only, which is the direction that matters: the SAME normalization
// runs on both the build and the probe side, so two ints that collide as
// doubles produce one term and match each other — a false positive the retained
// predicate removes. No row is ever dropped.
func packLeafValue(p *types.Packer, l bytejson.Leaf) {
	switch l.Kind {
	case bytejson.LeafString:
		p.EncodeStringType(truncValue(l.Str))
	case bytejson.LeafInt64:
		p.EncodeFloat64(float64(l.I64))
	case bytejson.LeafUint64:
		p.EncodeFloat64(float64(l.U64))
	case bytejson.LeafFloat64:
		p.EncodeFloat64(l.F64)
	}
}

// truncateTerm bounds a term to maxTermBytes.
//
// Both the build side and the probe side truncate identically, so
// T == Q implies T[:n] == Q[:n]: an over-long term degrades to a prefix match,
// which yields extra rows (removed by the retained predicate) but never drops
// one. Truncating on only one side would silently lose rows.
func truncateTerm(b []byte) string {
	if len(b) > maxTermBytes {
		b = b[:maxTermBytes]
	}
	return string(b)
}

// path is used only when the index carries full paths; pass withPath=false for
// a leaf-only index.
// JSONStringTerm builds the probe term for a string constant at key tag. The
// value is truncated exactly as the build side truncates it, so an over-long
// constant probes a prefix (a superset) rather than missing every document.
func JSONStringTerm(tag, value string) string {
	return packProbe(func(p *types.Packer) {
		p.EncodeStringType([]byte(tag))
		p.EncodeStringType(truncValue([]byte(value)))
	})
}

// JSONFloatTerm builds the probe term for a numeric constant at key tag. It is
// the ONLY numeric encoding: integer leaves are normalized to float64 at index
// time (see packLeafValue), so this one probe reaches them all.
func JSONFloatTerm(tag string, value float64) string {
	return packProbe(func(p *types.Packer) {
		p.EncodeStringType([]byte(tag))
		p.EncodeFloat64(value)
	})
}

func packProbe(head func(*types.Packer)) string {
	p := types.NewPacker()
	defer p.Close()
	head(p)
	return truncateTerm(p.Bytes())
}

// JSONEqualProbeTerms returns the terms implied by
// `json_extract_string(col,'$.<path>.<tag>') = value` — a single exact term.
//
// Only the STRING encoding, because json_extract_string returns NULL for every
// numeric leaf (verified: json_extract_string('{"v":3.14}','$.v') IS NULL). A
// document satisfying the comparison therefore always holds the string form,
// and probing the float encoding too would add a term no qualifying document
// can hold.
func JSONEqualProbeTerms(tag, value string) []string {
	return []string{JSONStringTerm(tag, value)}
}

// JSONNumericTermBounds returns the term range covering EVERY numeric leaf
// under tag: ±Inf are the extreme float64 values, so the packed bounds bracket
// every finite value the encoder can produce. An open-ended comparison
// (`> 3.14` has no upper bound) uses these as its other end.
//
// NaN is deliberately not covered. It compares false against every SQL operator,
// so no `<op> const` predicate this rule rewrites can be true for a NaN leaf,
// and excluding it drops no row.
func JSONNumericTermBounds(tag string) (loTerm, hiTerm string) {
	return JSONFloatTerm(tag, math.Inf(-1)),
		JSONFloatTerm(tag, math.Inf(1))
}

// JSONStringTermBounds returns the term range covering EVERY string leaf under
// tag. The empty string is the smallest encoded string and EncodeStringTypeMax
// sorts above any of them, so the pair brackets them all.
func JSONStringTermBounds(tag string) (loTerm, hiTerm string) {
	lo := packProbe(func(p *types.Packer) {
		p.EncodeStringType([]byte(tag))
		p.EncodeStringType(nil)
	})
	hi := packProbe(func(p *types.Packer) {
		p.EncodeStringType([]byte(tag))
		p.EncodeStringTypeMax()
	})
	return lo, hi
}

// JSONTermOptions resolves the term shape this index was built with. See the
// TableConfig fields for why the "no keys" flag is stored inverted.
func (c TableConfig) JSONTermOptions() JSONTermOptions {
	if c.JSONNoKeys {
		return JSONTermOptions{}
	}
	return JSONTermOptions{IncludeKeys: true}
}

// UsesJSONTupleTerms reports whether this config indexes json as (tag, value)
// tuples. json_value keeps its own whole-value tokenization and is excluded.
func (c TableConfig) UsesJSONTupleTerms() bool {
	return IsJSONParser(c.Parser) && !IsJSONValueParser(c.Parser) && c.JSONTermOptions().IncludeKeys
}

// JSONTupleColumnTerms parses ONE indexed column's raw json value and returns
// its tuple terms. binary selects the encoding: true for a T_json column's
// stored bytes, false for text/varchar json — the same distinction FlattenJSON
// makes.
//
// Both build paths call this, per column, in column order. That is what makes a
// CREATE-built row and an ISCP-built row byte-identical.
func JSONTupleColumnTerms(raw []byte, binary bool, opt JSONTermOptions) ([]string, error) {
	var bj bytejson.ByteJson
	var err error
	if binary {
		err = bj.Unmarshal(raw)
	} else {
		bj, err = bytejson.ParseFromString(string(raw))
	}
	if err != nil {
		return nil, err
	}
	return JSONTupleTerms(bj, opt), nil
}

// JSONTupleColumn is the ISCP-side analogue of FlattenJSONColumn: a T_json
// column arrives from the CDC row already parsed as a bytejson.ByteJson
// (repr-independent), while a text/varchar json column arrives as string or
// []byte. Both funnel into the same JSONTupleTerms as the CREATE path's raw
// bytes do, which is what keeps the two builds byte-identical.
func JSONTupleColumn(v any, opt JSONTermOptions) ([]string, error) {
	switch t := v.(type) {
	case bytejson.ByteJson:
		return JSONTupleTerms(t, opt), nil
	case []byte:
		return JSONTupleColumnTerms(t, false, opt)
	case string:
		return JSONTupleColumnTerms([]byte(t), false, opt)
	default:
		return nil, nil
	}
}

// JSONTermOptionsFrom builds the options from their IndexAlgoParams string
// values. Absent ("") takes the default, so a plain `WITH PARSER json` index
// gets DefaultJSONTermOptions.
func JSONTermOptionsFrom(includeKeys string) JSONTermOptions {
	opt := DefaultJSONTermOptions()
	if includeKeys != "" {
		opt.IncludeKeys = includeKeys == "true"
	}
	return opt
}

// --- CDC carrier -----------------------------------------------------------
//
// The ISCP (incremental index build) path moves a row from the writer to the
// TailBuilder as one opaque string inside the CDC blob, which is length-
// prefixed and CRC-checked (Cdc.Encode), so it carries arbitrary bytes safely.
// Tuple terms cannot be recovered from the old space-joined flatten — that
// throws the keys away — so for a tuple json index the writer encodes the
// FINISHED terms and the tokenizer just decodes them. Both sides of the index
// therefore run the one encoder in this file, which is what keeps CREATE and
// ISCP byte-identical.

// EncodeJSONTermCarrier packs terms as [u32 len][bytes]… .
func EncodeJSONTermCarrier(terms []string) string {
	if len(terms) == 0 {
		return ""
	}
	n := 4 * len(terms)
	for _, t := range terms {
		n += len(t)
	}
	buf := make([]byte, 0, n)
	var hdr [4]byte
	for _, t := range terms {
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(t)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, t...)
	}
	return string(buf)
}

// DecodeJSONTermCarrier reverses EncodeJSONTermCarrier, numbering the terms in
// order. A malformed carrier yields what was parsed so far rather than an
// error: the blob is already CRC-checked upstream, so a short read here means a
// bug, not corruption, and dropping the tail is preferable to failing the whole
// incremental build.
func DecodeJSONTermCarrier(s string) []WordPos {
	var out []WordPos
	for i, pos := 0, int32(0); i+4 <= len(s); pos++ {
		n := int(binary.LittleEndian.Uint32([]byte(s[i : i+4])))
		i += 4
		if n < 0 || i+n > len(s) {
			break
		}
		out = append(out, WordPos{Word: s[i : i+n], Pos: pos})
		i += n
	}
	return out
}
