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
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func termsOf(t *testing.T, doc string, opt JSONTermOptions) []string {
	t.Helper()
	bj, err := bytejson.ParseFromString(doc)
	require.NoError(t, err)
	return JSONTupleTerms(bj, opt)
}

// THE load-bearing test: the term the BUILD writes for a document must be
// byte-identical to the term the PROBE looks up for the same key/value. If these
// two ever drift, every query silently returns nothing.
func TestBuildAndProbeTermsAgree(t *testing.T) {
	leafOnly := JSONTermOptions{IncludeKeys: true}

	got := termsOf(t, `{"a":{"b":"XXX"}}`, leafOnly)
	require.Len(t, got, 1)
	require.Equal(t, JSONStringTerm("b", "XXX", "", false), got[0])

	got = termsOf(t, `{"a":{"b":3.5}}`, leafOnly)
	require.Len(t, got, 1)
	require.Equal(t, JSONFloatTerm("b", 3.5, "", false), got[0])

	// ...and with the ancestor path carried
	full := JSONTermOptions{IncludeKeys: true, IncludeFullPath: true}
	got = termsOf(t, `{"a":{"b":"XXX"}}`, full)
	require.Len(t, got, 1)
	require.Equal(t, JSONStringTerm("b", "XXX", "a", true), got[0])
}

// The defect being fixed: the same value under different keys must not collide.
func TestTermsDistinguishKeys(t *testing.T) {
	opt := JSONTermOptions{IncludeKeys: true}
	require.NotEqual(t,
		termsOf(t, `{"b":"XXX"}`, opt),
		termsOf(t, `{"c":"XXX"}`, opt))
}

// Order preservation is what makes ranges possible; assert it directly rather
// than trusting the packer by reputation.
func TestTermOrderFollowsValueOrder(t *testing.T) {
	vals := []float64{-1e9, -3.5, -1, 0, 1, 3.14159, 3.1416, 1e9}
	terms := make([]string, len(vals))
	for i, v := range vals {
		terms[i] = JSONFloatTerm("k", v, "", false)
	}
	require.True(t, sort.StringsAreSorted(terms), "float terms must sort in value order: %q", terms)

	strs := []string{"", "a", "aa", "ab", "b", "zzz"}
	sterms := make([]string, len(strs))
	for i, s := range strs {
		sterms[i] = JSONStringTerm("k", s, "", false)
	}
	require.True(t, sort.StringsAreSorted(sterms), "string terms must sort in value order: %q", sterms)
}

// A leaf-only term must be a strict prefix of the full-path term for the same
// leaf — that is what lets a full-path index answer path-agnostic probes.
func TestLeafOnlyTermIsPrefixOfFullPathTerm(t *testing.T) {
	leaf := JSONStringTerm("b", "XXX", "", false)
	full := JSONStringTerm("b", "XXX", "a.c", true)
	require.True(t, strings.HasPrefix(full, leaf),
		"full-path term %q must extend leaf-only term %q", full, leaf)
	require.Greater(t, len(full), len(leaf))
}

// json_extract_string and json_extract_float64 are DISJOINT on leaf type:
// json_extract_string('{"v":3.14}','$.v') IS NULL. So an equality probe needs
// exactly ONE encoding — the string form — even for a numeric-looking constant.
func TestEqualProbeUsesOnlyTheStringEncoding(t *testing.T) {
	require.Len(t, JSONEqualProbeTerms("b", "XXX", "", false), 1)

	probes := JSONEqualProbeTerms("b", "3.14", "", false)
	require.Len(t, probes, 1, "a numeric-looking constant must NOT add a float term")
	require.Equal(t, JSONStringTerm("b", "3.14", "", false), probes[0])

	opt := JSONTermOptions{IncludeKeys: true}
	// the STRING document is reachable...
	require.Contains(t, probes, termsOf(t, `{"b":"3.14"}`, opt)[0])
	// ...and the NUMERIC one is not, which is correct: the predicate is NULL
	// for it, so it must not be returned
	require.NotContains(t, probes, termsOf(t, `{"b":3.14}`, opt)[0])
}

// JSON has ONE number type: {"b":3} and {"b":3.0} are the same value and must
// produce the same term, whatever integer width bytejson parsed them into. If
// they diverged, {"b":3} would be unreachable from every numeric probe.
func TestIntegerAndFloatLeavesShareOneEncoding(t *testing.T) {
	opt := JSONTermOptions{IncludeKeys: true}
	intTerm := termsOf(t, `{"b":3}`, opt)[0]
	floatTerm := termsOf(t, `{"b":3.0}`, opt)[0]
	require.Equal(t, intTerm, floatTerm, "the same JSON number must encode identically")
	require.Equal(t, JSONFloatTerm("b", 3, "", false), intTerm)

	// reachable from the NUMERIC probe (json_extract_float64), which is the only
	// extractor that returns a value for it
	require.Equal(t, JSONFloatTerm("b", 3, "", false), intTerm)

	// large integers still round-trip through the SAME normalization on both
	// sides, so they match each other even where float64 loses precision
	big := `{"b":9007199254740993}` // 2^53+1
	require.Equal(t, JSONFloatTerm("b", 9007199254740993, "", false),
		termsOf(t, big, opt)[0])
}

// Negative and zero values must keep ordering across the sign boundary.
func TestNumericTermOrderingAcrossSignBoundary(t *testing.T) {
	opt := JSONTermOptions{IncludeKeys: true}
	neg := termsOf(t, `{"b":-1}`, opt)[0]
	zero := termsOf(t, `{"b":0}`, opt)[0]
	pos := termsOf(t, `{"b":1}`, opt)[0]
	require.True(t, neg < zero && zero < pos, "negative < zero < positive must hold in term order")
}

// Truncation must be symmetric or long values silently stop matching.
func TestTruncationIsSymmetric(t *testing.T) {
	long := strings.Repeat("x", 400)
	docTerm := termsOf(t, `{"b":"`+long+`"}`, JSONTermOptions{IncludeKeys: true})[0]
	probe := JSONStringTerm("b", long, "", false)
	require.Equal(t, docTerm, probe)
	require.LessOrEqual(t, len(docTerm), maxTermBytes)
}

// IncludeKeys=false keeps the historical value-only index: no tuple terms.
func TestIncludeKeysFalseEmitsNoTupleTerms(t *testing.T) {
	require.Empty(t, termsOf(t, `{"a":{"b":"XXX"}}`, JSONTermOptions{}))
}

func TestDefaultOptionsIncludeKeys(t *testing.T) {
	require.True(t, DefaultJSONTermOptions().IncludeKeys)
	require.False(t, DefaultJSONTermOptions().IncludeFullPath)
}

// Terms are raw packed bytes, not text: they legitimately contain 0x00 and
// BOOLEAN-mode syntax bytes. This is the evidence that they must never be
// routed through a pattern parser or a SQL literal.
func TestTermsAreBinaryNotText(t *testing.T) {
	term := JSONStringTerm("b", "XXX", "", false)
	require.Contains(t, term, "\x00", "packed terms carry NUL; the path must be binary-clean")

	// a value made of pattern metacharacters round-trips unharmed
	meta := `+a -b *c "d" (e)`
	doc, err := json.Marshal(map[string]string{"b": meta})
	require.NoError(t, err)
	docTerm := termsOf(t, string(doc), JSONTermOptions{IncludeKeys: true})[0]
	require.Equal(t, JSONStringTerm("b", meta, "", false), docTerm)
}

// Arrays: every element is reachable under the enclosing key.
func TestArrayElementsProduceTermsUnderTheKey(t *testing.T) {
	terms := termsOf(t, `{"a":[1,2]}`, JSONTermOptions{IncludeKeys: true})
	require.Len(t, terms, 2)
	require.Contains(t, terms, JSONFloatTerm("a", 1, "", false))
	require.Contains(t, terms, JSONFloatTerm("a", 2, "", false))
}

// --- CREATE / ISCP parity --------------------------------------------------

// The two build paths must emit byte-identical (word, pos) pairs for the same
// document. A mismatch here is the exact failure mode recorded at query.go:264:
// rows inserted after CREATE INDEX become silently unsearchable.
func TestCreateAndIscpAgreeOnTerms(t *testing.T) {
	const doc = `{"a":{"b":"XXX","n":3},"z":[1,"q"]}`

	for _, opt := range []JSONTermOptions{
		{IncludeKeys: true},
		{IncludeKeys: true, IncludeFullPath: true},
	} {
		// CREATE side: raw text/varchar json bytes off the source vector
		createTerms, err := JSONTupleColumnTerms([]byte(doc), false, opt)
		require.NoError(t, err)
		create := make([]WordPos, len(createTerms))
		for i, term := range createTerms {
			create[i] = WordPos{Word: term, Pos: int32(i)}
		}

		// ISCP side: the writer encodes finished terms, the tokenizer decodes them
		iscpTerms, err := JSONTupleColumn(doc, opt)
		require.NoError(t, err)
		tokenize, err := CdcTokenizerWithJSONOptions(ParserJSON, opt)
		require.NoError(t, err)
		iscp := tokenize(EncodeJSONTermCarrier(iscpTerms))

		require.Equal(t, create, iscp, "CREATE and ISCP must agree (opt=%+v)", opt)
		require.NotEmpty(t, create)
	}
}

// A T_json column reaches CREATE as raw stored bytes and ISCP as an
// already-parsed ByteJson. Both must land on the same terms.
func TestCreateAndIscpAgreeForBinaryJSONColumn(t *testing.T) {
	bj, err := bytejson.ParseFromString(`{"a":{"b":"XXX","n":3.5}}`)
	require.NoError(t, err)
	raw, err := bj.Marshal()
	require.NoError(t, err)

	opt := JSONTermOptions{IncludeKeys: true, IncludeFullPath: true}
	fromRaw, err := JSONTupleColumnTerms(raw, true, opt) // CREATE: T_json raw bytes
	require.NoError(t, err)
	fromParsed, err := JSONTupleColumn(bj, opt) // ISCP: already-parsed
	require.NoError(t, err)
	require.Equal(t, fromRaw, fromParsed)
	require.NotEmpty(t, fromRaw)
}

// The carrier must survive terms containing NUL and pattern metacharacters —
// which every packed term does.
func TestTermCarrierRoundTripsBinary(t *testing.T) {
	terms := []string{"a\x00b", "", "\xff\xfe", `+x -y "z"`, strings.Repeat("q", 127)}
	got := DecodeJSONTermCarrier(EncodeJSONTermCarrier(terms))
	require.Len(t, got, len(terms))
	for i, w := range got {
		require.Equal(t, terms[i], w.Word)
		require.Equal(t, int32(i), w.Pos)
	}
	require.Empty(t, DecodeJSONTermCarrier(""))
}

// A TableConfig that never set the json fields must still index keys: the
// default is ON, so the zero value has to mean ON or a forgotten field would
// silently produce an index with no tuple terms.
func TestZeroTableConfigStillIndexesKeys(t *testing.T) {
	var cfg TableConfig
	require.True(t, cfg.JSONTermOptions().IncludeKeys)
	require.False(t, cfg.JSONTermOptions().IncludeFullPath)

	cfg.Parser = ParserJSON
	require.True(t, cfg.UsesJSONTupleTerms())

	// json_value keeps its own whole-value tokenization
	cfg.Parser = ParserJSONValue
	require.False(t, cfg.UsesJSONTupleTerms())

	// explicit opt-out
	cfg = TableConfig{Parser: ParserJSON, JSONNoKeys: true}
	require.False(t, cfg.UsesJSONTupleTerms())
}

func TestJSONTermOptionsFromParams(t *testing.T) {
	require.Equal(t, JSONTermOptions{IncludeKeys: true}, JSONTermOptionsFrom("", ""))
	require.Equal(t, JSONTermOptions{IncludeKeys: true, IncludeFullPath: true},
		JSONTermOptionsFrom("", "true"))
	require.Equal(t, JSONTermOptions{}, JSONTermOptionsFrom("false", ""))
	// a full path with no key to hang it on is meaningless, not a half-state
	require.Equal(t, JSONTermOptions{}, JSONTermOptionsFrom("false", "true"))
}

// A non-tuple parser must be unaffected by the new code path.
func TestNonTupleParsersUnchanged(t *testing.T) {
	for _, p := range []string{ParserNgram, ParserDefault, ParserJSONValue} {
		tokenize, err := CdcTokenizerWithJSONOptions(p, JSONTermOptions{IncludeKeys: true})
		require.NoError(t, err)
		require.NotNil(t, tokenize)
		// it must NOT be the carrier decoder: plain text still tokenizes as text
		require.NotEmpty(t, tokenize("hello world"))
	}
}

// --- probe payload + term range ---------------------------------------------

func TestJSONProbePayloadRoundTrip(t *testing.T) {
	terms := []string{"a\x00b", "", `+x -y "z"`}
	ranges := [][2]string{{"lo\x00", "hi\xff"}, {"", ""}}
	got, ok := DecodeJSONProbePayload(EncodeJSONProbePayload(terms, ranges))
	require.True(t, ok)
	require.Equal(t, terms, got.Terms)
	require.Equal(t, ranges, got.Ranges)

	// empty probe still round-trips (and is distinguishable from garbage)
	got, ok = DecodeJSONProbePayload(EncodeJSONProbePayload(nil, nil))
	require.True(t, ok)
	require.Empty(t, got.Terms)
	require.Empty(t, got.Ranges)
}

// A malformed payload must be REPORTED, not silently read as an empty probe:
// an empty probe matches nothing, which would drop every row.
func TestJSONProbePayloadRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"x", "\x01\x00\x00\x00", "\x01\x00\x00\x00\xff\xff\xff\xff",
		EncodeJSONProbePayload([]string{"a"}, nil) + "trailing"} {
		_, ok := DecodeJSONProbePayload(bad)
		require.False(t, ok, "%q must be rejected", bad)
	}
}

func TestSegmentTermRange(t *testing.T) {
	seg := &Segment{sortedTerms: []string{"a", "b", "c", "c", "d", "f"}}
	require.Equal(t, []string{"b", "c", "c", "d"}, seg.TermRange("b", "d"))
	require.Equal(t, []string{"c", "c"}, seg.TermRange("c", "c"), "both ends inclusive")
	require.Equal(t, []string{"a", "b", "c", "c", "d", "f"}, seg.TermRange("", "z"))
	require.Empty(t, seg.TermRange("d", "b"), "inverted range is empty")
	require.Empty(t, seg.TermRange("x", "z"))
	// bounds that fall between existing terms
	require.Equal(t, []string{"b", "c", "c"}, seg.TermRange("ab", "cz"))
}

// The range must select exactly the terms a numeric comparison implies.
func TestTermRangeSelectsNumericTerms(t *testing.T) {
	var terms []string
	for _, v := range []float64{-10, -1, 0, 1, 3.14, 100} {
		terms = append(terms, JSONFloatTerm("n", v, "", false))
	}
	sort.Strings(terms)
	seg := &Segment{sortedTerms: terms}

	// n >= 1  ->  [term(1), +Inf]
	_, hi := JSONNumericTermBounds("n")
	got := seg.TermRange(JSONFloatTerm("n", 1, "", false), hi)
	require.Equal(t, 3, len(got), "1, 3.14 and 100 qualify")

	// n < 0  ->  [-Inf, term(0)]  (inclusive, so 0 comes back too and the
	// retained predicate drops it)
	lo, _ := JSONNumericTermBounds("n")
	got = seg.TermRange(lo, JSONFloatTerm("n", 0, "", false))
	require.Equal(t, 3, len(got), "-10, -1 and the boundary 0")
}
