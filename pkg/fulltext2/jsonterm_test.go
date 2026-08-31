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
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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
	require.Equal(t, JSONStringTerm("b", "XXX"), got[0])

	got = termsOf(t, `{"a":{"b":3.5}}`, leafOnly)
	require.Len(t, got, 1)
	require.Equal(t, JSONFloatTerm("b", 3.5), got[0])

	// a nested leaf produces the same term as a top-level one: the probe is
	// path-agnostic by design
	require.Equal(t, termsOf(t, `{"b":"XXX"}`, leafOnly),
		termsOf(t, `{"a":{"b":"XXX"}}`, leafOnly))
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
		terms[i] = JSONFloatTerm("k", v)
	}
	require.True(t, sort.StringsAreSorted(terms), "float terms must sort in value order: %q", terms)

	strs := []string{"", "a", "aa", "ab", "b", "zzz"}
	sterms := make([]string, len(strs))
	for i, s := range strs {
		sterms[i] = JSONStringTerm("k", s)
	}
	require.True(t, sort.StringsAreSorted(sterms), "string terms must sort in value order: %q", sterms)
}

// json_extract_string and json_extract_float64 are DISJOINT on leaf type:
// json_extract_string('{"v":3.14}','$.v') IS NULL. So an equality probe needs
// exactly ONE encoding — the string form — even for a numeric-looking constant.
func TestEqualProbeUsesOnlyTheStringEncoding(t *testing.T) {
	require.Len(t, JSONEqualProbeTerms("b", "XXX"), 1)

	probes := JSONEqualProbeTerms("b", "3.14")
	require.Len(t, probes, 1, "a numeric-looking constant must NOT add a float term")
	require.Equal(t, JSONStringTerm("b", "3.14"), probes[0])

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
	require.Equal(t, JSONFloatTerm("b", 3), intTerm)

	// reachable from the NUMERIC probe (json_extract_float64), which is the only
	// extractor that returns a value for it
	require.Equal(t, JSONFloatTerm("b", 3), intTerm)

	// large integers still round-trip through the SAME normalization on both
	// sides, so they match each other even where float64 loses precision
	big := `{"b":9007199254740993}` // 2^53+1
	require.Equal(t, JSONFloatTerm("b", 9007199254740993),
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

// The value element is capped at maxTermValueBytes on BOTH sides, so equality on
// an over-long value degrades to a PREFIX match: a superset, which the retained
// predicate narrows. Truncating on only one side would drop the row instead.
func TestValueTruncationIsSymmetricAndPrefixMatching(t *testing.T) {
	opt := JSONTermOptions{IncludeKeys: true}
	long := strings.Repeat("x", maxTermValueBytes+300)

	docTerm := termsOf(t, `{"b":"`+long+`"}`, opt)[0]
	require.Equal(t, docTerm, JSONStringTerm("b", long), "both sides cut identically")
	require.LessOrEqual(t, len(docTerm), maxTermBytes)

	// two values agreeing only on the first maxTermValueBytes collapse to ONE
	// term — the superset the prefix match returns
	a := strings.Repeat("x", maxTermValueBytes) + "AAA"
	b := strings.Repeat("x", maxTermValueBytes) + "BBB"
	require.Equal(t, termsOf(t, `{"b":"`+a+`"}`, opt), termsOf(t, `{"b":"`+b+`"}`, opt))
	// ...and a probe for either reaches both, so no row is dropped
	require.Equal(t, termsOf(t, `{"b":"`+a+`"}`, opt)[0], JSONStringTerm("b", b))

	// At the boundary the collapse is expected, not a defect: a value of exactly
	// maxTermValueBytes and a longer one sharing that prefix are the same term.
	// The retained predicate is what separates them.
	exact := strings.Repeat("y", maxTermValueBytes)
	require.Equal(t, termsOf(t, `{"b":"`+exact+`"}`, opt)[0],
		termsOf(t, `{"b":"`+exact+`z"}`, opt)[0])
	// one byte under the cap is untouched, so it stays distinct
	short := strings.Repeat("y", maxTermValueBytes-1)
	require.NotEqual(t, termsOf(t, `{"b":"`+short+`"}`, opt)[0],
		termsOf(t, `{"b":"`+short+`z"}`, opt)[0],
		"values below the cap must remain exactly matched")

	// the tag still survives an over-long value: it is encoded before it
	require.NotEqual(t, JSONStringTerm("b", long), JSONStringTerm("c", long))
}

// IncludeKeys=false keeps the historical value-only index: no tuple terms.
func TestIncludeKeysFalseEmitsNoTupleTerms(t *testing.T) {
	require.Empty(t, termsOf(t, `{"a":{"b":"XXX"}}`, JSONTermOptions{}))
}

func TestDefaultOptionsIncludeKeys(t *testing.T) {
	require.True(t, DefaultJSONTermOptions().IncludeKeys)
}

// Terms are raw packed bytes, not text: they legitimately contain 0x00 and
// BOOLEAN-mode syntax bytes. This is the evidence that they must never be
// routed through a pattern parser or a SQL literal.
func TestTermsAreBinaryNotText(t *testing.T) {
	term := JSONStringTerm("b", "XXX")
	require.Contains(t, term, "\x00", "packed terms carry NUL; the path must be binary-clean")

	// a value made of pattern metacharacters round-trips unharmed
	meta := `+a -b *c "d" (e)`
	doc, err := json.Marshal(map[string]string{"b": meta})
	require.NoError(t, err)
	docTerm := termsOf(t, string(doc), JSONTermOptions{IncludeKeys: true})[0]
	require.Equal(t, JSONStringTerm("b", meta), docTerm)
}

// Arrays: every element is reachable under the enclosing key.
func TestArrayElementsProduceTermsUnderTheKey(t *testing.T) {
	terms := termsOf(t, `{"a":[1,2]}`, JSONTermOptions{IncludeKeys: true})
	require.Len(t, terms, 2)
	require.Contains(t, terms, JSONFloatTerm("a", 1))
	require.Contains(t, terms, JSONFloatTerm("a", 2))
}

// --- CREATE / ISCP parity --------------------------------------------------

// The two build paths must emit byte-identical (word, pos) pairs for the same
// document. A mismatch here is the exact failure mode recorded at query.go:264:
// rows inserted after CREATE INDEX become silently unsearchable.
func TestCreateAndIscpAgreeOnTerms(t *testing.T) {
	const doc = `{"a":{"b":"XXX","n":3},"z":[1,"q"]}`

	for _, opt := range []JSONTermOptions{
		{IncludeKeys: true},
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

	opt := JSONTermOptions{IncludeKeys: true}
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
	vals := []float64{-10, -1, 0, 1, 3.14, 100}
	terms := make([]string, 0, len(vals))
	for _, v := range vals {
		terms = append(terms, JSONFloatTerm("n", v))
	}
	sort.Strings(terms)
	seg := &Segment{sortedTerms: terms}

	// n >= 1  ->  [term(1), +Inf]
	_, hi := JSONNumericTermBounds("n")
	got := seg.TermRange(JSONFloatTerm("n", 1), hi)
	require.Equal(t, 3, len(got), "1, 3.14 and 100 qualify")

	// n < 0  ->  [-Inf, term(0)]  (inclusive, so 0 comes back too and the
	// retained predicate drops it)
	lo, _ := JSONNumericTermBounds("n")
	got = seg.TermRange(lo, JSONFloatTerm("n", 0))
	require.Equal(t, 3, len(got), "-10, -1 and the boundary 0")
}

// --- probe search over a real index -----------------------------------------

// jsonProbeIndex builds a one-segment index whose terms are the tuple terms of
// the given documents, so a probe can be resolved end to end in-process.
func jsonProbeIndex(t *testing.T, docs map[int64]string) *Index {
	t.Helper()
	opt := JSONTermOptions{IncludeKeys: true}
	pks := make([]int64, 0, len(docs))
	for pk := range docs {
		pks = append(pks, pk)
	}
	sort.Slice(pks, func(i, j int) bool { return pks[i] < pks[j] })

	tdocs := make([]TokenizedDoc, 0, len(docs))
	for _, pk := range pks {
		bj, err := bytejson.ParseFromString(docs[pk])
		require.NoError(t, err)
		terms := JSONTupleTerms(bj, opt)
		pos := make([]int32, len(terms))
		for i := range terms {
			pos[i] = int32(i)
		}
		tdocs = append(tdocs, TokenizedDoc{Pk: pk, Terms: terms, Positions: pos})
	}
	seg, err := BuildSegmentFromTokenized("jp", int32(types.T_int64), tdocs)
	require.NoError(t, err)
	return NewIndex([]*Segment{seg}, nil)
}

func probeHits(t *testing.T, idx *Index, terms []string, ranges [][2]string) []int64 {
	t.Helper()
	res, err := idx.SearchJSONProbe([]byte(EncodeJSONProbePayload(terms, ranges)), TfIdf, 100, nil)
	require.NoError(t, err)
	out := make([]int64, 0, len(res))
	for _, r := range res {
		out = append(out, r.Pk.(int64))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestSearchJSONProbeExactTerms(t *testing.T) {
	idx := jsonProbeIndex(t, map[int64]string{
		1: `{"foo":"bar","n":10}`,
		2: `{"foo":"baz","n":20}`,
		3: `{"other":"bar","n":30}`,
	})
	// the key is part of the term: doc 3 has "bar" under a DIFFERENT key
	require.Equal(t, []int64{1},
		probeHits(t, idx, []string{JSONStringTerm("foo", "bar")}, nil))
	// a term no document holds
	require.Empty(t, probeHits(t, idx, []string{JSONStringTerm("foo", "nope")}, nil))
	// two terms are ORed
	require.Equal(t, []int64{1, 2}, probeHits(t, idx, []string{
		JSONStringTerm("foo", "bar"),
		JSONStringTerm("foo", "baz"),
	}, nil))
}

// The range path: this is what silently returned nothing until TermRange learned
// to read the loaded FST as well as the build-side key list.
func TestSearchJSONProbeRanges(t *testing.T) {
	idx := jsonProbeIndex(t, map[int64]string{
		1: `{"n":10}`, 2: `{"n":20}`, 3: `{"n":30}`, 4: `{"n":-5}`,
	})
	_, hi := JSONNumericTermBounds("n")
	lo, _ := JSONNumericTermBounds("n")

	// n >= 20
	require.Equal(t, []int64{2, 3},
		probeHits(t, idx, nil, [][2]string{{JSONFloatTerm("n", 20), hi}}))
	// n <= 10  (includes the negative doc)
	require.Equal(t, []int64{1, 4},
		probeHits(t, idx, nil, [][2]string{{lo, JSONFloatTerm("n", 10)}}))
	// full numeric sweep reaches every doc
	require.Equal(t, []int64{1, 2, 3, 4}, probeHits(t, idx, nil, [][2]string{{lo, hi}}))
	// an empty range selects nothing
	require.Empty(t, probeHits(t, idx, nil, [][2]string{
		{JSONFloatTerm("n", 100), JSONFloatTerm("n", 200)}}))
}

// terms and ranges are a UNION, and duplicates across them collapse.
func TestSearchJSONProbeUnionsTermsAndRanges(t *testing.T) {
	idx := jsonProbeIndex(t, map[int64]string{
		1: `{"foo":"bar","n":10}`, 2: `{"n":99}`,
	})
	_, hi := JSONNumericTermBounds("n")
	require.Equal(t, []int64{1, 2}, probeHits(t,
		idx,
		[]string{JSONStringTerm("foo", "bar")},
		[][2]string{{JSONFloatTerm("n", 50), hi}}))
}

func TestSearchJSONProbeRejectsMalformedPayload(t *testing.T) {
	idx := jsonProbeIndex(t, map[int64]string{1: `{"foo":"bar"}`})
	_, err := idx.SearchJSONProbe([]byte("garbage"), TfIdf, 10, nil)
	require.Error(t, err, "a malformed payload must fail loudly, not match nothing")
}

// jsonProbeLoadedIndex is jsonProbeIndex round-tripped through Serialize /
// Deserialize, i.e. the LOADED representation a persisted index actually has.
// This is the path that matters: sortedTerms is build-side only, so a range
// resolved against a loaded segment must go through the FST term dictionary.
func jsonProbeLoadedIndex(t *testing.T, docs map[int64]string) *Index {
	t.Helper()
	built := jsonProbeIndex(t, docs)
	blob, err := built.segments[0].Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("jp", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	return NewIndex([]*Segment{loaded}, nil)
}

// A range over a LOADED segment must find the same terms as over a build-side
// one. It did not: TermRange consulted only the build-side key list, so every
// range silently returned zero rows against a persisted index.
func TestSearchJSONProbeRangesOnLoadedSegment(t *testing.T) {
	docs := map[int64]string{1: `{"n":10}`, 2: `{"n":20}`, 3: `{"n":30}`, 4: `{"n":-5}`}
	lo, hi := JSONNumericTermBounds("n")
	ge20 := [][2]string{{JSONFloatTerm("n", 20), hi}}

	require.Equal(t, []int64{2, 3}, probeHits(t, jsonProbeLoadedIndex(t, docs), nil, ge20),
		"a loaded segment must resolve ranges through its FST")
	// identical to the build-side answer
	require.Equal(t, probeHits(t, jsonProbeIndex(t, docs), nil, ge20),
		probeHits(t, jsonProbeLoadedIndex(t, docs), nil, ge20))

	// exact terms work on the loaded path too, and the full sweep reaches all
	require.Equal(t, []int64{1, 2, 3, 4},
		probeHits(t, jsonProbeLoadedIndex(t, docs), nil, [][2]string{{lo, hi}}))
	require.Equal(t, []int64{3}, probeHits(t, jsonProbeLoadedIndex(t, docs),
		[]string{JSONFloatTerm("n", 30)}, nil))
}

// The streaming path (no pushed LIMIT) must return the same docs as the top-k
// one — it is a different walk over the same term disjunction.
func TestStreamJSONProbeMatchesSearch(t *testing.T) {
	mp := mpool.MustNewZero()
	idx := jsonProbeIndex(t, map[int64]string{
		1: `{"foo":"bar","n":10}`, 2: `{"foo":"baz","n":20}`, 3: `{"foo":"bar","n":30}`,
	})
	term := JSONStringTerm("foo", "bar")
	payload := []byte(EncodeJSONProbePayload([]string{term}, nil))

	out := vector.NewVec(types.T_int64.ToType())
	err := idx.StreamJSONProbe(payload, TfIdf, nil, false,
		func(o *vectorindex.SearchOutput) error {
			e := vectorindex.AppendColumnBuffer(o.Keys, out, mp)
			PutColumnBuffer(o.Keys)
			return e
		})
	require.NoError(t, err)
	streamed := vector.MustFixedColWithTypeCheck[int64](out)
	require.ElementsMatch(t, []int64{1, 3}, streamed)
	require.ElementsMatch(t, probeHits(t, idx, []string{term}, nil), streamed)

	// a malformed payload fails here too, rather than streaming nothing
	require.Error(t, idx.StreamJSONProbe([]byte("garbage"), TfIdf, nil, false, nil))
}

// The string bounds must bracket every string leaf under the tag and exclude
// the numeric encoding, so a string range never leaks into numeric terms.
func TestJSONStringTermBounds(t *testing.T) {
	lo, hi := JSONStringTermBounds("k")
	require.Less(t, lo, hi)
	for _, v := range []string{"", "a", "zzz", "\xff"} {
		term := JSONStringTerm("k", v)
		require.GreaterOrEqual(t, term, lo, "%q below the low bound", v)
		require.LessOrEqual(t, term, hi, "%q above the high bound", v)
	}
	// a numeric term for the same tag sits outside the string range
	num := JSONFloatTerm("k", 1)
	require.True(t, num < lo || num > hi, "numeric term must not fall inside the string range")
}

// JSONTupleColumn accepts the three shapes an indexed column arrives in.
func TestJSONTupleColumnAcceptsEveryShape(t *testing.T) {
	opt := JSONTermOptions{IncludeKeys: true}
	want := []string{JSONStringTerm("b", "x")}

	fromStr, err := JSONTupleColumn(`{"b":"x"}`, opt)
	require.NoError(t, err)
	require.Equal(t, want, fromStr)

	fromBytes, err := JSONTupleColumn([]byte(`{"b":"x"}`), opt)
	require.NoError(t, err)
	require.Equal(t, want, fromBytes)

	bj, err := bytejson.ParseFromString(`{"b":"x"}`)
	require.NoError(t, err)
	fromBJ, err := JSONTupleColumn(bj, opt)
	require.NoError(t, err)
	require.Equal(t, want, fromBJ)

	// an unknown shape yields nothing rather than an error
	none, err := JSONTupleColumn(42, opt)
	require.NoError(t, err)
	require.Nil(t, none)

	// malformed json is reported
	_, err = JSONTupleColumn(`{not json`, opt)
	require.Error(t, err)
}

// streamProbeHits runs the STREAMING probe path and returns the pks it emitted,
// in emission order and WITHOUT sorting or de-duplicating — so a caller can
// assert that the walk itself yields each document exactly once.
func streamProbeHits(t *testing.T, idx *Index, terms []string, ranges [][2]string) []int64 {
	t.Helper()
	mp := mpool.MustNewZero()
	out := vector.NewVec(types.T_int64.ToType())
	err := idx.StreamJSONProbe([]byte(EncodeJSONProbePayload(terms, ranges)), TfIdf, nil, false,
		func(o *vectorindex.SearchOutput) error {
			e := vectorindex.AppendColumnBuffer(o.Keys, out, mp)
			PutColumnBuffer(o.Keys)
			return e
		})
	require.NoError(t, err)
	return append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](out)...)
}

// The streaming walk visits one term at a time instead of merging them, so a
// document holding SEVERAL terms of a range is emitted once per term. That is
// the index's contract, not a defect: de-duplication is the GROUP BY the planner
// puts over this scan (QueryBuilder.dedupFulltextDocIDs), so the index does not
// carry a second copy of it. What must hold here is that the DISTINCT set is
// right and nothing is lost.
func TestStreamJSONProbeRangeEmitsEveryMatchingTerm(t *testing.T) {
	// every doc has three leaves under "n", so a full-range sweep reaches each
	// document three times
	docs := map[int64]string{
		1: `{"n":[1,2,3]}`,
		2: `{"n":[4,5,6]}`,
		3: `{"n":[7,8,9]}`,
	}
	lo, hi := JSONNumericTermBounds("n")
	full := [][2]string{{lo, hi}}

	for _, tc := range []struct {
		name string
		idx  *Index
	}{
		{"build-side", jsonProbeIndex(t, docs)},
		{"loaded/FST", jsonProbeLoadedIndex(t, docs)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := streamProbeHits(t, tc.idx, nil, full)
			require.ElementsMatch(t, []int64{1, 2, 3}, distinctInt64(got),
				"the distinct doc set is what the probe asserts")
			require.Len(t, got, 9, "one emission per matching term, deduped by the planner's GROUP BY")
		})
	}
}

// distinctInt64 is what the planner's GROUP BY does, applied here so a test can
// state the probe's real contract: the DISTINCT set is correct.
func distinctInt64(in []int64) []int64 {
	seen := make(map[int64]bool, len(in))
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// An exact term that also falls inside a range is walked twice, for the same
// reason. The distinct set is still exactly right.
func TestStreamJSONProbeOverlappingTermsAndRanges(t *testing.T) {
	docs := map[int64]string{1: `{"n":10}`, 2: `{"n":20}`, 3: `{"n":30}`}
	lo, hi := JSONNumericTermBounds("n")

	got := streamProbeHits(t, jsonProbeIndex(t, docs),
		[]string{JSONFloatTerm("n", 20)}, [][2]string{{lo, hi}})
	require.ElementsMatch(t, []int64{1, 2, 3}, distinctInt64(got))
	require.Len(t, got, 4, "doc 2 is reached by both the exact term and the range")
}

// The streaming range answer must equal the materializing top-k answer on both
// segment representations — they are two different walks over the same range.
func TestStreamJSONProbeRangeMatchesSearch(t *testing.T) {
	docs := map[int64]string{
		1: `{"n":10}`, 2: `{"n":20}`, 3: `{"n":30}`, 4: `{"n":-5}`, 5: `{"foo":"bar"}`,
	}
	_, hi := JSONNumericTermBounds("n")
	ge20 := [][2]string{{JSONFloatTerm("n", 20), hi}}

	for _, tc := range []struct {
		name string
		make func() *Index
	}{
		{"build-side", func() *Index { return jsonProbeIndex(t, docs) }},
		{"loaded/FST", func() *Index { return jsonProbeLoadedIndex(t, docs) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streamed := streamProbeHits(t, tc.make(), nil, ge20)
			require.ElementsMatch(t, []int64{2, 3}, streamed)
			require.ElementsMatch(t, probeHits(t, tc.make(), nil, ge20), streamed)
		})
	}
}

// A range that selects nothing must stream nothing rather than everything — the
// failure mode that would turn a prefilter into a no-op.
func TestStreamJSONProbeEmptyRange(t *testing.T) {
	docs := map[int64]string{1: `{"n":10}`, 2: `{"n":20}`}
	above := [][2]string{{JSONFloatTerm("n", 1000), JSONFloatTerm("n", 2000)}}
	require.Empty(t, streamProbeHits(t, jsonProbeIndex(t, docs), nil, above))
	require.Empty(t, streamProbeHits(t, jsonProbeLoadedIndex(t, docs), nil, above))

	// an inverted range (lo > hi) selects nothing, and must not error
	inverted := [][2]string{{JSONFloatTerm("n", 100), JSONFloatTerm("n", 1)}}
	require.Empty(t, streamProbeHits(t, jsonProbeIndex(t, docs), nil, inverted))
	require.Empty(t, streamProbeHits(t, jsonProbeLoadedIndex(t, docs), nil, inverted))
}

// A string range must not sweep up numeric leaves under the same key, and the
// reverse — the disjointness the encoding guarantees, asserted through a real
// index rather than on the terms alone.
func TestStreamJSONProbeRangeRespectsLeafType(t *testing.T) {
	docs := map[int64]string{
		1: `{"v":"apple"}`, 2: `{"v":"pear"}`, 3: `{"v":10}`, 4: `{"v":20}`,
	}
	slo, shi := JSONStringTermBounds("v")
	nlo, nhi := JSONNumericTermBounds("v")

	require.ElementsMatch(t, []int64{1, 2},
		streamProbeHits(t, jsonProbeLoadedIndex(t, docs), nil, [][2]string{{slo, shi}}))
	require.ElementsMatch(t, []int64{3, 4},
		streamProbeHits(t, jsonProbeLoadedIndex(t, docs), nil, [][2]string{{nlo, nhi}}))
}
