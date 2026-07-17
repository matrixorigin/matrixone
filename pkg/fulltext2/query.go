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
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BooleanTreeString parses a boolean-mode query and returns its parse tree as an
// S-expression (e.g. `+Matrix -Origin` → `(+ (text 0 matrix)) (- (text 1 origin))`).
// A debug aid: it reuses classic fulltext's PatternToString so the printed shape
// is identical to pkg/fulltext (fulltext2's boolean structure is parity with it).
func BooleanTreeString(query string) (string, error) {
	return fulltext.PatternToString(query, int64(tree.FULLTEXT_BOOLEAN))
}

// BooleanTreeStringWithPosition is BooleanTreeString with each leaf's byte
// position included: `(text <index> <bytepos> <word>)` — mirrors classic
// fulltext's PatternToStringWithPosition (used for phrase-position debugging).
func BooleanTreeStringWithPosition(query string) (string, error) {
	return fulltext.PatternToStringWithPosition(query, int64(tree.FULLTEXT_BOOLEAN))
}

// NLTreeString parses a natural-language query and returns its parse tree (the
// ngram/word patterns AGAINST(...) expands to), reusing classic fulltext's
// PatternToString. Debug aid for the NL side.
func NLTreeString(query string) (string, error) {
	return fulltext.PatternToString(query, int64(tree.FULLTEXT_NL))
}

// NLTreeStringWithPosition is NLTreeString with each leaf's byte position:
// `(text <index> <bytepos> <word>)` — mirrors PatternToStringWithPosition.
func NLTreeStringWithPosition(query string) (string, error) {
	return fulltext.PatternToStringWithPosition(query, int64(tree.FULLTEXT_NL))
}

// Parser selects how documents and queries are tokenized. fulltext2 mirrors
// classic fulltext's parser set:
//   - default / ngram : SimpleTokenizer — whole words for Latin, sliding
//     3-grams for CJK (the classic build-side ngram tokenizer).
//   - gojieba         : dictionary word segmentation (CJK + Latin) via jieba.
//   - json / json_value: json values are flattened to text, then tokenized as
//     ngram; the query pattern is plain text (also ngram).
const (
	ParserDefault   = "default"
	ParserNgram     = "ngram"
	ParserGojieba   = "gojieba"
	ParserJSON      = "json"
	ParserJSONValue = "json_value"

	ngramSize = 3
)

// normalizeParser lowercases/trims and maps "" to the default parser.
func normalizeParser(p string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	if p == "" {
		return ParserDefault
	}
	return p
}

// hasCJK reports whether s contains a CJK-class rune — a non-Latin, non-breaker
// rune, matching SimpleTokenizer's own CJK classification (isLatin: rune<0x7FF).
// A bare CJK boolean operand is routed to the positional (byte-offset) phrase path
// so a short compound (e.g. 中文, a 2-char bigram absent as a whole token from an
// ngram-3 index) matches via a word* prefix rather than an empty exact-term lookup.
func hasCJK(s string) bool {
	for _, r := range s {
		if r > 0x7FF && !unicode.IsPunct(r) && !unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// DocTokenizer returns the tokenizer that indexes DOCUMENTS under parser.
// json/json_value documents must be flattened first (FlattenJSON); their
// flattened text is tokenized as ngram.
func DocTokenizer(parser string) (tokenizer.Tokenizer, error) {
	switch normalizeParser(parser) {
	case ParserDefault, ParserNgram, ParserJSON, ParserJSONValue:
		return tokenizer.NewSimpleTokenizer(), nil
	case ParserGojieba:
		return tokenizer.SharedJiebaTokenizer(false)
	default:
		return nil, moerr.NewInternalErrorNoCtx("fulltext2: invalid parser " + parser)
	}
}

// queryTokenizer returns the tokenizer for a QUERY pattern under parser. It is
// SimpleTokenizer for every parser except gojieba — a json index is queried with
// plain text, so json uses the ngram tokenizer at query time.
func queryTokenizer(parser string) (tokenizer.Tokenizer, error) {
	if normalizeParser(parser) == ParserGojieba {
		return tokenizer.SharedJiebaTokenizer(false)
	}
	return tokenizer.NewSimpleTokenizer(), nil
}

// flattenJSONValues parses raw as a TEXT JSON document and returns its leaf
// values joined by spaces. Convenience for text/varchar json columns and tests.
func flattenJSONValues(raw []byte) ([]byte, error) {
	return FlattenJSON(raw, false)
}

// FlattenJSON returns a JSON document's leaf values joined by spaces, so an ngram
// tokenizer can index them as ordinary text. binary selects the encoding: true
// for a T_json column's stored bytes (bytejson.Unmarshal), false for text/varchar
// json (bytejson.ParseFromString).
func FlattenJSON(raw []byte, binary bool) ([]byte, error) {
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
	return flattenBJ(bj), nil
}

// flattenBJ joins a parsed json document's leaf values with spaces (shared by
// FlattenJSON and FlattenJSONColumn).
func flattenBJ(bj bytejson.ByteJson) []byte {
	var b strings.Builder
	for tk := range bj.TokenizeValue(false) {
		n := int(tk.TokenBytes[0])
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.Write(tk.TokenBytes[1 : 1+n])
	}
	return []byte(b.String())
}

// FlattenJSONColumn flattens ONE indexed column's value to its space-joined leaf
// values for a json parser — the CDC-write-side analogue of the create-TVF's
// per-column flatten (rowTerms). A T_json column arrives as an already-parsed
// bytejson.ByteJson (types.DecodeJson, repr-independent); a text/varchar json column
// arrives as a string/[]byte of json TEXT. Doing this per column (vs joining raw
// columns then flattening the blob once) is what makes CDC json tokens match CREATE:
// the old joined-then-flattened path yielded ZERO tokens for T_json and for
// multi-column json, silently making CDC-inserted rows unsearchable.
func FlattenJSONColumn(v any) ([]byte, error) {
	switch t := v.(type) {
	case bytejson.ByteJson:
		return flattenBJ(t), nil
	case []byte:
		return FlattenJSON(t, false)
	case string:
		return FlattenJSON([]byte(t), false)
	default:
		return nil, nil
	}
}

// IsJSONParser reports whether parser is one of the json variants.
func IsJSONParser(parser string) bool {
	p := normalizeParser(parser)
	return p == ParserJSON || p == ParserJSONValue
}

// IsJSONValueParser reports whether parser is the json_value variant. json_value
// indexes each json leaf value as ONE whole atomic token and matches it EXACTLY
// (classic fulltext: tokenize.go stores the whole value, sql.go:173 queries
// `word = value`) — distinct from json, which ngrams over the flattened values.
func IsJSONValueParser(parser string) bool {
	return normalizeParser(parser) == ParserJSONValue
}

// flattenBJLines joins a parsed json document's leaf values with NEWLINES (not
// spaces): the json_value parser keeps each whole value as a separable atomic token,
// so a '\n' separator lets JSONValueTokenize recover the values (a space join would
// fuse values and lose the boundaries — and values may themselves contain spaces).
func flattenBJLines(bj bytejson.ByteJson) []byte {
	var b strings.Builder
	for tk := range bj.TokenizeValue(false) {
		n := int(tk.TokenBytes[0])
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.Write(tk.TokenBytes[1 : 1+n])
	}
	return []byte(b.String())
}

// FlattenJSONLines is FlattenJSON for the json_value parser: leaf values joined by
// '\n' so each stays a separable whole token. binary selects the encoding as in
// FlattenJSON.
func FlattenJSONLines(raw []byte, binary bool) ([]byte, error) {
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
	return flattenBJLines(bj), nil
}

// FlattenJSONValueColumn is FlattenJSONColumn for the json_value parser: ONE indexed
// column's leaf values joined by '\n' (the CDC-write-side analogue of the create-TVF's
// per-column flatten). Mirrors FlattenJSONColumn's input-type handling.
func FlattenJSONValueColumn(v any) ([]byte, error) {
	switch t := v.(type) {
	case bytejson.ByteJson:
		return flattenBJLines(t), nil
	case []byte:
		return FlattenJSONLines(t, false)
	case string:
		return FlattenJSONLines([]byte(t), false)
	default:
		return nil, nil
	}
}

// JSONValueTokenize splits '\n'-joined json values into whole (word, byte-position)
// tokens — the json_value parser's atomic-value tokenization: no ngram, no
// case-folding, punctuation kept, so `red..blue--brown` and `中文學習教材` each index
// as ONE token that a query matches exactly (classic fulltext json_value parity).
func JSONValueTokenize(text []byte) []WordPos {
	var words []WordPos
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && text[j] != '\n' {
			j++
		}
		if j > i {
			words = append(words, WordPos{Word: string(text[i:j]), Pos: int32(i)})
		}
		i = j + 1
	}
	return words
}

// CdcTokenizer returns the parser-aware tokenize closure the CDC consumer feeds to
// TailBuilder — ordered words per parser. For a json parser the CDC WRITER
// (Fulltext2SqlWriter.rowText) already flattens each json column to its values,
// binary-aware and PER COLUMN, exactly as the create-TVF's rowTerms does — so the CDC
// text is plain flattened text here and is tokenized as ngram, NOT flattened again.
// (The old form flattened the whole joined multi-column blob once with binary=false,
// which produced zero tokens for a T_json column and mis-parsed multi-column json — so
// CDC-inserted json rows were unsearchable.)
func CdcTokenizer(parser string) (func(string) []WordPos, error) {
	if IsJSONValueParser(parser) {
		// json_value: the CDC writer (rowText) already flattened each json column to its
		// '\n'-joined whole values, so here we just recover them as atomic tokens — NOT
		// ngram — matching the create/base build.
		return func(text string) []WordPos { return JSONValueTokenize([]byte(text)) }, nil
	}
	tok, err := DocTokenizer(parser)
	if err != nil {
		return nil, err
	}
	return func(text string) []WordPos { return tokenizeWords(tok, []byte(text)) }, nil
}

// WordPos is a tokenized word with its BYTE position in the source text — carried
// through the CDC/merge build so positional phrase queries match by byte offset.
type WordPos struct {
	Word string
	Pos  int32
}

// tokenizeWords flattens a tokenizer stream into ordered (word, byte-position) pairs.
func tokenizeWords(tok tokenizer.Tokenizer, text []byte) []WordPos {
	var words []WordPos
	for tk, err := range tok.Tokenize(text) {
		if err != nil {
			break
		}
		words = append(words, WordPos{tokenWord(tk), tk.BytePos})
	}
	return words
}

// BuildSegmentFromDocsParser builds a segment from docs under parser, selecting
// the right document tokenizer and flattening json docs first. It is the
// parser-aware build entry (compile-side build-from-source uses it).
func BuildSegmentFromDocsParser(id string, pkType int32, docs []Doc, parser string) (*Segment, error) {
	p := normalizeParser(parser)
	if p == ParserJSONValue {
		// json_value: each leaf value is one whole atomic token (no ngram). Build the
		// tokenized docs directly from the '\n'-joined values so build matches query.
		tds := make([]TokenizedDoc, 0, len(docs))
		for _, d := range docs {
			lines, ferr := FlattenJSONLines(d.Text, false)
			if ferr != nil {
				return nil, ferr
			}
			td := TokenizedDoc{Pk: d.Pk}
			for _, wp := range JSONValueTokenize(lines) {
				td.Terms = append(td.Terms, wp.Word)
				td.Positions = append(td.Positions, wp.Pos)
			}
			tds = append(tds, td)
		}
		return BuildSegmentFromTokenized(id, pkType, tds)
	}
	tok, err := DocTokenizer(parser)
	if err != nil {
		return nil, err
	}
	if p == ParserJSON {
		fdocs := make([]Doc, 0, len(docs))
		for _, d := range docs {
			ft, ferr := flattenJSONValues(d.Text)
			if ferr != nil {
				return nil, ferr
			}
			fdocs = append(fdocs, Doc{Pk: d.Pk, Text: ft})
		}
		docs = fdocs
	}
	return BuildSegmentFromDocs(id, pkType, docs, tok)
}

// SearchQuery runs a MATCH query over the index under parser and mode. It is the
// single query entry the fulltext2 search TVF calls.
//
// NL mode (boolean=false): an EXACT ORDERED PHRASE for EVERY parser — the terms
// must occur contiguously and in order (classic fulltext NL semantics). There is
// NO bag-of-words path: a CJK query is matched positionally by BYTE offset over
// classic fulltext's redundant-ngram-removed slots (full trigrams as TEXT, short
// tails as word* prefixes), so the phrase is robust despite overlapping/tail 3-grams.
//
// Boolean mode (boolean=true): standard +/-/>/</~ operators; each operand is an
// exact positional phrase (a bare CJK operand included) — never an OR-group of its
// ngrams. Distinct SHOULD operands still OR together (boolean semantics), and a
// word* prefix / ( ) group behave as their operators dictate.
func (idx *Index) SearchQuery(pattern []byte, boolean bool, parser string, algo ScoreAlgo, k int, filter docfilter.MembershipFilter) ([]Result, error) {
	if k <= 0 || idx.globalN == 0 {
		return nil, nil
	}
	pat := string(pattern)
	p := normalizeParser(parser)

	if boolean {
		q, err := buildBooleanQuery(pat, p)
		if err != nil {
			return nil, err
		}
		return idx.SearchBoolean(q, algo, k, filter)
	}

	// NL mode is an exact ORDERED PHRASE for every parser — the deduped, byte-positioned
	// slots matched positionally, matching classic fulltext's NL semantics (incl. CJK).
	slots, err := phraseSlots(pat, p)
	if err != nil {
		return nil, err
	}
	return idx.SearchPhrase(slots, algo, k, filter), nil
}

// phraseSlots turns an NL pattern into the positional phrase slots to match. For
// gojieba it tokenizes into words with byte positions. For ngram/default/json it uses
// ngramPhraseSlots: EXACT full trigrams covering each CJK run (no word* prefix for runs
// >= 3 chars), so matching is exact-term lookups + byte-position adjacency instead of the
// old ParsePatternInNLMode prefix decomposition, whose word* expansion + full-position
// materialization OOM-killed the CN at scale. Offsets are relative to the first slot.
func phraseSlots(pattern, parser string) ([]phraseSlot, error) {
	if IsJSONValueParser(parser) {
		// json_value: the whole pattern is ONE atomic value matched exactly (no ngram) —
		// a single exact-term slot, mirroring classic `word = value`.
		if pattern == "" {
			return nil, nil
		}
		return []phraseSlot{{term: pattern, off: 0}}, nil
	}
	if normalizeParser(parser) == ParserGojieba {
		tok, err := queryTokenizer(ParserGojieba)
		if err != nil {
			return nil, err
		}
		return tokenizePhraseSlots(tok, []byte(pattern)), nil
	}
	return ngramPhraseSlots(pattern), nil
}

// isBreakerRune / isLatinRune mirror SimpleTokenizer's run classification (simple.go) so
// the query decomposition segments runs exactly as the document tokenizer does.
func isBreakerRune(r rune) bool {
	if r < 128 {
		return !((r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'))
	}
	return unicode.IsPunct(r) || unicode.IsSpace(r)
}

func isLatinRune(r rune) bool { return r < 0x7FF }

// ngramPhraseSlots decomposes an ngram-parser query into positional phrase slots that
// match the SimpleTokenizer document index WITHOUT word* prefixes for CJK runs >= 3
// chars. A CJK run is covered by EXACT full trigrams at char step ngramSize PLUS a final
// trigram that ends at the run's end (the last trigram is shifted back to cover the tail),
// so byte-position adjacency alone pins the whole run — the overlapping/adjacent trigrams
// uniquely determine the substring. Latin runs become whole (lowercased) word slots. Only
// a CJK run SHORTER than ngramSize (1-2 chars, e.g. a bare 2-char term) has no trigram and
// falls back to a single prefix (star) slot, handled presence-only at match time.
func ngramPhraseSlots(pattern string) []phraseSlot {
	runes := []rune(pattern)
	n := len(runes)
	if n == 0 {
		return nil
	}
	bpos := make([]int32, n+1) // byte offset of each rune index
	b := int32(0)
	for i, r := range runes {
		bpos[i] = b
		b += int32(utf8.RuneLen(r))
	}
	bpos[n] = b

	var slots []phraseSlot
	i := 0
	for i < n {
		if isBreakerRune(runes[i]) {
			i++
			continue
		}
		if isLatinRune(runes[i]) {
			j := i
			for j < n && !isBreakerRune(runes[j]) && isLatinRune(runes[j]) {
				j++
			}
			slots = append(slots, phraseSlot{term: strings.ToLower(string(runes[i:j])), off: bpos[i]})
			i = j
			continue
		}
		// CJK run [i, j)
		j := i
		for j < n && !isBreakerRune(runes[j]) && !isLatinRune(runes[j]) {
			j++
		}
		if j-i < ngramSize {
			// 1-2 char CJK run: no trigram exists → prefix (rare; presence-only in matchPhrase)
			slots = append(slots, phraseSlot{term: string(runes[i:j]), star: true, off: bpos[i]})
			i = j
			continue
		}
		last := j - ngramSize // start index of the trigram ending at the run's end
		for c := i; c <= last; c += ngramSize {
			slots = append(slots, phraseSlot{term: string(runes[c : c+ngramSize]), off: bpos[c]})
		}
		if (last-i)%ngramSize != 0 { // tail not covered by the step-ngramSize loop → add the final trigram
			slots = append(slots, phraseSlot{term: string(runes[last : last+ngramSize]), off: bpos[last]})
		}
		i = j
	}
	if len(slots) == 0 {
		return nil
	}
	base := slots[0].off
	for k := range slots {
		slots[k].off -= base
	}
	return slots
}

// slotsKey is a deterministic string key for a phrase (term+star+offset per slot), used
// to memoize phrase hits/df per query.
func slotsKey(slots []phraseSlot) string {
	var b strings.Builder
	for _, s := range slots {
		b.WriteString(s.term)
		if s.star {
			b.WriteByte('*')
		}
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(int(s.off)))
		b.WriteByte(0)
	}
	return b.String()
}

// tokenizePhraseSlots tokenizes text into (word, byte-position) TEXT slots — for gojieba
// NL and boolean-mode explicit "phrases".
func tokenizePhraseSlots(tok tokenizer.Tokenizer, text []byte) []phraseSlot {
	wps := tokenizeWords(tok, text)
	if len(wps) == 0 {
		return nil
	}
	base := wps[0].Pos
	slots := make([]phraseSlot, len(wps))
	for i, wp := range wps {
		slots[i] = phraseSlot{term: wp.Word, off: wp.Pos - base}
	}
	return slots
}

// buildBooleanQuery parses a boolean-mode query parser-aware: gojieba/Latin
// operands tokenize into word phrases/terms; a bare CJK operand under ngram/json
// is an exact positional (byte-offset) phrase, matching it contiguously — NOT an
// OR-group of its ngrams (which would over-match any doc sharing a single ngram).
func buildBooleanQuery(query, parser string) (BoolQuery, error) {
	var q BoolQuery
	for _, rc := range scanBoolean(query) {
		c, ok, err := rawToClauseParser(rc, parser)
		if err != nil {
			return q, err
		}
		if !ok {
			continue
		}
		switch bucketOf(rc.prefix) {
		case bMust:
			q.must = append(q.must, c)
		case bMustNot:
			q.mustNot = append(q.mustNot, c)
		case bAdjust:
			q.adjust = append(q.adjust, c)
		default:
			q.should = append(q.should, c)
		}
	}
	return q, nil
}

// rawToClauseParser is the parser-aware form of rawToClause (boolean.go): a bare
// CJK operand under ngram/json becomes an exact positional phrase (byte-offset
// slots); quoted phrases, prefixes, groups and gojieba/Latin operands behave as
// in rawToClause.
func rawToClauseParser(rc rawClause, parser string) (clause, bool, error) {
	w := weightOf(rc.prefix)
	if rc.group {
		var children []clause
		for _, ch := range rc.children {
			cc, ok, err := rawToClauseParser(ch, parser)
			if err != nil {
				return clause{}, false, err
			}
			if ok {
				children = append(children, cc)
			}
		}
		if len(children) == 0 {
			return clause{}, false, nil
		}
		return clause{kind: clauseGroup, children: children, weight: w}, true, nil
	}

	// json_value: the whole operand is ONE atomic value matched exactly (no ngram, no
	// tokenization) — classic fulltext queries `word = operand` (sql.go:173). A quoted
	// operand ("update_json") is the same whole-value match. A trailing * is a value
	// prefix.
	if IsJSONValueParser(parser) {
		if rc.text == "" {
			return clause{}, false, nil
		}
		if rc.star {
			return clause{kind: clausePrefix, terms: []string{rc.text}, weight: w}, true, nil
		}
		return clause{kind: clauseTerm, terms: []string{rc.text}, weight: w}, true, nil
	}

	// A bare (non-quoted, non-prefix) CJK operand under ngram/json is an EXACT positional
	// phrase (byte-offset slots), the same as NL and as classic fulltext — which matches
	// the operand contiguously. (It previously expanded to an OR-group of its n-grams,
	// which over-matched any doc sharing a single n-gram, e.g. `+中文學習` matched 中文學校.
	// That was a workaround for the old token-index phrase matcher; byte positions make
	// the phrase robust, so it is no longer needed.)
	if parser != ParserGojieba && !rc.quoted && !rc.star && hasCJK(rc.text) {
		slots, err := phraseSlots(rc.text, parser)
		if err != nil {
			return clause{}, false, err
		}
		if len(slots) == 0 {
			return clause{}, false, nil
		}
		return clause{kind: clausePhrase, phrase: slots, weight: w}, true, nil
	}

	tok, err := queryTokenizer(parser)
	if err != nil {
		return clause{}, false, err
	}
	terms, err := tokenizeToTerms([]byte(rc.text), tok)
	if err != nil {
		return clause{}, false, err
	}
	if len(terms) == 0 {
		return clause{}, false, nil
	}
	switch {
	case rc.quoted:
		// Explicit "phrase": positional (byte-offset) slots — deduped/prefixed the same
		// way NL is, so a CJK phrase matches by byte offset, not fragile token adjacency.
		slots, serr := phraseSlots(rc.text, parser)
		if serr != nil {
			return clause{}, false, serr
		}
		if len(slots) == 0 {
			return clause{}, false, nil
		}
		return clause{kind: clausePhrase, phrase: slots, weight: w}, true, nil
	case rc.star:
		return clause{kind: clausePrefix, terms: terms[:1], weight: w}, true, nil
	case len(terms) == 1:
		return clause{kind: clauseTerm, terms: terms, weight: w}, true, nil
	default:
		slots, serr := phraseSlots(rc.text, parser)
		if serr != nil {
			return clause{}, false, serr
		}
		if len(slots) == 0 {
			return clause{}, false, nil
		}
		return clause{kind: clausePhrase, phrase: slots, weight: w}, true, nil
	}
}
