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
	"strings"
	"unicode"

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
// Only such input goes through the ngram bag-of-words path; pure-Latin queries
// stay exact-phrase.
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
	var b strings.Builder
	for tk := range bj.TokenizeValue(false) {
		n := int(tk.TokenBytes[0])
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.Write(tk.TokenBytes[1 : 1+n])
	}
	return []byte(b.String()), nil
}

// IsJSONParser reports whether parser is one of the json variants.
func IsJSONParser(parser string) bool {
	p := normalizeParser(parser)
	return p == ParserJSON || p == ParserJSONValue
}

// CdcTokenizer returns the parser-aware tokenize closure the CDC consumer feeds
// to TailBuilder — ordered words per parser (json text flattened to its values
// first). It mirrors the create-TVF's row tokenization so CDC tail tokens match
// the base build and the query side.
func CdcTokenizer(parser string) (func(string) []string, error) {
	if IsJSONParser(parser) {
		simple := tokenizer.NewSimpleTokenizer()
		return func(text string) []string {
			ft, err := FlattenJSON([]byte(text), false)
			if err != nil {
				return nil
			}
			return tokenizeWords(simple, ft)
		}, nil
	}
	tok, err := DocTokenizer(parser)
	if err != nil {
		return nil, err
	}
	return func(text string) []string { return tokenizeWords(tok, []byte(text)) }, nil
}

// tokenizeWords flattens a tokenizer stream into ordered words.
func tokenizeWords(tok tokenizer.Tokenizer, text []byte) []string {
	var words []string
	for tk, err := range tok.Tokenize(text) {
		if err != nil {
			break
		}
		words = append(words, tokenWord(tk))
	}
	return words
}

// BuildSegmentFromDocsParser builds a segment from docs under parser, selecting
// the right document tokenizer and flattening json docs first. It is the
// parser-aware build entry (compile-side build-from-source uses it).
func BuildSegmentFromDocsParser(id string, pkType int32, docs []Doc, parser string) (*Segment, error) {
	tok, err := DocTokenizer(parser)
	if err != nil {
		return nil, err
	}
	p := normalizeParser(parser)
	if p == ParserJSON || p == ParserJSONValue {
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
// NL mode (boolean=false):
//   - gojieba, or Latin under ngram/json: EXACT PHRASE of the tokenized words
//     (fulltext2's distinguishing NL semantics).
//   - CJK under ngram/json: BAG-OF-WORDS over classic fulltext's redundant-ngram-
//     removed terms (fulltext.ParsePatternInNLMode), because exact-phrase over
//     overlapping/tail 3-grams is unreliable for CJK. This matches classic
//     fulltext NL membership.
//
// Boolean mode (boolean=true): standard +/-/>/</~ operators; a CJK operand under
// ngram/json expands to an OR-group of its redundant-removed ngrams (rather than
// a fragile sliding-3-gram phrase).
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

	// NL mode.
	if p != ParserGojieba && hasCJK(pat) {
		// CJK ngram → bag-of-words over redundant-removed ngrams.
		q, err := ngramBagQuery(pat)
		if err != nil {
			return nil, err
		}
		return idx.SearchBoolean(q, algo, k, filter)
	}

	// Word tokens (gojieba, or Latin) → exact phrase.
	tok, err := queryTokenizer(p)
	if err != nil {
		return nil, err
	}
	return idx.SearchText(pattern, tok, algo, k, filter)
}

// ngramBagQuery turns an NL pattern into a SHOULD-only BoolQuery of TEXT/STAR
// terms produced by classic fulltext's redundant-ngram removal — a bag-of-words
// disjunction evaluated by the WAND/boolean engine.
func ngramBagQuery(pattern string) (BoolQuery, error) {
	ps, err := fulltext.ParsePatternInNLMode(pattern, ParserNgram)
	if err != nil {
		return BoolQuery{}, err
	}
	var q BoolQuery
	for _, c := range patternsToClauses(ps) {
		q.should = append(q.should, c)
	}
	return q, nil
}

// patternsToClauses maps classic TEXT/STAR NL patterns to fulltext2 term/prefix
// clauses (default weight). Non-leaf operators are not produced by
// ParsePatternInNLMode, so only TEXT and STAR appear.
func patternsToClauses(ps []*fulltext.Pattern) []clause {
	out := make([]clause, 0, len(ps))
	for _, p := range ps {
		if p.Operator == fulltext.STAR {
			out = append(out, clause{kind: clausePrefix, terms: []string{strings.TrimSuffix(p.Text, "*")}, weight: 1.0})
		} else {
			out = append(out, clause{kind: clauseTerm, terms: []string{p.Text}, weight: 1.0})
		}
	}
	return out
}

// buildBooleanQuery parses a boolean-mode query parser-aware: gojieba/Latin
// operands tokenize into word phrases/terms; a CJK operand under ngram/json is
// expanded into an OR-group of its redundant-removed ngrams.
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

// rawToClauseParser is the parser-aware form of rawToClause (boolean.go): it
// adds CJK-operand ngram expansion for ngram/json parsers; quoted phrases,
// prefixes, groups and gojieba/Latin operands behave as in rawToClause.
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

	// A bare (non-quoted, non-prefix) CJK operand under ngram/json expands to an
	// OR-group of redundant-removed ngrams rather than a fragile sliding phrase.
	if parser != ParserGojieba && !rc.quoted && !rc.star && hasCJK(rc.text) {
		ps, err := fulltext.ParsePatternInNLMode(rc.text, ParserNgram)
		if err != nil {
			return clause{}, false, err
		}
		children := patternsToClauses(ps)
		if len(children) == 0 {
			return clause{}, false, nil
		}
		if len(children) == 1 {
			c := children[0]
			c.weight = w
			return c, true, nil
		}
		return clause{kind: clauseGroup, children: children, weight: w}, true, nil
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
		return clause{kind: clausePhrase, terms: terms, weight: w}, true, nil
	case rc.star:
		return clause{kind: clausePrefix, terms: terms[:1], weight: w}, true, nil
	case len(terms) == 1:
		return clause{kind: clauseTerm, terms: terms, weight: w}, true, nil
	default:
		return clause{kind: clausePhrase, terms: terms, weight: w}, true, nil
	}
}
