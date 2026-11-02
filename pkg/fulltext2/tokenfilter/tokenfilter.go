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

// Package tokenfilter is fulltext2's Lucene-style analyzer layer: an Analyzer is
// a tokenizer.Tokenizer followed by an ordered TokenFilter chain. It is used
// symmetrically on every tokenize path (create, CDC tail, compact, query) so a
// token dropped or rewritten at index time is handled identically at query time.
package tokenfilter

import (
	"iter"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// TokenFilter transforms a token stream, modelled on Lucene's TokenFilter: it
// consumes the upstream (Token, error) sequence and yields the filtered one, so
// a filter may drop, replace, or inject tokens. Filters chain in order.
//
// A filter that DROPS a token must leave the surviving tokens' TokenPos/BytePos
// untouched, so a positional phrase query still sees the original gap (a dropped
// stopword widens the gap between its neighbours, exactly as Lucene leaves a
// position increment).
type TokenFilter interface {
	Filter(in iter.Seq2[tokenizer.Token, error]) iter.Seq2[tokenizer.Token, error]
}

// Analyzer is a Tokenizer followed by zero or more TokenFilters applied in order
// (Lucene's Analyzer = Tokenizer + TokenFilter chain). It implements
// tokenizer.Tokenizer, so it is a drop-in wherever a Tokenizer is expected; with
// no filters it is equivalent to the wrapped tokenizer.
type Analyzer struct {
	tok     tokenizer.Tokenizer
	filters []TokenFilter
}

// NewAnalyzer returns an Analyzer applying filters (in order) to tok's output.
func NewAnalyzer(tok tokenizer.Tokenizer, filters ...TokenFilter) *Analyzer {
	return &Analyzer{tok: tok, filters: filters}
}

func (a *Analyzer) Tokenize(input []byte) iter.Seq2[tokenizer.Token, error] {
	seq := a.tok.Tokenize(input)
	for _, f := range a.filters {
		seq = f.Filter(seq)
	}
	return seq
}

// Apply wraps tok in an Analyzer applying filters in order; with no filters it
// returns tok unchanged.
func Apply(tok tokenizer.Tokenizer, filters []TokenFilter) tokenizer.Tokenizer {
	if len(filters) == 0 {
		return tok
	}
	return NewAnalyzer(tok, filters...)
}

// Stop is the only token filter supported today: it drops stopwords (the builtin
// English+Chinese list) at index time and query time, shrinking the index and
// keeping build/CDC/query tokenization symmetric. The WITH option is
// `token_filter='stop'` (comma-separated, Lucene-style, reserved for future
// filters such as synonym).
const Stop = "stop"

// ParseNames splits a persisted token_filter algo_param ("stop", or a
// comma-separated list) into normalized filter names, dropping blanks. It is the
// inverse of the join done when the option is stored, so every build path
// (create, CDC, query) reconstructs the same list.
func ParseNames(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if n := strings.ToLower(strings.TrimSpace(p)); n != "" {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return names
}

// Build resolves token filter names into the TokenFilter chain. The builtin
// stopword set is loaded once and shared (read-only), so building the chain per
// build/query is cheap. An unknown name is rejected so a typo in the WITH option
// fails loudly rather than silently indexing everything.
func Build(names []string) ([]TokenFilter, error) {
	if len(names) == 0 {
		return nil, nil
	}
	filters := make([]TokenFilter, 0, len(names))
	for _, name := range names {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "":
			continue
		case Stop:
			set, err := BuiltinStopwords()
			if err != nil {
				return nil, err
			}
			filters = append(filters, NewStopFilter(set))
		default:
			return nil, moerr.NewInvalidInputNoCtxf("fulltext2: unsupported token_filter %q", name)
		}
	}
	return filters, nil
}
