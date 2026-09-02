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
//
// Portions derived from github.com/rashiq/mysql-digest; see LICENSE.

// Package digest computes MySQL 8.4-compatible normalized statement digests.
package digest

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql/mysql_digest/internal"
)

// Digest contains the SHA-256 token hash and normalized statement text.
type Digest struct {
	Hash string
	Text string
	// CommentOnly distinguishes a comment-only statement from a statement
	// whose token buffer was truncated before any token could be retained.
	CommentOnly bool
}

// SQLMode contains the lexical SQL modes that affect digest tokenization.
type SQLMode = internal.SQLMode

const (
	ModeNoBackslashEscapes = internal.MODE_NO_BACKSLASH_ESCAPES
	ModeANSIQuotes         = internal.MODE_ANSI_QUOTES
	ModePipesAsConcat      = internal.MODE_PIPES_AS_CONCAT
	ModeHighNotPrecedence  = internal.MODE_HIGH_NOT_PRECEDENCE
	DefaultMaxDigestLength = 1024
)

// Options configures MySQL 8.4 digest tokenization.
type Options struct {
	SQLMode                SQLMode
	MaxDigestLength        *int
	RejectParameterMarkers bool
}

// Digester computes statement digests with a fixed set of options.
type Digester struct {
	opts Options
}

// NewDigester creates a statement digester.
func NewDigester(opts Options) *Digester {
	return &Digester{opts: opts}
}

// Digest computes a digest for sql.
func (d *Digester) Digest(sql string) (Digest, error) {
	return compute(sql, d.opts)
}

// Compute computes a digest for sql using the supplied options, or MySQL 8.4
// defaults when options is omitted.
func Compute(sql string, opts ...Options) (Digest, error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	return compute(sql, opt)
}

func compute(sql string, opt Options) (Digest, error) {
	lexer := internal.NewLexer(sql)
	lexer.SetSQLMode(opt.SQLMode)
	lexer.SetPrepareMode(opt.RejectParameterMarkers)

	maxDigestLength := opt.MaxDigestLength
	if maxDigestLength == nil {
		value := DefaultMaxDigestLength
		maxDigestLength = &value
	}
	store := internal.NewTokenStore(maxDigestLength)
	reducer := internal.NewReducer(store)
	handler := internal.NewTokenHandler(lexer, store, reducer)
	handler.SetRejectParameterMarkers(opt.RejectParameterMarkers)

	err := handler.ProcessAll()

	return Digest{
		Hash:        store.ComputeHash(),
		Text:        store.BuildText(),
		CommentOnly: lexer.SawComment() && !lexer.SawNonComment() && !handler.SawToken(),
	}, err
}
