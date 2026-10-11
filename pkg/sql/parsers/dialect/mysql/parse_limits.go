// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/util"
)

// ParseLimits is opt-in; ordinary SQL parsing keeps its existing limits.
// Work includes scanner lookahead and character visits, not just input length.
type ParseLimits struct{ Input, Tokens, Work int }
type parseLimitsKey struct{}
type parseControl struct {
	ctx          context.Context
	limits       ParseLimits
	work, tokens int
}
type parseAbort struct {
	control *parseControl
	err     error
}

var ErrParseLimit error = moerr.NewResourceExhaustedf(context.Background(), "SQL parse resource limit exceeded")

func WithParseLimits(ctx context.Context, limits ParseLimits) context.Context {
	return context.WithValue(ctx, parseLimitsKey{}, limits)
}
func (s *Scanner) scanCheckpoint() {
	c := s.parseControl
	if c == nil {
		return
	}
	c.work++
	if c.work > c.limits.Work {
		panic(parseAbort{c, ErrParseLimit})
	}
	if c.work&1023 == 0 {
		if err := context.Cause(c.ctx); err != nil {
			panic(parseAbort{c, err})
		}
	}
}
func (c *parseControl) token() {
	if c == nil {
		return
	}
	if err := context.Cause(c.ctx); err != nil {
		panic(parseAbort{c, err})
	}
	c.tokens++
	if c.tokens > c.limits.Tokens {
		panic(parseAbort{c, ErrParseLimit})
	}
}
func signedIntegral(lexer yyLexer, value any) (int64, bool) {
	result, message := util.GetInt64(value)
	if message != "" {
		lexer.Error(message)
		return 0, false
	}
	return result, true
}
