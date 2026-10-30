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

package plan

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestBinaryLiteralHeadingIsValidUTF8 covers #31731: an unaliased binary
// literal with bytes that are not UTF-8 must not leak those bytes into the
// result column name, which clients decode as UTF-8.
func TestBinaryLiteralHeadingIsValidUTF8(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	bindCtx := NewBindContext(builder, nil)

	sql := "select _binary'\\0\xff', _binary'\x80', _binary'abc', concat(_binary'\xfe\xff'), _binary'\xff' as payload, 'é'"
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err = appendSelectList(builder, bindCtx, nil, selectClause.Exprs...)
	require.NoError(t, err)
	require.Len(t, bindCtx.headings, 6)
	for i, heading := range bindCtx.headings {
		require.True(t, utf8.ValidString(heading), "heading %d %q", i, heading)
	}
	require.NotContains(t, bindCtx.headings[0], "\xff")
	require.Contains(t, bindCtx.headings[0], "?")
	require.Contains(t, bindCtx.headings[1], "?")
	require.Contains(t, bindCtx.headings[2], "abc")
	require.Contains(t, bindCtx.headings[3], "??")
	require.Equal(t, "payload", bindCtx.headings[4])
	require.Contains(t, bindCtx.headings[5], "é")
}

func TestValidUTF8Heading(t *testing.T) {
	require.Equal(t, "abc", validUTF8Heading("abc"))
	require.Equal(t, "é中", validUTF8Heading("é中"))
	require.Equal(t, "\x00?", validUTF8Heading("\x00\xff"))
	require.Equal(t, "a??b", validUTF8Heading("a\xfe\xffb"))
	require.Equal(t, "?é", validUTF8Heading("\x80é"))
}
