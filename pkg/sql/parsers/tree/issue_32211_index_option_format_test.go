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

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// Issue #32211 review R1: the global IndexOption formatter must keep
// rendering ASYNC-only options. The delegated-empty-option normalization
// owns only the whitelisted KEY_BLOCK_SIZE/comment/visibility fields and
// must never drop unrelated legal option content (previously an ASYNC-only
// option still rendered through the caller's leading separator).
func TestIssue32211IndexOptionAsyncOnlyStillRenders(t *testing.T) {
	opt := &IndexOption{Async: true}

	ctx := NewFmtCtx(dialect.MYSQL)
	opt.Format(ctx)
	require.Contains(t, ctx.String(), "ASYNC",
		"an ASYNC-only legal index option must keep its ASYNC render")

	wrapped := &Index{
		Name:        "idx_async",
		KeyType:     INDEX_TYPE_INVALID,
		IndexOption: opt,
	}
	serialized := StringWithOpts(wrapped, dialect.MYSQL,
		WithQuoteIdentifier(), WithSingleQuoteString())
	require.Contains(t, serialized, "ASYNC",
		"the serialized index must keep the ASYNC option")
}

// Issue #32211 review R2: the single-quote comment branch must reuse the
// OP_TYPE owner semantics — FormatString for the consumer's backslash-aware
// ParseOne, then single-quote doubling — so the raw-SQL byte loop restores
// the exact comment. Expected bytes are pinned per case per mode as an
// independent oracle; the default mode keeps writing raw bytes.
func TestIssue32211IndexOptionCommentEscapingBytes(t *testing.T) {
	cases := []struct {
		name string
		// default-mode bytes: the raw comment exactly as before
		wantDefault string
		// singleQuoteString-mode bytes: FormatString + quote doubling
		wantSingleQuoted string
		// singleQuoteString + NO_BACKSLASH_ESCAPES form: only quote doubling
		wantNoBackslash string
	}{
		{
			name:             "real newline",
			wantDefault:      " comment line1\nline2 ",
			wantSingleQuoted: " comment 'line1\\nline2' ",
			wantNoBackslash:  " comment 'line1\nline2' ",
		},
		{
			name:             "real tab",
			wantDefault:      " comment col1\tcol2 ",
			wantSingleQuoted: " comment 'col1\\tcol2' ",
			wantNoBackslash:  " comment 'col1\tcol2' ",
		},
		{
			// value bytes: a backslash b space c
			name:             "backslash before letter",
			wantDefault:      " comment a\\b c ",
			wantSingleQuoted: " comment 'a\\\\b c' ",
			wantNoBackslash:  " comment 'a\\b c' ",
		},
		{
			// value bytes: p r e backslash quote p o s t
			name:             "backslash before quote",
			wantDefault:      " comment pre\\'post ",
			wantSingleQuoted: " comment 'pre\\\\''post' ",
			wantNoBackslash:  " comment 'pre\\''post' ",
		},
		{
			name:             "utf8",
			wantDefault:      " comment 数据库 ✓ ok ",
			wantSingleQuoted: " comment '数据库 ✓ ok' ",
			wantNoBackslash:  " comment '数据库 ✓ ok' ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Restore the exact producing source values for each expected
			// byte form (reverse of the escaping the oracle pins).
			var comment string
			switch tc.name {
			case "real newline":
				comment = "line1\nline2"
			case "real tab":
				comment = "col1\tcol2"
			case "backslash before letter":
				comment = "a\\b c"
			case "backslash before quote":
				comment = "pre\\'post"
			default:
				comment = "数据库 ✓ ok"
			}

			def := NewFmtCtx(dialect.MYSQL)
			(&IndexOption{Comment: comment}).Format(def)
			require.Equal(t, tc.wantDefault, def.String(),
				"default formatting must keep writing the raw comment bytes")

			sq := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString())
			(&IndexOption{Comment: comment}).Format(sq)
			require.Equal(t, tc.wantSingleQuoted, sq.String(),
				"single-quote mode must mirror the OP_TYPE owner bytes")

			nb := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithNoBackslashEscape())
			(&IndexOption{Comment: comment}).Format(nb)
			require.Equal(t, tc.wantNoBackslash, nb.String(),
				"NO_BACKSLASH_ESCAPES form must only double quotes")
		})
	}
}
