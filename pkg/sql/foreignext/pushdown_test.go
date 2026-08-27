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

package foreignext

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapPushdownQuery(t *testing.T) {
	// the wrapper is a derived table with a generated alias; the projection
	// stays `*` because the scan maps the remote result by POSITION
	wrapped := WrapPushdownQuery("select id, name from src", "(id > 3)")
	require.True(t, strings.HasPrefix(wrapped, "select * from ("), wrapped)
	require.Contains(t, wrapped, "select id, name from src")
	require.Contains(t, wrapped, PushdownAlias("select id, name from src")+" where (id > 3)")
	// the alias is MO's own, spelled from [a-z0-9_], so it is written bare:
	// quoting is for identifiers MO does not control, and its character is
	// dialect-specific
	require.NotContains(t, wrapped, "`"+PushdownAlias("select id, name from src"))
	require.Contains(t, wrapped, PushdownAlias("select id, name from src"))

	// an empty filter is a no-op: nothing to push means nothing to wrap, and
	// the verbatim text keeps working for queries a derived table cannot hold
	require.Equal(t, "select 1", WrapPushdownQuery("select 1", ""))
	require.Equal(t, "select 1", WrapPushdownQuery("select 1", "   "))
}

// TestWrapPushdownQueryTolerantOfQueryTails covers the two ways a perfectly
// valid verbatim query becomes a syntax error once it is a derived table.
func TestWrapPushdownQueryTolerantOfQueryTails(t *testing.T) {
	// a trailing statement terminator cannot appear inside a derived table
	wrapped := WrapPushdownQuery("  select 1 ;  ", "(a = 1)")
	require.NotContains(t, wrapped, ";")
	require.Contains(t, wrapped, "select 1\n)")

	// a trailing line comment must not swallow the closing paren and WHERE:
	// the newline the wrapper inserts is what ends the comment
	wrapped = WrapPushdownQuery("select 1 -- trailing", "(a = 1)")
	require.Contains(t, wrapped, "-- trailing\n)")
	after := wrapped[strings.Index(wrapped, "-- trailing"):]
	require.Contains(t, after, "\n", "the comment must be terminated before the wrapper's own SQL")
	require.Contains(t, wrapped, " where (a = 1)")
}

func TestPushdownAlias(t *testing.T) {
	// deterministic: one scan always renders the same SQL
	require.Equal(t, PushdownAlias("select 1"), PushdownAlias("select 1"))
	// and distinct per query text
	require.NotEqual(t, PushdownAlias("select 1"), PushdownAlias("select 2"))
	// a plain identifier, and one no user would collide with by accident
	require.True(t, strings.HasPrefix(PushdownAlias("select 1"), "__mo_subq_"))
	require.Len(t, PushdownAlias("select 1"), len("__mo_subq_")+8)
}

// TestPushdownAliasIsABareIdentifier pins the property the wrapper relies on
// when it writes the alias unquoted: every character comes from an alphabet
// that needs no quoting in either dialect MO can talk to, and the name can
// neither start with a digit nor collide with a reserved word.
func TestPushdownAliasIsABareIdentifier(t *testing.T) {
	for _, q := range []string{"", "select 1", "select * from `weird name`", "SELECT\n\t1 -- x"} {
		alias := PushdownAlias(q)
		require.True(t, strings.HasPrefix(alias, "__mo_subq_"), alias)
		for _, r := range alias {
			require.True(t,
				r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'),
				"alias %q holds %q, which would need quoting", alias, r)
		}
	}
}
