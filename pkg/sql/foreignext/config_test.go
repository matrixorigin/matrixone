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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestParseTableOptions(t *testing.T) {
	ctx := context.Background()

	mk := func(kind string, kv ...string) *tree.ForeignTableParam {
		p := &tree.ForeignTableParam{Kind: kind}
		for i := 0; i < len(kv); i += 2 {
			p.Options = append(p.Options, tree.ForeignTableOption{Key: tree.Identifier(kv[i]), Val: kv[i+1]})
		}
		return p
	}

	// full config
	cfg, err := ParseTableOptions(ctx, mk("SQL", "config", `{"driver":"mysql","dsn":"d"}`, "query", "select 1"))
	require.NoError(t, err)
	require.Equal(t, KindSQL, cfg.Kind)
	require.Equal(t, `{"driver":"mysql","dsn":"d"}`, cfg.ConfigJSON)
	require.Equal(t, "select 1", cfg.DefaultQuery)

	// options are optional; keys case-insensitive
	cfg, err = ParseTableOptions(ctx, mk("esql"))
	require.NoError(t, err)
	require.Equal(t, KindESQL, cfg.Kind)
	require.Empty(t, cfg.ConfigJSON)
	cfg, err = ParseTableOptions(ctx, mk("ESQL", "CONFIG", "env:ES_CFG"))
	require.NoError(t, err)
	require.Equal(t, "env:ES_CFG", cfg.ConfigJSON)

	// duplicate key
	_, err = ParseTableOptions(ctx, mk("sql", "config", "a", "config", "b"))
	require.Error(t, err)
	// empty value
	_, err = ParseTableOptions(ctx, mk("sql", "query", "  "))
	require.Error(t, err)
	// unknown option
	_, err = ParseTableOptions(ctx, mk("sql", "recheck", "true"))
	require.Error(t, err)
	// unknown kind
	_, err = ParseTableOptions(ctx, mk("mongodb"))
	require.Error(t, err)
}

func TestEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()

	cases := []Config{
		{Kind: KindSQL, ConfigJSON: `{"driver":"mysql","dsn":"u:p@tcp(h:3306)/db"}`, DefaultQuery: "select * from t"},
		{Kind: KindESQL, ConfigJSON: "env:ES_CFG", DefaultQuery: ""},
		{Kind: KindESQL, ConfigJSON: "", DefaultQuery: "FROM idx | LIMIT 10"},
		// hostile values: ';', '*/', newline in DSN or query must not break the envelope
		{Kind: KindSQL, ConfigJSON: `{"dsn":"a;b*/c"}`, DefaultQuery: "select 1; -- */ \n"},
	}
	for _, want := range cases {
		env := BuildCreateSQLEnvelope(want)
		require.True(t, strings.HasPrefix(env, "/* "+CreateSQLEnvelopePrefix))
		// the envelope must be a single closed comment
		require.Equal(t, strings.Index(env, "*/"), len(env)-2, env)
		got, ok, err := ParseCreateSQLEnvelope(ctx, env)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, got)
	}
}

func TestEnvelopeRecognition(t *testing.T) {
	ctx := context.Background()
	// not an envelope at all
	_, ok, err := ParseCreateSQLEnvelope(ctx, `{"Create": "..."}`)
	require.NoError(t, err)
	require.False(t, ok)
	// a datastream envelope is not a foreign envelope
	_, ok, err = ParseCreateSQLEnvelope(ctx, "/* MO_DATASTREAM: version=1 */")
	require.NoError(t, err)
	require.False(t, ok)
	// forged: not anchored at the start
	_, ok, err = ParseCreateSQLEnvelope(ctx, `x /* MO_FOREIGN: version=1; kind=foreign_table; engine=sql */`)
	require.NoError(t, err)
	require.False(t, ok)
	// recognized but malformed
	for _, bad := range []string{
		"/* MO_FOREIGN: version=1; kind=foreign_table; engine=sql", // unclosed
		"/* MO_FOREIGN: version=2; kind=foreign_table; engine=sql */",
		"/* MO_FOREIGN: version=1; kind=other; engine=sql */",
		"/* MO_FOREIGN: version=1; kind=foreign_table; engine=nosuch */",
		"/* MO_FOREIGN: version=1; kind=foreign_table */",             // missing engine
		"/* MO_FOREIGN: version=1; kind=foreign_table; engine=%zz */", // bad escape
	} {
		_, ok, err := ParseCreateSQLEnvelope(ctx, bad)
		require.True(t, ok, bad)
		require.Error(t, err, bad)
	}
}

func TestResolveConfig(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	// literal passes through
	v, err := ResolveConfig(ctx, `{"driver":"mysql"}`)
	require.NoError(t, err)
	require.Equal(t, `{"driver":"mysql"}`, v)
	// empty stays empty (session-var fallback is the caller's job)
	v, err = ResolveConfig(ctx, "")
	require.NoError(t, err)
	require.Empty(t, v)
	// env reference
	t.Setenv("MO_FOREIGNEXT_TEST_CFG", `{"driver":"mysql","dsn":"x"}`)
	v, err = ResolveConfig(ctx, "env:MO_FOREIGNEXT_TEST_CFG")
	require.NoError(t, err)
	require.Equal(t, `{"driver":"mysql","dsn":"x"}`, v)
	// unset env / empty name error
	_, err = ResolveConfig(ctx, "env:MO_FOREIGNEXT_TEST_UNSET")
	require.Error(t, err)
	_, err = ResolveConfig(ctx, "env:")
	require.Error(t, err)
	// non-sys accounts (and account-less contexts) may not resolve env: refs
	_, err = ResolveConfig(defines.AttachAccountId(context.Background(), 42), "env:MO_FOREIGNEXT_TEST_CFG")
	require.ErrorContains(t, err, "require the sys account")
	_, err = ResolveConfig(context.Background(), "env:MO_FOREIGNEXT_TEST_CFG")
	require.ErrorContains(t, err, "require the sys account")
}

// TestRedactedConfigRejectedClearly proves replaying SHOW CREATE output (whose
// inline config is redacted) fails with an explanation, not a JSON error.
func TestRedactedConfigRejectedClearly(t *testing.T) {
	ctx := context.Background()
	p := &tree.ForeignTableParam{Kind: "sql", Options: tree.ForeignTableOptions{
		{Key: "config", Val: "<redacted>"},
	}}
	_, err := ParseTableOptions(ctx, p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHOW CREATE")
	require.Contains(t, err.Error(), "env:NAME")
}
