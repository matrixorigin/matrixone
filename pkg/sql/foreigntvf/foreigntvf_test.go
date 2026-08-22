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

package foreigntvf

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newDefaultSQLDialectParser mirrors newCSVParserFromReader's default (MySQL)
// dialect used by the external CSV reader for sql_tvf.
func newDefaultSQLDialectParser(t *testing.T, r *bytes.Reader) *csvparser.CSVParser {
	cfg := &csvparser.CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesTerminatedBy:  "\n",
		NotNull:            false,
		Null:               []string{`\N`},
		UnescapedQuote:     true,
	}
	p, err := csvparser.NewCSVParser(cfg, bufio.NewReader(r), csvparser.ReadBlockSize, false)
	require.NoError(t, err)
	return p
}

// TestEncodeCSVRowRoundTrip is the core correctness gate: the sql_tvf encoder
// must round-trip through the exact parser the external reader uses, preserving
// NULL vs empty string and every special character.
func TestEncodeCSVRowRoundTrip(t *testing.T) {
	fields := []sql.RawBytes{
		[]byte("alice"),
		nil,              // SQL NULL
		[]byte(""),       // empty string (NOT null)
		[]byte("a,b"),    // embedded separator
		[]byte(`a"b`),    // embedded quote
		[]byte(`a\b`),    // embedded backslash
		[]byte("l1\nl2"), // embedded newline
		[]byte("NULL"),   // literal text, must not become NULL
		[]byte(`\N`),     // literal backslash-N text, must not become NULL
	}
	wantVal := []string{"alice", "", "", "a,b", `a"b`, `a\b`, "l1\nl2", "NULL", `\N`}
	wantNull := []bool{false, true, false, false, false, false, false, false, false}

	line := encodeCSVRow(nil, fields)
	p := newDefaultSQLDialectParser(t, bytes.NewReader(line))
	row, err := p.Read(nil)
	require.NoError(t, err)
	require.Len(t, row, len(fields))
	for i := range fields {
		require.Equalf(t, wantNull[i], row[i].IsNull, "field %d null flag", i)
		if !wantNull[i] {
			require.Equalf(t, wantVal[i], row[i].Val, "field %d value", i)
		}
	}
}

func TestMakeHandle(t *testing.T) {
	// Deterministic and kind-prefixed.
	h1 := MakeHandle(KindESQL, `{"a":1}`)
	require.Equal(t, h1, MakeHandle(KindESQL, `{"a":1}`))
	require.True(t, strings.HasPrefix(h1, "esql:"))
	// Different config -> different handle.
	require.NotEqual(t, h1, MakeHandle(KindESQL, `{"a":2}`))
	// Same config, different kind -> different handle.
	require.NotEqual(t, h1, MakeHandle(KindSQL, `{"a":1}`))
}

func TestConnectSQLBadConfig(t *testing.T) {
	ctx := context.Background()
	// Malformed JSON.
	_, err := connectSQL(ctx, `not json`)
	require.Error(t, err)
	// Unsupported driver.
	_, err = connectSQL(ctx, `{"driver":"nope","dsn":"x"}`)
	require.Error(t, err)
	// Missing dsn.
	_, err = connectSQL(ctx, `{"driver":"mysql"}`)
	require.Error(t, err)
}

func TestDriverAliases(t *testing.T) {
	require.Equal(t, "pgx", driverAliases["postgres"])
	require.Equal(t, "pgx", driverAliases["postgresql"])
	require.Equal(t, "mysql", driverAliases["mysql"])
}

type fakeConnCache struct {
	conns map[string]process.ForeignConn
}

func newFakeConnCache() *fakeConnCache {
	return &fakeConnCache{conns: make(map[string]process.ForeignConn)}
}

func (c *fakeConnCache) PutForeignConn(handle string, conn process.ForeignConn) {
	c.conns[handle] = conn
}
func (c *fakeConnCache) GetForeignConn(handle string) (process.ForeignConn, bool) {
	v, ok := c.conns[handle]
	return v, ok
}
func (c *fakeConnCache) RemoveForeignConn(handle string) (process.ForeignConn, bool) {
	v, ok := c.conns[handle]
	if ok {
		delete(c.conns, handle)
	}
	return v, ok
}

var _ process.ForeignConnCache = (*fakeConnCache)(nil)

// TestResolveOrConnectEnvRef proves env:NAME config references resolve from
// the process environment BEFORE hashing, so the handle is derived from the
// resolved JSON (an env: caller and an inline caller of the same config share
// one cache entry), and that unset/empty references error clearly.
func TestResolveOrConnectEnvRef(t *testing.T) {
	ctx := context.Background()
	cache := newFakeConnCache()

	// The env var holds a config with an unsupported driver: resolution must
	// happen first (we see the driver error, not a JSON error on "env:...").
	t.Setenv("MO_FOREIGNTVF_TEST_CFG", `{"driver":"nope","dsn":"x"}`)
	_, _, err := ResolveOrConnect(ctx, cache, KindSQL, "env:MO_FOREIGNTVF_TEST_CFG")
	require.ErrorContains(t, err, `unsupported driver "nope"`)

	// unset / empty-name references error clearly.
	_, _, err = ResolveOrConnect(ctx, cache, KindSQL, "env:MO_FOREIGNTVF_TEST_UNSET")
	require.ErrorContains(t, err, "unset or empty")
	_, _, err = ResolveOrConnect(ctx, cache, KindSQL, "env:")
	require.ErrorContains(t, err, "no variable name")

	// the handle is derived from the RESOLVED config: seed the cache under the
	// resolved-config handle and connect via the env reference — it must hit.
	seeded := &seedConn{}
	resolvedHandle := MakeHandle(KindSQL, `{"driver":"nope","dsn":"x"}`)
	cache.conns[resolvedHandle] = seeded
	conn, handle, err := ResolveOrConnect(ctx, cache, KindSQL, "env:MO_FOREIGNTVF_TEST_CFG")
	require.NoError(t, err)
	require.Equal(t, resolvedHandle, handle)
	require.Same(t, seeded, conn.(*seedConn))
}

// seedConn is a minimal Conn for cache-hit testing.
type seedConn struct{}

func (c *seedConn) Close() error { return nil }
func (c *seedConn) Kind() Kind   { return KindSQL }
func (c *seedConn) Query(ctx context.Context, q string) (io.ReadCloser, error) {
	return nil, nil
}
