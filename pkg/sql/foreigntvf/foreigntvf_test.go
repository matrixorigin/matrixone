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
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

func (c *fakeConnCache) PutForeignConn(_ context.Context, handle string, conn process.ForeignConn) (process.ForeignConn, error) {
	if existing, ok := c.conns[handle]; ok && existing != nil {
		return existing, nil
	}
	c.conns[handle] = conn
	return conn, nil
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

// errReadCloser yields data then a chosen error.
type errReadCloser struct {
	data []byte
	err  error
}

func (r *errReadCloser) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}
func (r *errReadCloser) Close() error { return nil }

// TestTruncationGuard proves a transport-level io.ErrUnexpectedEOF (premature
// connection close) becomes a hard error instead of end-of-data, while a clean
// EOF passes through.
func TestTruncationGuard(t *testing.T) {
	g := &truncationGuard{ctx: context.Background(),
		body: &errReadCloser{data: []byte("a,b\n"), err: io.ErrUnexpectedEOF}}
	buf, err := io.ReadAll(g)
	require.Equal(t, "a,b\n", string(buf))
	require.Error(t, err)
	require.Contains(t, err.Error(), "response truncated")

	g = &truncationGuard{ctx: context.Background(),
		body: &errReadCloser{data: []byte("a,b\n"), err: io.EOF}}
	buf, err = io.ReadAll(g)
	require.NoError(t, err) // io.ReadAll swallows clean EOF
	require.Equal(t, "a,b\n", string(buf))
	require.NoError(t, g.Close())
}

// TestConfigFromSessionVar covers the @esql_tvf_config / @sql_tvf_config
// fallback: resolver missing, variable unset, non-string value, and success.
func TestConfigFromSessionVar(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)

	// no resolver installed
	proc.SetResolveVariableFunc(nil)
	_, err := ConfigFromSessionVar(ctx, proc, KindSQL)
	require.ErrorContains(t, err, "unavailable")

	vars := map[string]any{}
	proc.SetResolveVariableFunc(func(name string, sys, glob bool) (any, error) {
		return vars[name], nil
	})

	// unset -> "is not set"
	_, err = ConfigFromSessionVar(ctx, proc, KindSQL)
	require.ErrorContains(t, err, "is not set")

	// non-string -> typed error
	vars[SessionVarSQLConfig] = 123
	_, err = ConfigFromSessionVar(ctx, proc, KindSQL)
	require.ErrorContains(t, err, "must be a string config")

	// string and []byte succeed, per kind
	vars[SessionVarSQLConfig] = `{"driver":"mysql","dsn":"x"}`
	got, err := ConfigFromSessionVar(ctx, proc, KindSQL)
	require.NoError(t, err)
	require.Equal(t, `{"driver":"mysql","dsn":"x"}`, got)
	vars[SessionVarESQLConfig] = []byte(`{"addresses":["http://h"]}`)
	got, err = ConfigFromSessionVar(ctx, proc, KindESQL)
	require.NoError(t, err)
	require.Equal(t, `{"addresses":["http://h"]}`, got)
}
