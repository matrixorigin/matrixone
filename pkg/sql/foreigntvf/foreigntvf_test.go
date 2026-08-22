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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
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
