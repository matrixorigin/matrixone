// Copyright 2020 PingCAP, Inc.
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

package csvparser

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func crStripIssueConfig(withQuote bool) *CSVConfig {
	cfg := &CSVConfig{
		FieldsTerminatedBy: "0",
		LinesTerminatedBy:  "\n",
		FieldsEscapedBy:    `\`,
		Null:               []string{`\N`},
	}
	if withQuote {
		cfg.FieldsEnclosedBy = "~"
		cfg.UnescapedQuote = true
	}
	return cfg
}

// TestTrailingCRBelongsToSealedField covers issue #30462: the trailing-CR
// strip at end of record must only remove a CR that belongs to the current
// (not yet bounded) field. A CR before a field separator is content of an
// earlier field whose boundary has already been recorded; dropping it used to
// push that boundary past the end of the record buffer and panic in readRow.
func TestTrailingCRBelongsToSealedField(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withQuote bool
		input     string
		row       []Field
	}{
		// The original repro: ~ is literal mid-field text, the CR before the
		// separator stays in field 1, the trailing empty field is preserved.
		{"issue repro", true, "a~\r0", []Field{
			newStringField("a~\r", false, false),
			newStringField("", false, false),
		}},
		// Without an enclosure the same byte pattern must behave identically.
		{"no quote, EOF end", false, "a\r0", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
		{"no quote, LF end", false, "a\r0\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
		// Repeated boundaries: CR kept in field 1, both empty fields kept.
		{"two empty trailing fields", false, "a\r00\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
			newStringField("", false, false),
		}},
		// Current-field CR (CRLF physical line) is still stripped; sealed-field
		// CR is not.
		{"CRLF with full last field", false, "a0b\r\n", []Field{
			newStringField("a", false, false),
			newStringField("b", false, false),
		}},
		{"CRLF with empty last field", false, "a0\r\n", []Field{
			newStringField("a", false, false),
			newStringField("", false, false),
		}},
		{"CRLF, sealed CR and current CR", false, "a\r0\r\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := NewCSVParser(crStripIssueConfig(tc.withQuote), NewStringReader(tc.input), int64(ReadBlockSize), false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, tc.row, row)
			_, err = parser.Read(nil)
			require.ErrorIs(t, err, io.EOF)
		})
	}

	t.Run("TrimLastSep drops only the trailing empty field", func(t *testing.T) {
		cfg := crStripIssueConfig(true)
		cfg.TrimLastSep = true
		parser, err := NewCSVParser(cfg, NewStringReader("a~\r0"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, []Field{
			newStringField("a~\r", false, false),
		}, row)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("header consumer reads sealed CR", func(t *testing.T) {
		cfg := crStripIssueConfig(true)
		cfg.HeaderSchemaMatch = true
		parser, err := NewCSVParser(cfg, NewStringReader("a~\r0\nx0y\n"), int64(ReadBlockSize), true)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, []Field{
			newStringField("x", false, false),
			newStringField("y", false, false),
		}, row)
		require.Equal(t, []string{"a~\r", ""}, parser.Columns())
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("a quote that opens a field still reports unterminated", func(t *testing.T) {
		parser, err := NewCSVParser(crStripIssueConfig(true), NewStringReader("~\r0"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, errUnterminatedQuotedField)
	})

	t.Run("strip and reuse across a small read block", func(t *testing.T) {
		// A 1-block buffer (5 bytes) ends at the first CR. Refilling seals
		// that field; the next record then exercises ordinary CRLF stripping.
		parser, err := NewCSVParser(crStripIssueConfig(true), NewStringReader("abc~\r0\ny0z\r\n"), 1, false)
		require.NoError(t, err)
		for _, want := range [][]Field{
			{newStringField("abc~\r", false, false), newStringField("", false, false)},
			{newStringField("y", false, false), newStringField("z", false, false)},
		} {
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, want, row)
		}
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestTrailingCRWithRetainedFields(t *testing.T) {
	parser, err := NewCSVParser(crStripIssueConfig(false), NewStringReader("a\r00\n"), 1, false)
	require.NoError(t, err)
	parser.RetainFields(1)
	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{newStringField("a\r", false, false)}, row)
	require.Equal(t, 3, parser.fieldsSeen, "discarded fields still count for strict LOAD validation")
	_, err = parser.Read(nil)
	require.ErrorIs(t, err, io.EOF)
}
