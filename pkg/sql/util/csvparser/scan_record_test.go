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
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestScanRecordMatchesReadFraming(t *testing.T) {
	for _, tc := range []struct {
		name, data, terminator, comment, startingBy string
		header, emptyLines, skipWhitespace          bool
		separator                                   string
		records                                     int
		syntaxError                                 bool
	}{
		{name: "multiline and escapes", data: "1,\"a\nb\"\n2,\"x\"\"y\\\\z\"\n3,\\N\n", records: 3},
		{name: "custom terminator", data: "1,\"a<END>b\"<END>2,x<END>", terminator: "<END>", records: 2},
		{name: "comments", data: "#skip\n1,x\n#skip too\n2,y\n", comment: "#", records: 2},
		{name: "starting prefix", data: "skip\nrow:1,\"a\nb\"\nrow:2,x\n", terminator: "\n", startingBy: "row:", records: 2},
		{name: "empty lines skipped", data: "\n1,x\n\n2,y\n", records: 2},
		{name: "empty lines retained", data: "\n1,x\n", emptyLines: true, records: 2},
		{name: "whitespace default data", data: " \nx\n\t", records: 3},
		{name: "JSONLINE blank records", data: "\n \n\u00a0\nx\n\t", records: 1, skipWhitespace: true},
		{name: "JSONLINE CRLF and EOF", data: " \r\nx\r\n ", records: 1, skipWhitespace: true},
		{name: "JSONLINE all whitespace", data: " \n\u2003", skipWhitespace: true},
		{name: "JSONLINE quoted whitespace", data: "\"\"\n\" \"\n", records: 2, skipWhitespace: true},
		{name: "JSONLINE tab fields", data: " \t\n", separator: "\t", records: 1, skipWhitespace: true},
		{name: "JSONLINE header", data: " \nID\n \nx\n", header: true, records: 1, skipWhitespace: true},
		{name: "empty file"},
		{name: "no final terminator", data: "1,x", records: 1},
		{name: "comment without final terminator", data: "1,x", comment: "#", records: 1},
		{name: "starting prefix without final terminator", data: "row:1,x", terminator: "\n", startingBy: "row:", records: 1},
		{name: "header", data: "ID,\"NaMe\"\n1,\"a\nb\"\n2,x\n", header: true, records: 2},
		{name: "unclosed quote", data: "1,\"a\nb", syntaxError: true},
		{name: "dangling escape", data: "1,a\\", syntaxError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CSVConfig{
				FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", FieldsEscapedBy: "\\",
				LinesTerminatedBy: tc.terminator, Comment: tc.comment,
				LinesStartingBy: tc.startingBy, AllowEmptyLine: tc.emptyLines,
				HeaderSchemaMatch: tc.header, Null: []string{`\N`},
			}
			cfg.SkipWhitespaceRecords = tc.skipWhitespace
			if tc.separator != "" {
				cfg.FieldsTerminatedBy = tc.separator
			}
			// Small buffers exercise framing across input chunks too.
			read, err := NewCSVParser(&cfg, strings.NewReader(tc.data), 7, tc.header)
			require.NoError(t, err)
			scan, err := NewCSVParser(&cfg, strings.NewReader(tc.data), 7, tc.header)
			require.NoError(t, err)
			var fields []Field
			for records := 0; ; records++ {
				fields, err = read.Read(fields)
				scanErr := scan.ScanRecord()
				require.Equal(t, read.Pos(), scan.Pos())
				require.Equal(t, read.columns, scan.columns)
				if err != nil {
					require.Equal(t, tc.records, records)
					require.EqualError(t, scanErr, err.Error())
					require.Equal(t, tc.syntaxError, IsSyntaxError(scanErr))
					if !tc.syntaxError {
						require.ErrorIs(t, scanErr, io.EOF)
					}
					break
				}
				require.NoError(t, scanErr)
			}
		})
	}
}

func TestScanRecordThenRead(t *testing.T) {
	cfg := CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", HeaderSchemaMatch: true}
	parser, err := NewCSVParser(&cfg, strings.NewReader("ID,Name\n1,\"a\nb\"\n2,\"x\"\"y\"\n"), 7, true)
	require.NoError(t, err)
	require.NoError(t, parser.ScanRecord())
	require.Equal(t, []string{"id", "name"}, parser.columns)
	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		{Val: "2"}, {Val: "x\"y", HasStringQuote: true},
	}, row)
	require.ErrorIs(t, parser.ScanRecord(), io.EOF)
	_, err = parser.Read(row)
	require.ErrorIs(t, err, io.EOF)
}

func TestCSVLookaheadPropagatesReadError(t *testing.T) {
	for _, tc := range []struct {
		name, data, comment, startingBy string
		completeRecords                 int
	}{
		{name: "comment", data: "1,xx\n2,y\n", comment: "#", completeRecords: 1},
		{name: "starting prefix", data: "row:1,x\nrow:2,y\n", startingBy: "row:"},
	} {
		for _, scan := range []bool{false, true} {
			method := "Read"
			if scan {
				method = "ScanRecord"
			}
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				cfg := CSVConfig{FieldsTerminatedBy: ",", LinesTerminatedBy: "\n",
					Comment: tc.comment, LinesStartingBy: tc.startingBy}
				// TimeoutReader fails only its second read; a third read would succeed.
				input := iotest.TimeoutReader(strings.NewReader(tc.data))
				parser, err := NewCSVParser(&cfg, input, 1, false)
				require.NoError(t, err)
				advance := func() error {
					if scan {
						return parser.ScanRecord()
					}
					_, err := parser.Read(nil)
					return err
				}
				for range tc.completeRecords {
					require.NoError(t, advance())
				}
				require.ErrorIs(t, advance(), iotest.ErrTimeout)
			})
		}
	}
}

func TestWhitespaceRecordCancellation(t *testing.T) {
	for _, scan := range []bool{false, true} {
		t.Run(fmt.Sprint(scan), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			parser, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", SkipWhitespaceRecords: true, Context: ctx}, strings.NewReader("x\n \n \n"), 64, false)
			require.NoError(t, err)
			require.NoError(t, parser.ScanRecord())
			cancel()
			if scan {
				require.ErrorIs(t, parser.ScanRecord(), context.Canceled)
			} else {
				_, err = parser.Read(nil)
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Equal(t, int64(2), parser.Pos(), "buffered whitespace must not consume records after cancellation")
		})
	}
}
