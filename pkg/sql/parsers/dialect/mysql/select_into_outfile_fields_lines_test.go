// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestSelectIntoOutfileFieldsEscapedBy covers the FIELDS ESCAPED BY clause of
// SELECT INTO OUTFILE: the grammar accepts the same sub-clause set as LOAD
// DATA (issue #30710/#30754), merged over the historical OUTFILE defaults
// (comma terminator, double-quote enclosure). Serialization through Format is
// pinned so clause presence is observable in statement text.
func TestSelectIntoOutfileFieldsEscapedBy(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name       string
		input      string
		terminated string
		enclosed   byte
		optionally bool
		// nil escaped means "not specified": the writer keeps its '\\' default.
		escaped *byte
		output  string
	}{
		{
			name:       "default fields keep historical framing",
			input:      "select * from t1 into outfile 'o.csv'",
			terminated: ",", enclosed: '"',
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			name:       "fields terminated only keeps defaults elsewhere",
			input:      "select * from t1 into outfile 'o.csv' fields terminated by ','",
			terminated: ",", enclosed: '"',
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			name:       "issue 30754 reproduce statement",
			input:      `select 1 into outfile 'o.csv' fields terminated by ',' enclosed by '' escaped by '\\'`,
			terminated: ",", enclosed: 0, escaped: b(`\`),
			output: "select 1 into outfile o.csv fields terminated by , escaped by \\ lines terminated by \n header true",
		},
		{
			name:       "escaped by alone",
			input:      "select * from t1 into outfile 'o.csv' fields escaped by '#'",
			terminated: ",", enclosed: '"', escaped: b("#"),
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \" escaped by # lines terminated by \n header true",
		},
		{
			name:       "escaped by empty disables escaping",
			input:      "select * from t1 into outfile 'o.csv' fields escaped by ''",
			terminated: ",", enclosed: '"', escaped: b(""),
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \" escaped by '' lines terminated by \n header true",
		},
		{
			name:       "escaped before terminated (any order)",
			input:      "select * from t1 into outfile 'o.csv' fields escaped by '!' terminated by ';'",
			terminated: ";", enclosed: '"', escaped: b("!"),
			output: "select * from t1 into outfile o.csv fields terminated by ; enclosed by \" escaped by ! lines terminated by \n header true",
		},
		{
			name:       "enclosed after escaped (any order)",
			input:      "select * from t1 into outfile 'o.csv' fields escaped by '~' enclosed by '|'",
			terminated: ",", enclosed: '|', escaped: b("~"),
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by | escaped by ~ lines terminated by \n header true",
		},
		{
			name:       "optionally enclosed",
			input:      `select * from t1 into outfile 'o.csv' fields optionally enclosed by '"' escaped by '\\'`,
			terminated: ",", enclosed: '"', optionally: true, escaped: b(`\`),
			output: "select * from t1 into outfile o.csv fields terminated by , optionally enclosed by \" escaped by \\ lines terminated by \n header true",
		},
		{
			name:       "duplicate clause: last one wins, mirroring the LOAD merge",
			input:      "select * from t1 into outfile 'o.csv' fields escaped by 'a' escaped by 'b'",
			terminated: ",", enclosed: '"', escaped: b("b"),
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \" escaped by b lines terminated by \n header true",
		},
		{
			name:       "enclosed by backslash accepted",
			input:      `select * from t1 into outfile 'o.csv' fields enclosed by '\\' escaped by '\\'`,
			terminated: ",", enclosed: '\\', escaped: b(`\`),
			output: "select * from t1 into outfile o.csv fields terminated by , enclosed by \\ escaped by \\ lines terminated by \n header true",
		},
		{
			name:       "all four sub-clauses together",
			input:      `select * from t1 into outfile 'o.csv' fields terminated by '|' optionally enclosed by '"' escaped by '^'`,
			terminated: "|", enclosed: '"', optionally: true, escaped: b("^"),
			output: "select * from t1 into outfile o.csv fields terminated by | optionally enclosed by \" escaped by ^ lines terminated by \n header true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := ParseOne(ctx, tc.input, 1)
			require.NoError(t, err, "parse error for: %s", tc.input)

			selectStmt, ok := ast.(*tree.Select)
			require.True(t, ok, "expected Select statement")
			require.NotNil(t, selectStmt.Ep, "expected ExportParam")

			f := selectStmt.Ep.Fields
			require.NotNil(t, f)
			require.Equal(t, tc.terminated, f.Terminated.Value)
			require.Equal(t, tc.enclosed, f.EnclosedBy.Value)
			require.Equal(t, tc.optionally, f.Optionally)
			if tc.escaped == nil {
				require.Nil(t, f.EscapedBy, "ESCAPED BY must stay unset when not specified")
			} else {
				require.NotNil(t, f.EscapedBy)
				require.Equal(t, *tc.escaped, f.EscapedBy.Value)
			}

			// Serialize/Format round-trip: identical output, stable tree.
			out := tree.String(ast, dialect.MYSQL)
			require.Equal(t, tc.output, out, "output mismatch for: %s", tc.input)
			out2 := tree.String(ast, dialect.MYSQL)
			require.Equal(t, out, out2)
		})
	}
}

// TestSelectIntoOutfileLinesStartingBy covers LINES STARTING BY in both
// clause orders, including a custom terminator and the defaults.
func TestSelectIntoOutfileLinesStartingBy(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name         string
		input        string
		startingBy   string
		terminatedBy string
		output       string
	}{
		{
			name:         "default lines",
			input:        "select * from t1 into outfile 'o.csv'",
			startingBy:   "",
			terminatedBy: "\n",
			output:       "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			name:         "issue 30754 reproduce statement",
			input:        `select 1 into outfile 'o.csv' lines starting by '>>' terminated by '\n'`,
			startingBy:   ">>",
			terminatedBy: "\n",
			output:       "select 1 into outfile o.csv fields terminated by , enclosed by \" lines starting by >> terminated by \n header true",
		},
		{
			name:         "starting by alone",
			input:        "select * from t1 into outfile 'o.csv' lines starting by '<<'",
			startingBy:   "<<",
			terminatedBy: "\n",
			output:       "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines starting by << terminated by \n header true",
		},
		{
			name:         "terminated first, starting second",
			input:        `select * from t1 into outfile 'o.csv' lines terminated by '\r\n' starting by '>>'`,
			startingBy:   ">>",
			terminatedBy: "\r\n",
			output:       "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines starting by >> terminated by \r\n header true",
		},
		{
			name:         "multi-character terminator with prefix",
			input:        "select * from t1 into outfile 'o.csv' lines starting by '#>' terminated by '||'",
			startingBy:   "#>",
			terminatedBy: "||",
			output:       "select * from t1 into outfile o.csv fields terminated by , enclosed by \" lines starting by #> terminated by || header true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := ParseOne(ctx, tc.input, 1)
			require.NoError(t, err, "parse error for: %s", tc.input)

			selectStmt, ok := ast.(*tree.Select)
			require.True(t, ok, "expected Select statement")
			require.NotNil(t, selectStmt.Ep, "expected ExportParam")

			l := selectStmt.Ep.Lines
			require.NotNil(t, l)
			require.Equal(t, tc.startingBy, l.StartingBy)
			require.Equal(t, tc.terminatedBy, l.TerminatedBy.Value)

			out := tree.String(ast, dialect.MYSQL)
			require.Equal(t, tc.output, out, "output mismatch for: %s", tc.input)
		})
	}
}

// TestSelectIntoOutfileFieldsLinesCombined exercises both clauses together,
// plus FORMAT and the other existing OUTFILE options staying parseable after
// the generalized field list, with the full serialization pinned.
func TestSelectIntoOutfileFieldsLinesCombined(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:   "fields and lines with all sub-clauses",
			input:  `select * from t1 into outfile 'o.csv' fields terminated by '|' enclosed by '' escaped by '!' lines starting by '>>' terminated by '\n'`,
			output: "select * from t1 into outfile o.csv fields terminated by | escaped by ! lines starting by >> terminated by \n header true",
		},
		{
			name:   "format csv before fields and lines",
			input:  `select 1 into outfile 'o.csv' format 'csv' fields escaped by '#' lines starting by '>>'`,
			output: "select 1 into outfile o.csv format csv fields terminated by , enclosed by \" escaped by # lines starting by >> terminated by \n header true",
		},
		{
			name:   "splitsize fields lines header max_file_size force_quote",
			input:  `select a, b from t1 into outfile 'd_%03d.csv' splitsize '1M' fields escaped by '!' lines starting by '$' header 'false' max_file_size 100 force_quote (a)`,
			output: "select a, b from t1 into outfile d_%03d.csv splitsize 1048576 fields terminated by , enclosed by \" escaped by ! lines starting by $ terminated by \n header false max_file_size 102400 force_quote a",
		},
		{
			name:   "escaped by empty with starting by",
			input:  `select * from t1 into outfile 'o.csv' fields terminated by '' escaped by '' lines starting by '>>' terminated by ''`,
			output: "select * from t1 into outfile o.csv fields terminated by '' enclosed by \" escaped by '' lines starting by >> terminated by '' header true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := ParseOne(ctx, tc.input, 1)
			require.NoError(t, err, "parse error for: %s", tc.input)
			require.Equal(t, tc.output, tree.String(ast, dialect.MYSQL))
		})
	}
}

// TestSelectIntoOutfileFieldsLinesInvalid keeps the single-byte validation for
// enclosure/escape and rejects words that are not part of the clause grammar.
func TestSelectIntoOutfileFieldsLinesInvalid(t *testing.T) {
	ctx := context.TODO()

	invalidCases := []struct {
		name  string
		input string
	}{
		{
			name:  "multi-byte escape rejected",
			input: "select * from t1 into outfile 'o.csv' fields escaped by 'xy'",
		},
		{
			name:  "multi-byte enclosure rejected",
			input: "select * from t1 into outfile 'o.csv' fields enclosed by 'xy'",
		},
		{
			name:  "unknown fields sub-clause rejected",
			input: "select * from t1 into outfile 'o.csv' fields doubled by ','",
		},
		{
			name:  "stray keyword between items rejected",
			input: `select * from t1 into outfile 'o.csv' fields terminated by ',' lines by '\n'`,
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseOne(ctx, tc.input, 1)
			require.Error(t, err, "expected parse error for: %s", tc.input)
		})
	}
}

// b is a tiny helper making single-character escape expectations readable:
// b(`\`) is '\\', b("") is 0 (ESCAPED BY ”), b("#") is '#'.
func b(s string) *byte {
	if s == "" {
		zero := byte(0)
		return &zero
	}
	v := s[0]
	return &v
}
