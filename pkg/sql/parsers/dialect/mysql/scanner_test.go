// Copyright 2021 Matrix Origin
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

package mysql

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestLiteralID(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in:  "`aa`",
		id:  QUOTE_ID,
		out: "aa",
	}, {
		in:  "```a```",
		id:  QUOTE_ID,
		out: "`a`",
	}, {
		in:  "`a``b`",
		id:  QUOTE_ID,
		out: "a`b",
	}, {
		in:  "`a``b`c",
		id:  QUOTE_ID,
		out: "a`b",
	}, {
		in:  "`a``b",
		id:  LEX_ERROR,
		out: "a`b",
	}, {
		in:  "`a``b``",
		id:  LEX_ERROR,
		out: "a`b`",
	}, {
		in:  "``",
		id:  LEX_ERROR,
		out: "",
	}, {
		in:  "```a``b``",
		id:  LEX_ERROR,
		out: "`a`b`",
	}, {
		in:  "```",
		id:  LEX_ERROR,
		out: "`",
	}}

	for _, tcase := range testcases {
		s := NewScanner(dialect.MYSQL, tcase.in)
		id, out := s.Scan()
		if tcase.id != id || string(out) != tcase.out {
			t.Errorf("Scan(%s): %d, %s, want %d, %s", tcase.in, id, out, tcase.id, tcase.out)
		}
	}
}

func TestQuotedUnicodeIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		value string
	}{
		{name: "quoted Arabic", input: "`الكمية`", value: "الكمية"},
		{name: "quoted Chinese", input: "`数量`", value: "数量"},
		{name: "escaped delimiter with Unicode", input: "`数``量`", value: "数`量"},
		{name: "latin1 client byte", input: "`\xe9`", value: "\xe9"},
		{name: "latin1 client byte before escaped delimiter", input: "`\xe9``name`", value: "\xe9`name"},
		{name: "latin1 bytes forming supplementary UTF-8", input: "`\xf0\x9f\x98\x80`", value: "\xf0\x9f\x98\x80"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.input)
			token, value := scanner.Scan()
			if token != QUOTE_ID || value != test.value {
				t.Fatalf("Scan(%q) = (%s, %q), want (%s, %q)",
					test.input, tokenName(token), value, tokenName(QUOTE_ID), test.value)
			}
		})
	}
}

func TestUnquotedExtendedIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		value string
	}{
		{name: "UTF-8 continuation", input: "t_ãg", value: "t_ãg"},
		{name: "UTF-8 leading", input: "数量", value: "数量"},
		{name: "maximum BMP code point", input: "\uFFFF", value: "\uFFFF"},
		{name: "digit leading", input: "1数量", value: "1数量"},
		{name: "digit leading with exponent letter", input: "1e数量", value: "1e数量"},
		{name: "raw latin1 client byte", input: "t_\xe9g", value: "t_\xe9g"},
		{name: "digit leading raw latin1 client byte", input: "1\xe9A", value: "1\xe9a"},
		{name: "keyword prefix", input: "selectã", value: "selectã"},
		{name: "charset name as identifier", input: "_utf8mb4", value: "_utf8mb4"},
		{name: "charset prefix with ASCII suffix", input: "_utf8mb4table", value: "_utf8mb4table"},
		{name: "charset prefix with digit suffix", input: "_utf8mb41", value: "_utf8mb41"},
		{name: "charset prefix with BMP suffix", input: "_utf8mb4数量", value: "_utf8mb4数量"},
		{name: "charset prefix with raw latin1 suffix", input: "_utf8mb4\xe9A", value: "_utf8mb4\xe9A"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.input)
			defer PutScanner(scanner)
			token, value := scanner.Scan()
			if token != ID || value != test.value {
				t.Fatalf("Scan(%q) = (%s, %q), want (%s, %q)",
					test.input, tokenName(token), value, tokenName(ID), test.value)
			}
		})
	}
}

func TestCharsetIntroducer(t *testing.T) {
	for _, input := range []string{"_utf8mb4'test'", "_utf8mb4 \t'test'"} {
		t.Run(input, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, input)
			defer PutScanner(scanner)
			token, value := scanner.Scan()
			if token != STRING || value != "test" {
				t.Fatalf("Scan charset introducer = (%s, %q), want (%s, %q)",
					tokenName(token), value, tokenName(STRING), "test")
			}
		})
	}
}

func TestUnquotedSupplementaryIdentifierRejected(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantTokens []int
	}{
		{name: "leading supplementary", input: "😀", wantTokens: []int{LEX_ERROR}},
		{name: "ASCII then supplementary", input: "t😀", wantTokens: []int{ID, LEX_ERROR}},
		{name: "digit then supplementary", input: "1😀", wantTokens: []int{INTEGRAL, LEX_ERROR}},
		{name: "hex prefix then supplementary", input: "0x😀", wantTokens: []int{LEX_ERROR}},
		{name: "bit prefix then supplementary", input: "0b😀", wantTokens: []int{LEX_ERROR}},
		{name: "user variable then supplementary", input: "@😀", wantTokens: []int{LEX_ERROR}},
		{name: "system variable then supplementary", input: "@@😀", wantTokens: []int{LEX_ERROR}},
		{name: "charset prefix then supplementary", input: "_utf8mb4😀", wantTokens: []int{ID, LEX_ERROR}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.input)
			defer PutScanner(scanner)
			for i, want := range test.wantTokens {
				token, _ := scanner.Scan()
				if token != want {
					t.Fatalf("Scan(%q) token %d = %s, want %s",
						test.input, i, tokenName(token), tokenName(want))
				}
			}
		})
	}
}

func TestUnquotedRawByteDirectIdentifierEntries(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantToken int
		wantValue string
	}{
		{name: "hex prefix", input: "0x\xe9A", wantToken: ID, wantValue: "0x\xe9a"},
		{name: "bit prefix", input: "0b\xe9A", wantToken: ID, wantValue: "0b\xe9a"},
		{name: "user variable", input: "@\xe9A", wantToken: AT_ID, wantValue: "\xe9A"},
		{name: "system variable", input: "@@\xe9A", wantToken: AT_AT_ID, wantValue: "\xe9A"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.input)
			defer PutScanner(scanner)
			token, value := scanner.Scan()
			if token != test.wantToken || value != test.wantValue {
				t.Fatalf("Scan(%q) = (%s, %q), want (%s, %q)",
					test.input, tokenName(token), value, tokenName(test.wantToken), test.wantValue)
			}
		})
	}
}

func TestUnquotedIdentifierRejectsPunctuationBeforeUnicode(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantToken     int
		wantValue     string
		wantNextToken int
		wantNextValue string
	}{
		{name: "decimal point", input: "1.数量", wantToken: FLOAT, wantValue: "1.", wantNextToken: ID, wantNextValue: "数量"},
		{name: "positive exponent", input: "1e+数量", wantToken: FLOAT, wantValue: "1e+", wantNextToken: ID, wantNextValue: "数量"},
		{name: "negative exponent", input: "1e-数量", wantToken: FLOAT, wantValue: "1e-", wantNextToken: ID, wantNextValue: "数量"},
		{name: "hex prefix", input: "0x.数量", wantToken: LEX_ERROR, wantValue: "."},
		{name: "bit prefix", input: "0b+数量", wantToken: LEX_ERROR, wantValue: "+"},
		{name: "user variable", input: "@+数量", wantToken: LEX_ERROR},
		{name: "system variable", input: "@@-数量", wantToken: LEX_ERROR},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.input)
			defer PutScanner(scanner)
			token, value := scanner.Scan()
			if token != test.wantToken || value != test.wantValue {
				t.Fatalf("Scan(%q) = (%s, %q), want (%s, %q)",
					test.input, tokenName(token), value, tokenName(test.wantToken), test.wantValue)
			}
			if test.wantNextToken != 0 {
				token, value = scanner.Scan()
				if token != test.wantNextToken || value != test.wantNextValue {
					t.Fatalf("Scan(%q) next = (%s, %q), want (%s, %q)",
						test.input, tokenName(token), value, tokenName(test.wantNextToken), test.wantNextValue)
				}
			}
		})
	}
}

func TestScannerSQLModePipeConcat(t *testing.T) {
	s := NewScannerWithSQLMode(dialect.MYSQL, "||", ParseSQLModeFlags("PIPES_AS_CONCAT"))
	id, _ := s.Scan()
	if id != PIPE_CONCAT {
		t.Fatalf("PIPES_AS_CONCAT || token = %s, want PIPE_CONCAT", tokenName(id))
	}
	PutScanner(s)

	s = NewScanner(dialect.MYSQL, "||")
	defer PutScanner(s)
	id, _ = s.Scan()
	if id != OR {
		t.Fatalf("default || token after scanner reuse = %s, want OR", tokenName(id))
	}
}

func tokenName(id int) string {
	if id == STRING {
		return "STRING"
	} else if id == HEXNUM {
		return "HEXNUM"
	} else if id == BIT_LITERAL {
		return "BIT_LITERAL"
	} else if id == LEX_ERROR {
		return "LEX_ERROR"
	}
	return fmt.Sprintf("%d", id)
}

func TestString(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "''",
		id:   STRING,
		want: "",
	}, {
		in:   "''''",
		id:   STRING,
		want: "'",
	}, {
		in:   "'hello'",
		id:   STRING,
		want: "hello",
	}, {
		in:   "'\\n'",
		id:   STRING,
		want: "\n",
	}, {
		in:   "'\\nhello\\n'",
		id:   STRING,
		want: "\nhello\n",
	}, {
		in:   "'a''b'",
		id:   STRING,
		want: "a'b",
	}, {
		in:   "'a\\'b'",
		id:   STRING,
		want: "a'b",
	}, {
		in:   "'\\'",
		id:   LEX_ERROR,
		want: "'",
	}, {
		in:   "'",
		id:   LEX_ERROR,
		want: "",
	}, {
		in:   "'hello\\'",
		id:   LEX_ERROR,
		want: "hello'",
	}, {
		in:   "'hello",
		id:   LEX_ERROR,
		want: "hello",
	}, {
		in:   "'hello\\",
		id:   LEX_ERROR,
		want: "hello",
	}, {
		in:   "'C:\\Program Files(x86)'",
		id:   STRING,
		want: "C:Program Files(x86)",
	}, {
		in:   "'C:\\\\Program Files(x86)'",
		id:   STRING,
		want: "C:\\Program Files(x86)",
	}, {
		in:   "$$a\\n$$",
		id:   STRING,
		want: "a\\n",
	}}

	for _, tcase := range testcases {
		id, got := NewScanner(dialect.MYSQL, tcase.in).Scan()
		if tcase.id != id || string(got) != tcase.want {
			t.Errorf("Scan(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
		}
	}
}

// TestSqlquoteStringRoundTrip proves sqlquote.String produces literals this
// scanner reads back UNCHANGED — the property the AUTO_UPDATE / REINDEX catalog
// writes depend on. Backslash-bearing values (Windows paths, trailing backslash)
// are exactly the cases plain quote-doubling corrupts; compare the
// 'C:\Program Files(x86)' -> "C:Program Files(x86)" case in TestString above.
func TestSqlquoteStringRoundTrip(t *testing.T) {
	for _, v := range []string{
		"abc",
		"",
		"a'b",
		`a\b`,
		`C:\Program Files`,
		`a\\b`,
		`x\`, // trailing backslash would otherwise swallow the closing quote
		`'`,
		`\`,
		`\n`, // literal backslash-n, must NOT decode to a newline
		`{"k":"v's","p":"a\b"}`,
		"tab\tnl\nq'bs\\x", // literal control chars pass through; quote + backslash escaped
		`维度\x'y`,           // multibyte UTF-8 alongside backslash + quote (byte-level escape must not corrupt it)
	} {
		lit := sqlquote.String(v)
		id, got := NewScanner(dialect.MYSQL, lit).Scan()
		if id != STRING || string(got) != v {
			t.Errorf("round-trip %q via %q: Scan() = (%s, %q), want (STRING, %q)",
				v, lit, tokenName(id), got, v)
		}
	}
}

func TestBuffer(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "'webapp'@'localhost'",
		id:   STRING,
		want: "webapp",
	}}

	for _, tcase := range testcases {
		id, got := NewScanner(dialect.MYSQL, tcase.in).Scan()
		if tcase.id != id || string(got) != tcase.want {
			t.Errorf("Scan(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
		}
	}
}

func TestComment(t *testing.T) {
	testcases := []struct {
		name  string
		in    string
		id    int
		want  string
		want2 string
	}{
		{
			name: "1",
			in:   "abc /* abc */ abc",
			id:   COMMENT,
			want: "/* abc */",
		},
		{
			name: "1",
			in:   "abc /** abc **/ abc",
			id:   COMMENT,
			want: "/** abc **/",
		},
		{
			name: "*/ after comment",
			in:   "abc /** abc **/*/ abc",
			id:   COMMENT,
			want: "/** abc **/",
		},
		{
			name: "//comment",
			in:   "abc //** abc **/*/ abc",
			id:   COMMENT,
			want: "//** abc **/*/ abc",
		},
		{
			name: "// in block comment",
			in:   "abc /** //abc **/*/ abc",
			id:   COMMENT,
			want: "/** //abc **/",
		},
		{
			name: "embedded block comment",
			in:   "abc /** /* /abc **/*/ abc",
			id:   COMMENT,
			want: "/** /* /abc **/",
		},
		{
			name: "no comment",
			in:   "abc /a/a abc",
			id:   eofChar,
			want: "",
		},
		{
			name: "no comment",
			in:   "abc /a/a abc/",
			id:   eofChar,
			want: "",
		},
		{
			name: "nothing",
			in:   "",
			id:   eofChar,
			want: "",
		},
		{
			name: "newline",
			in:   "\n",
			id:   eofChar,
			want: "",
		},
		{
			name: "newline",
			in:   "dfa fda \r\n",
			id:   eofChar,
			want: "",
		},
		{
			name: "no newline",
			in:   "dfa fda ",
			id:   eofChar,
			want: "",
		},
		{
			name: "incomplete line comment",
			in:   " / ",
			id:   eofChar,
			want: "",
		},
		{
			name: "incomplete block comment",
			in:   " /* ",
			id:   LEX_ERROR,
			want: "",
		},
		{
			name: "incomplete block comment",
			in:   " /* * ",
			id:   LEX_ERROR,
			want: "",
		},
		{
			name: "incomplete block comment",
			in:   " /* * /",
			id:   LEX_ERROR,
			want: "",
		},
		{
			name: "incomplete block comment",
			in:   " / * * /",
			id:   eofChar,
			want: "",
		},
		{
			name: "block comment",
			in:   " /* * /  /* */ ",
			id:   COMMENT,
			want: "/* * /  /* */",
		},
		{
			name: "block comment",
			in:   " /* * /  /* */ ",
			id:   COMMENT,
			want: "/* * /  /* */",
		},
		{
			name:  "two block comment",
			in:    " /* * /  /* */ /* abc */ ",
			id:    COMMENT,
			want:  "/* * /  /* */",
			want2: "/* abc */",
		},
		{
			name:  "two block comment",
			in:    " /* * /  /* */ // ",
			id:    COMMENT,
			want:  "/* * /  /* */",
			want2: "// ",
		},
	}

	for _, tcase := range testcases {
		scan := NewScanner(dialect.MYSQL, tcase.in)
		id, got := scan.ScanComment()
		if tcase.id != id || id != LEX_ERROR && string(got) != tcase.want {
			t.Errorf("ScanComment(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
		}

		if tcase.want2 != "" {
			id, got = scan.ScanComment()
			if tcase.id != id || id != LEX_ERROR && string(got) != tcase.want2 {
				t.Errorf("ScanComment(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want2)
			}
		}
	}
}

func TestBitValueLiteral(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "b'00011011'",
		id:   BIT_LITERAL,
		want: "0b00011011",
	}, {
		in:   "0b00011011",
		id:   BIT_LITERAL,
		want: "0b00011011",
	}, {
		in:   "0b",
		id:   ID,
		want: "0b",
	}, {
		in:   "0b0a1fg",
		id:   ID,
		want: "0b0a1fg",
	}}

	for _, tcase := range testcases {
		id, got := NewScanner(dialect.MYSQL, tcase.in).Scan()
		if tcase.id != id || got != tcase.want {
			t.Errorf("Scan(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
		}
	}
}

func TestHexadecimalLiteral(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "x'616263'",
		id:   HEXNUM,
		want: "0x616263",
	}, {
		in:   "0x616263",
		id:   HEXNUM,
		want: "0x616263",
	}, {
		in:   "0x",
		id:   ID,
		want: "0x",
	}, {
		in:   "0X0a1fg",
		id:   ID,
		want: "0x0a1fg",
	}}

	for _, tcase := range testcases {
		id, got := NewScanner(dialect.MYSQL, tcase.in).Scan()
		if tcase.id != id || got != tcase.want {
			t.Errorf("Scan(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
		}
	}
}

func TestScannerPoolCleanupAndThreshold(t *testing.T) {
	// Case 1: normal small SQL should be pooled and fields cleared
	s := NewScanner(dialect.MYSQL, "select 1")
	// grow strBuilder a little to ensure it is cleared on Put
	s.strBuilder.WriteString("abc")
	s.executableCommentEnd = 42
	PutScanner(s)

	// Fetch again to see if we receive a cleared scanner from pool
	s2 := NewScanner(dialect.MYSQL, "select 2")
	if s2.LastToken != "" || s2.LastError != nil || s2.MysqlSpecialComment != nil || s2.Pos != 0 || s2.Line != 0 || s2.Col != 0 || s2.PrePos != 0 || s2.executableCommentEnd != 0 {
		t.Fatalf("pooled scanner should be reset: %+v", s2)
	}
	if s2.strBuilder.Len() != 0 {
		t.Fatalf("strBuilder should be cleared")
	}
	PutScanner(s2)

	// Case 2: big SQL (>1MiB) should NOT be pooled
	big := make([]byte, (1<<20)+10)
	for i := range big {
		big[i] = 'a'
	}
	sbig := NewScanner(dialect.MYSQL, string(big))
	// also grow internal builder to simulate expansion
	sbig.strBuilder.Grow(1 << 20)
	PutScanner(sbig)

	// Next Get should not necessarily return the same oversized instance; at least, it must be a clean one
	s3 := NewScanner(dialect.MYSQL, "select 3")
	if s3.buf != "select 3" {
		t.Fatalf("unexpected scanner buf after Get")
	}
	PutScanner(s3)
}

func TestExecutableCommentEndSkipsQuotedTerminator(t *testing.T) {
	const sql = "prepare fromx /*! from select 'x*/y' */"
	s := NewScanner(dialect.MYSQL, sql)
	defer PutScanner(s)

	for i := 0; i < 3; i++ {
		if token, _ := s.Scan(); token == LEX_ERROR || token == EofChar() {
			t.Fatalf("unexpected token %d", token)
		}
	}
	s.TakeExecutableCommentEnd()
	for {
		token, _ := s.Scan()
		if end := s.TakeExecutableCommentEnd(); end != 0 {
			if end != len(sql) {
				t.Fatalf("comment end = %d, want %d", end, len(sql))
			}
			return
		}
		if token == LEX_ERROR || token == EofChar() {
			t.Fatal("executable comment terminator not found")
		}
	}
}

func TestPutScannerSmallKeepsBuffers(t *testing.T) {
	// Small SQL should keep buf and builder content when returned to pool
	sql := "select 1"
	s := NewScanner(dialect.MYSQL, sql)
	s.strBuilder.WriteString("xyz")
	PutScanner(s)

	if s.buf == "" {
		t.Fatalf("small scanner buf should not be cleared on PutScanner")
	}
	if s.strBuilder.Len() == 0 {
		t.Fatalf("small scanner strBuilder should retain content on PutScanner")
	}

	// When taking from pool next time, setSql will Reset the builder length
	s2 := NewScanner(dialect.MYSQL, "select 2")
	if s2.strBuilder.Len() != 0 {
		t.Fatalf("builder length must be reset on setSql")
	}
	PutScanner(s2)
}

func TestPutScannerOversizedClearsBuffers(t *testing.T) {
	// Big SQL should be cleared and dropped
	big := make([]byte, (1<<20)+123)
	for i := range big {
		big[i] = 'b'
	}
	s := NewScanner(dialect.MYSQL, string(big))
	s.strBuilder.Grow(1 << 20)
	s.strBuilder.WriteString("payload")
	PutScanner(s)

	if s.buf != "" {
		t.Fatalf("oversized scanner buf should be cleared on PutScanner")
	}
	if s.strBuilder.Len() != 0 {
		t.Fatalf("oversized scanner strBuilder should be zeroed on PutScanner")
	}
}
