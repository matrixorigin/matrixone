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
	}}

	for _, tcase := range testcases {
		id, got := NewScanner(dialect.MYSQL, tcase.in).Scan()
		if tcase.id != id || string(got) != tcase.want {
			t.Errorf("Scan(%q) = (%s, %q), want (%s, %q)", tcase.in, tokenName(id), got, tokenName(tcase.id), tcase.want)
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
