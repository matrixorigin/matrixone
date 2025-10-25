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

func TestScannerPoolCleanupAndThreshold(t *testing.T) {
	// Case 1: normal small SQL should be pooled and fields cleared
	s := NewScanner(dialect.MYSQL, "select 1")
	// grow strBuilder a little to ensure it is cleared on Put
	s.strBuilder.WriteString("abc")
	PutScanner(s)

	// Fetch again to see if we receive a cleared scanner from pool
	s2 := NewScanner(dialect.MYSQL, "select 2")
	if s2.LastToken != "" || s2.LastError != nil || s2.MysqlSpecialComment != nil || s2.Pos != 0 || s2.Line != 0 || s2.Col != 0 || s2.PrePos != 0 {
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
