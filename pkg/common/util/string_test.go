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

package util

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/smartystreets/goconvey/convey"
)

func TestSubStringFromBegin(t *testing.T) {
	convey.Convey("SubStringFromBegin", t, func() {
		// Test normal truncation
		convey.So(Abbreviate("abcdef", 3), convey.ShouldEqual, "abc...")

		// Test string shorter than length
		convey.So(Abbreviate("abc", 5), convey.ShouldEqual, "abc")

		// Test exact length match
		convey.So(Abbreviate("abc", 3), convey.ShouldEqual, "abc")

		// Test empty string
		convey.So(Abbreviate("", 5), convey.ShouldEqual, "")

		// Test length 0
		convey.So(Abbreviate("abcdef", 0), convey.ShouldEqual, "")

		// Test length -1 (return complete string)
		convey.So(Abbreviate("abcdef", -1), convey.ShouldEqual, "abcdef")

		// Test length < -1
		convey.So(Abbreviate("abcdef", -2), convey.ShouldEqual, "")

		// Test long string
		longStr := "a" + string(make([]byte, 2000))
		result := Abbreviate(longStr, 1024)
		convey.So(len(result), convey.ShouldEqual, 1027) // 1024 + "..."
		convey.So(result[:1024], convey.ShouldEqual, longStr[:1024])
		convey.So(result[1024:], convey.ShouldEqual, "...")
	})
}

func TestAbbreviateMalformedUTF8(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"\xff", "?"},
		{"\x80\xbf", "??"},
		{"\xc0\xaf", "??"},
		{"\xed\xa0\x80", "???"},
		{"\xf4\x90\x80\x80", "????"},
		{"\xe4\xbd", "??"},
		{"\xf0\x9f\x98", "???"},
		{"a你\xff😀�\xe4\xbdz", "a你?😀�??z"},
		{"\x00\t\n\r\x7f你�", "\x00\t\n\r\x7f你�"},
	} {
		for budget := -2; budget <= len(tc.raw)+1; budget++ {
			want := tc.want
			if budget == 0 || budget < -1 {
				want = ""
			} else if budget > 0 && len(tc.raw) > budget {
				end := 0
				for offset, r := range tc.want {
					if offset+utf8.RuneLen(r) > budget {
						break
					}
					end = offset + utf8.RuneLen(r)
				}
				want = tc.want[:end] + "..."
			}
			got := Abbreviate(tc.raw, budget)
			if got != want || !utf8.ValidString(got) {
				t.Fatalf("raw %x budget %d: got %q want %q", tc.raw, budget, got, want)
			}
		}
	}
	// An invalid suffix is discarded without changing the retained text.
	if got := Abbreviate("你"+strings.Repeat("\xff", 8192), 3); got != "你..." {
		t.Fatalf("discarded suffix: %q", got)
	}
}

func TestUTF8PrefixLen(t *testing.T) {
	for _, text := range []string{"", "abcdef", "a¢你😀z", "你你你", "a😀😀", "�x"} {
		for budget := 0; budget <= len(text)+1; budget++ {
			want := 0
			for offset, r := range text {
				end := offset + utf8.RuneLen(r)
				if end > budget {
					break
				}
				want = end
			}
			got := UTF8PrefixLen(text, budget)
			if got != want {
				t.Fatalf("%q budget %d: got %d want %d", text, budget, got, want)
			}
			if !utf8.ValidString(text[:got]) {
				t.Fatalf("invalid prefix: %q", text[:got])
			}
			abbreviated := Abbreviate(text, budget)
			expected := text[:want]
			if budget > 0 && budget < len(text) {
				expected += "..."
			}
			if abbreviated != expected {
				t.Fatalf("abbreviation %q budget %d: got %q want %q", text, budget, abbreviated, expected)
			}
		}
	}
	for _, tc := range []struct {
		text         string
		budget, want int
	}{
		{"abc", -1, 0}, {"\xffx", 1, 1}, {"a\x80\x80z", 2, 2}, {"\xf0\x80\x80\x80z", 2, 2},
	} {
		if got := UTF8PrefixLen(tc.text, tc.budget); got != tc.want {
			t.Errorf("malformed/sentinel %q: got %d want %d", tc.text, got, tc.want)
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = Abbreviate("a你😀", 100); _ = UTF8PrefixLen("a你😀", 3) }); n != 0 {
		t.Fatalf("boundary/unchanged path allocated: %v", n)
	}
}
