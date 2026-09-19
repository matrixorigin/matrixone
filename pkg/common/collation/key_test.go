// Copyright 2026 Matrix Origin
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

package collation

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestMySQL8045Oracle(t *testing.T) {
	f, err := os.Open("testdata/mysql_8_0_45.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	var oracle struct {
		Server, Image       string
		Values              []string
		Order               map[string][]byte
		GeneralWeightSHA256 string `json:"general_weight_sha256"`
		BinWeightSHA256     string `json:"bin_weight_sha256"`
	}
	if err := json.NewDecoder(gz).Decode(&oracle); err != nil {
		t.Fatal(err)
	}
	if oracle.Server != "8.0.45\taarch64" || oracle.Image != "mysql@sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff" {
		t.Fatal("unexpected oracle provenance")
	}
	for name, domain := range map[string]Domain{"utf8mb4_bin": UTF8MB4Bin, "utf8mb4_general_ci": UTF8MB4GeneralCI} {
		t.Run(name, func(t *testing.T) {
			keys := make([][]byte, len(oracle.Values))
			for i, v := range oracle.Values {
				keys[i], err = domain.Key(nil, []byte(v))
				if err != nil {
					t.Fatal(err)
				}
				if err := domain.ValidateKey(keys[i]); err != nil {
					t.Fatal(err)
				}
			}
			if len(oracle.Order[name]) != len(keys)*len(keys) {
				t.Fatal("incomplete oracle")
			}
			for i, a := range keys {
				for j, b := range keys {
					want := int(oracle.Order[name][i*len(keys)+j]) - 1
					if got := bytes.Compare(a, b); got != want {
						t.Fatalf("%q vs %q: got %d, MySQL %d", oracle.Values[i], oracle.Values[j], got, want)
					}
				}
			}
		})
	}
	// The native UCA 9.0 key is the stage-A deliverable. Compare the fixed
	// Vitess adapter against the independent MySQL 8.0.45 ordering corpus,
	// rather than only comparing it with its own comparator.
	domain := UTF8MB40900AI
	keys := make([][]byte, len(oracle.Values))
	for i, value := range oracle.Values {
		keys[i], err = domain.Key(nil, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, left := range keys {
		for j, right := range keys {
			want := int(oracle.Order["utf8mb4_0900_ai_ci"][i*len(keys)+j]) - 1
			if got := bytes.Compare(left, right); got != want {
				t.Fatalf("0900 %q vs %q: got %d, MySQL %d", oracle.Values[i], oracle.Values[j], got, want)
			}
		}
	}
	// WEIGHT_STRING is an independent mapping oracle, not a variable-length
	// PAD SPACE comparison key. Compare every valid BMP character plus 2,048
	// supplementary samples in the exact order queried from the server.
	g, b := sha256.New(), sha256.New()
	check := func(r rune) {
		w := utf8mb4GeneralCIWeight(r)
		g.Write([]byte{byte(w >> 8), byte(w)})
		b.Write([]byte{byte(r >> 16), byte(r >> 8), byte(r)})
		for _, domain := range []Domain{UTF8MB4Bin, UTF8MB4GeneralCI} {
			key, err := domain.Key(nil, []byte(string(r)))
			if err != nil || domain.ValidateKey(key) != nil || bytes.IndexByte(key, 0) >= 0 {
				t.Fatalf("U+%04X produced invalid or zero-containing key %x", r, key)
			}
		}
	}
	for r := rune(0); r <= 0xffff; r++ {
		if r < 0xd800 || r > 0xdfff {
			check(r)
		}
	}
	for i := 0; i < 2048; i++ {
		check(rune(0x10000 + i*509))
	}
	if fmt.Sprintf("%x", g.Sum(nil)) != oracle.GeneralWeightSHA256 || fmt.Sprintf("%x", b.Sum(nil)) != oracle.BinWeightSHA256 {
		t.Fatal("native MySQL weight mapping mismatch")
	}
	if bytes.Equal(oracle.Order["utf8mb4_general_ci"], oracle.Order["utf8mb4_0900_ai_ci"]) {
		t.Fatal("oracle must distinguish native 0900 from MO's general-ci alias")
	}
}

func TestPADKeyExhaustive(t *testing.T) {
	// Independent comparator explicitly pads the shorter value with SPACE.
	// Enumerating all words captures differing interior space runs, controls,
	// empty values and trailing padding; it does not compare the codec to itself.
	words := []string{""}
	level := []string{""}
	for n := 0; n < 4; n++ {
		next := []string{}
		for _, s := range level {
			for _, r := range []rune{0, 31, 32, 33, 0xffff} {
				next = append(next, s+string(r))
			}
		}
		words = append(words, next...)
		level = next
	}
	keys := make([][]byte, len(words))
	runes := make([][]rune, len(words))
	for i, s := range words {
		keys[i], _ = UTF8MB4Bin.Key(nil, []byte(s))
		runes[i] = []rune(s)
	}
	for i, a := range runes {
		for j, b := range runes {
			want := 0
			for k := 0; k < max(len(a), len(b)); k++ {
				wa, wb := rune(32), rune(32)
				if k < len(a) {
					wa = a[k]
				}
				if k < len(b) {
					wb = b[k]
				}
				if wa < wb {
					want = -1
					break
				}
				if wa > wb {
					want = 1
					break
				}
			}
			if got := bytes.Compare(keys[i], keys[j]); got != want {
				t.Fatalf("%q vs %q: %d != %d", words[i], words[j], got, want)
			}
		}
	}
}

func TestKeyGoldenAndAdmission(t *testing.T) {
	for _, tc := range []struct{ s, hex string }{
		{"", "22"}, {" ", "22"}, {"\x00", "0122"},
		{"a", "4422"}, {"A ", "4422"},
		{"a \x00", "44210122"},
		{"a b", "44234522"},
	} {
		k, err := UTF8MB4GeneralCI.Key(nil, []byte(tc.s))
		if err != nil || fmt.Sprintf("%x", k) != tc.hex {
			t.Fatalf("%q: %x %v", tc.s, k, err)
		}
	}
	raw := []byte{0xff, 0, 0x80}
	k, err := Raw.Key(nil, raw)
	if err != nil || &k[0] != &raw[0] {
		t.Fatal("raw path must borrow unchanged bytes")
	}
	for _, d := range []Domain{UTF8MB4Bin, UTF8MB4GeneralCI, 99} {
		scratch := []byte("untouched")
		_, err := d.Key(scratch, raw)
		if err == nil || string(scratch) != "untouched" {
			t.Fatal("invalid input changed scratch or was accepted")
		}
	}
	for _, k := range [][]byte{nil, {0x22, 0}, {0x21, 0x22}, {0xc2}, {0}, {0x21, 0x44, 0x22}, {0x23, 1, 0x22}, {0xf4, 0x90, 0x80, 0x80, 0x22}, {0xed, 0xa0, 0x80, 0x22}, {0x83, 0x22}} {
		if UTF8MB4Bin.ValidateKey(k) == nil {
			t.Fatalf("accepted malformed key %x", k)
		}
	}
}

func FuzzKey(f *testing.F) {
	for _, s := range []string{"", "Alpha ", "a \x00", "中😀", "\xff"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, v []byte) {
		for _, d := range []Domain{Raw, UTF8MB4Bin, UTF8MB4GeneralCI, UTF8MB40900AI, UTF8MB40900Bin} {
			k, err := d.Key(nil, v)
			if err == nil && d.ValidateKey(k) != nil {
				t.Fatalf("invalid generated key %x", k)
			}
			_ = d.ValidateKey(v)
		}
	})
}

func BenchmarkKey(b *testing.B) {
	for _, n := range []int{16, 128, 4096} {
		for _, d := range []Domain{Raw, UTF8MB4Bin, UTF8MB4GeneralCI} {
			b.Run(fmt.Sprintf("domain%d/bytes%d", d, n), func(b *testing.B) {
				v := []byte(strings.Repeat("a", n))
				scratch := make([]byte, 0, 4*n+1)
				b.ReportAllocs()
				b.SetBytes(int64(n))
				for i := 0; i < b.N; i++ {
					if _, err := d.Key(scratch, v); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
