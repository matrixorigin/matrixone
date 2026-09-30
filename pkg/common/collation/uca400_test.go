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

package collation

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
)

type uca400Oracle struct {
	Server, Collation  string
	Values, Weights    []string
	Order              []int
	ScalarSHA256       string   `json:"scalar_sha256"`
	ScalarWeights      []string `json:"scalar_weights"`
	MB3VerifiedSamples int      `json:"mb3_verified_samples"`
}

func readUCA400Oracle(t *testing.T) uca400Oracle {
	t.Helper()
	f, err := os.Open("testdata/mysql_8_4_11_uca400.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	b, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	var o uca400Oracle
	if err := json.Unmarshal(b, &o); err != nil {
		t.Fatal(err)
	}
	if o.Server != "8.4.11\tarm64" || o.Collation != "utf8mb4_unicode_ci" ||
		len(o.Values) == 0 || len(o.Weights) != len(o.Values) || len(o.Order) != len(o.Values)*len(o.Values) {
		t.Fatal("incomplete or unexpected oracle")
	}
	return o
}

func TestUCA400MySQLOracle(t *testing.T) {
	o := readUCA400Oracle(t)
	if uca400 == nil || !bytes.Equal(uca400.WeightString(nil, []byte(" "), 0), []byte{2, 9}) {
		t.Fatal("pinned SPACE weight changed")
	}
	keys := make([][]byte, len(o.Values))
	h := sha256.New()
	bmpSamples := 0
	for i, s := range o.Values {
		v := []byte(s)
		if got := hex.EncodeToString(uca400Weights(nil, v)); got != o.Weights[i] {
			// HEX() returns uppercase; decode instead of case-folding Unicode.
			want, err := hex.DecodeString(o.Weights[i])
			if err != nil || !bytes.Equal(uca400Weights(nil, v), want) {
				t.Fatalf("raw weight %d: %s != %s", i, got, o.Weights[i])
			}
		}
		k, err := UTF8MB4UnicodeCI.Key(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = k
		var header [8]byte
		binary.BigEndian.PutUint32(header[:4], uint32(i))
		binary.BigEndian.PutUint32(header[4:], uint32(len(k)))
		h.Write(header[:])
		h.Write(k)
		k3, e3 := UTF8MB3UnicodeCI.Key(nil, v)
		if e3 == nil {
			bmpSamples++
			if !bytes.Equal(k3, k) {
				t.Fatal("sample BMP mismatch")
			}
		} else if e3 != ErrRepertoire {
			t.Fatal(e3)
		}
		if err := UTF8MB4UnicodeCI.ValidateKey(k); err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
		bound, _ := UTF8MB4UnicodeCI.KeySizeUpperBound(len(v))
		if len(k) > bound {
			t.Fatal("size bound")
		}
		for _, n := range []int{0, 1, 7, len(k), len(k) + 10} {
			got, err := UTF8MB4UnicodeCI.Key(bytes.Repeat([]byte{0xcc}, n), v)
			if err != nil || !bytes.Equal(k, got) {
				t.Fatalf("scratch dependence %d/%d", i, n)
			}
		}
	}
	for i, a := range keys {
		for j, b := range keys {
			if got, want := bytes.Compare(a, b), o.Order[i*len(keys)+j]; got != want {
				t.Fatalf("%q vs %q: got %d want %d", o.Values[i], o.Values[j], got, want)
			}
		}
	}
	t.Logf("MySQL oracle: %d samples, %d ordered pairs", len(keys), len(o.Order))
	if bmpSamples != o.MB3VerifiedSamples {
		t.Fatal("missing mb3 oracle")
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != "531cb4c2ef87a3b1367ce716bdb800e26ccffd760b15ea3f35988efda9eaec14" {
		t.Fatal("U4P1 byte contract changed; do not regenerate without format review")
	}
}

func TestUCA400ScalarMapping(t *testing.T) {
	o := readUCA400Oracle(t)
	if len(o.ScalarWeights) != 0x10000-0x800+2048 {
		t.Fatal("incomplete scalar fixture")
	}
	h := sha256.New()
	idx := 0
	check := func(cp rune) {
		v := []byte(string(cp))
		w := uca400Weights(nil, v)
		want, _ := hex.DecodeString(o.ScalarWeights[idx])
		idx++
		if !bytes.Equal(w, want) {
			t.Errorf("scalar U+%04X: %x != %x", cp, w, want)
		}
		var header [8]byte
		binary.BigEndian.PutUint32(header[:4], uint32(cp))
		binary.BigEndian.PutUint32(header[4:], uint32(len(w)))
		h.Write(header[:])
		h.Write(w)
	}
	for cp := rune(0); cp < 0x10000; cp++ {
		if cp < 0xd800 || cp > 0xdfff {
			check(cp)
		}
	}
	for i := rune(0); i < 2048; i++ {
		check(0x10000 + i*509)
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != o.ScalarSHA256 {
		t.Fatalf("scalar oracle %s != %s", got, o.ScalarSHA256)
	}
	// Exhaust the full scalar range to defend the allocation bound and mb3
	// repertoire, not just the sampled MySQL mapping. ID224 has no contractions.
	for cp := rune(0); cp <= 0x10ffff; cp++ {
		if cp >= 0xd800 && cp <= 0xdfff {
			continue
		}
		v := []byte(string(cp))
		w := uca400Weights(nil, v)
		if len(w)%2 != 0 || len(w) > 36 {
			t.Fatalf("weight bound U+%04X: %d", cp, len(w))
		}
		k, err := UTF8MB3UnicodeCI.Key(nil, v)
		if cp > 0xffff {
			if err != ErrRepertoire {
				t.Fatalf("accepted supplementary U+%X", cp)
			}
			continue
		}
		k4, e4 := UTF8MB4UnicodeCI.Key(nil, v)
		if err != nil || e4 != nil || !bytes.Equal(k, k4) {
			t.Fatalf("BMP domain mismatch U+%X", cp)
		}
	}
}

func TestUCA400Errors(t *testing.T) {
	for _, d := range []Domain{UTF8MB4UnicodeCI, UTF8MB3UnicodeCI} {
		for _, n := range []int{-1, math.MaxInt, (math.MaxInt-1)/54 + 1} {
			if _, err := d.KeySizeUpperBound(n); err != ErrSize {
				t.Fatalf("size %d: %v", n, err)
			}
		}
		for _, v := range [][]byte{{0xff}, {0xc0, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}} {
			s := []byte{1, 2, 3}
			_, err := d.Key(s, v)
			if err != ErrUTF8 || !bytes.Equal(s, []byte{1, 2, 3}) {
				t.Fatal("invalid UTF8 modified scratch")
			}
		}
		for _, k := range [][]byte{nil, {}, {0x20, 0x20}, {0x19, 0x20}, {0x29, 0x20}, {0x19, 0x29, 0x30, 3, 0, 0x20},
			{0x10}, {0x10, 1}, {0x10, 0, 0, 0x20}, {0x10, 2, 9, 0x20}, {0x30, 2, 8, 0x20},
			{0x10, 3, 0, 0x20}, {0x19, 0x30, 3, 0, 0x20}, {0x29, 0x10, 1, 0, 0x20}, {0xff}} {
			if d.ValidateKey(k) != ErrKey {
				t.Fatalf("accepted malformed key %x", k)
			}
		}
	}
	s := []byte{1, 2, 3}
	_, err := UTF8MB3UnicodeCI.Key(s, []byte("😀"))
	if err != ErrRepertoire || !bytes.Equal(s, []byte{1, 2, 3}) {
		t.Fatal("mb3 error modified scratch")
	}
}

func BenchmarkUCA400Key(b *testing.B) {
	for name, v := range map[string][]byte{"ascii": []byte("User.name@example.com"), "expansion": []byte("Straße æther ﬀ"), "spaces": []byte(" a  b   c ")} {
		b.Run(name, func(b *testing.B) {
			var scratch []byte
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				scratch, _ = UTF8MB4UnicodeCI.Key(scratch, v)
			}
		})
	}
}
