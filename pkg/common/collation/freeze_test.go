// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package collation

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type freezeOracle struct {
	Server, Image string
	ParentSHA256  string `json:"parent_sha256"`
	Values        []string
	Order         map[string][]byte
	NativeWeights map[string][]string `json:"native_weights"`
	NativeMapping map[string]string   `json:"native_mapping_sha256"`
}

func loadFreezeOracle(t *testing.T) freezeOracle {
	t.Helper()
	f, err := os.Open("testdata/mysql_8_0_45_extended.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	var o freezeOracle
	if err = json.NewDecoder(z).Decode(&o); err != nil {
		t.Fatal(err)
	}
	parent, err := os.ReadFile("testdata/mysql_8_0_45.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(parent)) != o.ParentSHA256 || o.Server != "8.0.45\taarch64" || o.Image != "mysql@sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff" {
		t.Fatal("oracle provenance mismatch")
	}
	return o
}

func TestFrozenKeysMySQLOracle(t *testing.T) {
	o := loadFreezeOracle(t)
	for name, d := range map[string]Domain{"utf8mb4_general_ci": UTF8MB4GeneralCI, "utf8mb4_bin": UTF8MB4Bin, "utf8mb4_0900_ai_ci": UTF8MB40900AI, "utf8mb4_0900_bin": UTF8MB40900Bin} {
		t.Run(name, func(t *testing.T) {
			n := len(o.Values)
			keys := make([][]byte, n)
			if len(o.Order[name]) != n*n {
				t.Fatal("incomplete oracle")
			}
			for i, v := range o.Values {
				k, err := d.Key(nil, []byte(v))
				if err != nil {
					t.Fatal(err)
				}
				keys[i] = bytes.Clone(k)
				if err = d.ValidateKey(k); err != nil {
					t.Fatal(err)
				}
				if (d == UTF8MB40900AI || d == UTF8MB40900Bin) && len(o.NativeWeights[name]) != n {
					t.Fatal("missing native weight oracle")
				}
				if expected, ok := o.NativeWeights[name]; ok {
					if len(expected) != n || hex.EncodeToString(k) != expected[i] {
						t.Fatalf("sample %d native bytes mismatch", i)
					}
				}
				// Output must not depend on capacity, stale scratch length or earlier rows.
				for _, capacity := range []int{0, 1, 15, 16, 17, len(k), len(k) + 16} {
					scratch := bytes.Repeat([]byte{0xcc}, capacity)
					got, err := d.Key(scratch, []byte(v))
					if err != nil || !bytes.Equal(got, k) {
						t.Fatalf("sample %d capacity %d changed key", i, capacity)
					}
				}
			}
			for i, a := range keys {
				for j, b := range keys {
					if got, want := bytes.Compare(a, b), int(o.Order[name][i*n+j])-1; got != want {
						t.Fatalf("pair %d/%d got %d want %d", i, j, got, want)
					}
				}
			}
			h := sha256.New()
			for i, k := range keys {
				var header [8]byte
				binary.BigEndian.PutUint32(header[:4], uint32(i))
				binary.BigEndian.PutUint32(header[4:], uint32(len(k)))
				h.Write(header[:])
				h.Write(k)
			}
			wantDigest := map[string]string{
				"utf8mb4_bin":        "34fdd70dd433ba2524c5c9a39c7c14dd5ed7c493297d808148a5661d0ea94e06",
				"utf8mb4_general_ci": "11912da320463b858f09a925e2f1ecfc579a8329a6ab7db57da0eed64f514ec5",
				"utf8mb4_0900_ai_ci": "e8bac8dad2189b444e1d740f92da581aed9b080a4b399d0d465e5bfa5d752314",
				"utf8mb4_0900_bin":   "74a39cbc7fb75f7dce7917fb637fb75f165be5bcb5d5774c53fbcc242300597f",
			}[name]
			if fmt.Sprintf("%x", h.Sum(nil)) != wantDigest {
				t.Fatal("V1 byte contract changed; do not regenerate golden hashes without a format-version review")
			}
			t.Logf("PASS independent comparisons=%d payload_sha256=%x", n*n, h.Sum(nil))
		})
	}
}

func TestFrozenNativeScalarMapping(t *testing.T) {
	o := loadFreezeOracle(t)
	for name, d := range map[string]Domain{"utf8mb4_0900_ai_ci": UTF8MB40900AI, "utf8mb4_0900_bin": UTF8MB40900Bin} {
		h := sha256.New()
		check := func(r rune) {
			k, err := d.Key(nil, []byte(string(r)))
			if err != nil {
				t.Fatal(err)
			}
			var header [8]byte
			binary.BigEndian.PutUint32(header[:4], uint32(r))
			binary.BigEndian.PutUint32(header[4:], uint32(len(k)))
			h.Write(header[:])
			h.Write(k)
		}
		for r := rune(0); r < 0x10000; r++ {
			if r < 0xd800 || r > 0xdfff {
				check(r)
			}
		}
		for i := 0; i < 2048; i++ {
			check(rune(0x10000 + i*509))
		}
		if fmt.Sprintf("%x", h.Sum(nil)) != o.NativeMapping[name] {
			t.Fatalf("%s independent scalar mapping mismatch", name)
		}
	}
}

func TestFrozenKeyOwnershipAndAllocations(t *testing.T) {
	for _, d := range []Domain{Raw, UTF8MB4GeneralCI, UTF8MB4Bin, UTF8MB40900AI, UTF8MB40900Bin} {
		for _, s := range []string{"Alpha", strings.Repeat("é😀\ufdfa\x00", 64)} {
			input := []byte(s)
			saved := bytes.Clone(input)
			scratch := make([]byte, 0, 16*len(input)+1)
			var failure error
			// Warm the fixed backend's iterator pool before measuring steady state.
			_, failure = d.Key(scratch, input)
			if failure != nil {
				t.Fatal(failure)
			}
			allocs := testing.AllocsPerRun(100, func() { _, failure = d.Key(scratch, input) })
			if failure != nil || allocs != 0 {
				t.Fatalf("domain %d allocs %v error %v", d, allocs, failure)
			}
			if !bytes.Equal(saved, input) {
				t.Fatal("mutated source")
			}
		}
	}
}

func BenchmarkFrozenKeys(b *testing.B) {
	for _, d := range []Domain{Raw, UTF8MB4GeneralCI, UTF8MB4Bin, UTF8MB40900AI, UTF8MB40900Bin} {
		for _, v := range []struct{ name, value string }{{"ascii", strings.Repeat("Alpha ", 64)}, {"expansion", strings.Repeat("\ufdfaé😀\x00", 64)}} {
			b.Run(fmt.Sprintf("domain%d/%s", d, v.name), func(b *testing.B) {
				input := []byte(v.value)
				scratch := make([]byte, 0, 16*len(input)+1)
				b.ReportAllocs()
				b.SetBytes(int64(len(input)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := d.Key(scratch, input); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestFrozenSizeBounds(t *testing.T) {
	for _, d := range []Domain{Raw, UTF8MB4GeneralCI, UTF8MB4Bin, UTF8MB40900AI, UTF8MB40900Bin} {
		if _, err := d.KeySizeUpperBound(-1); err != ErrSize {
			t.Fatal("negative size accepted")
		}
		for _, s := range []string{"", "Alpha", strings.Repeat("\ufdfaé😀\x00", 1024)} {
			limit, err := d.KeySizeUpperBound(len(s))
			if err != nil {
				t.Fatal(err)
			}
			key, err := d.Key(nil, []byte(s))
			if err != nil || len(key) > limit {
				t.Fatalf("domain %d exceeded bound", d)
			}
		}
	}
	maxInt := int(^uint(0) >> 1)
	for _, d := range []Domain{UTF8MB4GeneralCI, UTF8MB4Bin, UTF8MB40900AI} {
		if _, err := d.KeySizeUpperBound(maxInt); err != ErrSize {
			t.Fatal("overflow accepted")
		}
	}
	if _, err := Domain(255).KeySizeUpperBound(0); err != ErrDomain {
		t.Fatal("unknown domain accepted")
	}
}
