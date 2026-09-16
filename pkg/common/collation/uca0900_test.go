// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package collation

import (
	"bytes"
	"testing"
)

func TestUCA0900WeightKeysMatchVitessComparator(t *testing.T) {
	values := []string{
		"", " ", "  ", "Alpha", "alpha", "alpha ", "a\x00", "a\tb",
		"é", "e\u0301", "ß", "ss", "中", "😀", "𐐀", "\U0001F600\U0001F642",
	}
	for _, tc := range []struct {
		name   string
		domain Domain
		cmp    func([]byte, []byte) int
	}{
		{name: "0900_ai_ci", domain: UTF8MB40900AI, cmp: UCA0900AICollate},
		{name: "0900_bin", domain: UTF8MB40900Bin, cmp: UCA0900BinCollate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := make([][]byte, len(values))
			for i, value := range values {
				var err error
				keys[i], err = tc.domain.Key(nil, []byte(value))
				if err != nil {
					t.Fatalf("%q: %v", value, err)
				}
				if err := tc.domain.ValidateKey(keys[i]); err != nil {
					t.Fatalf("%q: invalid key: %v", value, err)
				}
			}
			for i, left := range values {
				for j, right := range values {
					got := bytes.Compare(keys[i], keys[j])
					want := sign(tc.cmp([]byte(left), []byte(right)))
					if got != want {
						t.Fatalf("%q vs %q: key=%d comparator=%d (%x vs %x)", left, right, got, want, keys[i], keys[j])
					}
				}
			}
		})
	}
}

func TestUCA0900RejectsInvalidUTF8(t *testing.T) {
	for _, domain := range []Domain{UTF8MB40900AI, UTF8MB40900Bin} {
		if _, err := domain.Key(nil, []byte{0xff}); err != ErrUTF8 {
			t.Fatalf("domain %d accepted invalid UTF-8: %v", domain, err)
		}
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}
