// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collationkey

import (
	"bytes"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
)

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	value = stringWithoutSpaces(value)
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode golden vector: %v", err)
	}
	return decoded
}

func stringWithoutSpaces(value string) string {
	result := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] != ' ' && value[i] != '\n' && value[i] != '\t' {
			result = append(result, value[i])
		}
	}
	return string(result)
}

func generalDomain(prefix uint32) Domain {
	return Domain{Type: Text, Charset: CharsetUTF8, Prefix: prefix, Unit: PrefixCharacters}
}

func binTextDomain(prefix uint32) Domain {
	return Domain{Type: Text, Charset: CharsetUTF8MB4Bin, Prefix: prefix, Unit: PrefixCharacters}
}

func binaryDomain(prefix uint32) Domain {
	return Domain{Type: Binary, Charset: CharsetBinary, Prefix: prefix, Unit: PrefixBytes}
}

func TestEncodeNormativeVectors(t *testing.T) {
	tests := []struct {
		name string
		part Part
		want string
	}{
		{
			name: "general-ci Alpha",
			part: Part{Domain: generalDomain(0), Value: []byte("Alpha")},
			want: "4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000014 00000041 0000004C 00000050 00000048 00000041",
		},
		{
			name: "general-ci embedded NUL",
			part: Part{Domain: generalDomain(0), Value: []byte("A\x00")},
			want: "4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000008 00000041 00000000",
		},
		{
			name: "general-ci supplementary",
			part: Part{Domain: generalDomain(0), Value: []byte("😀")},
			want: "4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000004 0000FFFD",
		},
		{
			name: "utf8-bin Alpha",
			part: Part{Domain: binTextDomain(0), Value: []byte("Alpha")},
			want: "4D4F4B59 02 0001 00 01 0002 0006 010000000001 00000005 416C706861",
		},
		{
			name: "binary trailing space",
			part: Part{Domain: binaryDomain(0), Value: []byte("Alpha ")},
			want: "4D4F4B59 02 0001 00 02 0003 0006 010000000002 00000006 416C70686120",
		},
		{
			name: "NULL",
			part: Part{Domain: generalDomain(0), Null: true, Value: []byte("ignored")},
			want: "4D4F4B59 02 0001 01 01 0001 0006 010000000001 00000000",
		},
		{
			name: "empty non-NULL",
			part: Part{Domain: generalDomain(0)},
			want: "4D4F4B59 02 0001 00 01 0001 0006 010000000001 00000000",
		},
		{
			name: "composite signed and text",
			part: Part{Domain: Domain{Type: SignedInteger, Width: 8}, Value: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
			want: "4D4F4B59 02 0001 00 03 0101 0003 010008 00000008 0000000000000001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "composite signed and text" {
				encoded, err := EncodeComposite(nil, []Part{
					tt.part,
					{Domain: generalDomain(0), Value: []byte("A")},
				})
				if err != nil {
					t.Fatal(err)
				}
				want := mustDecodeHex(t, "4D4F4B59 02 0002 00 03 0101 0003 010008 00000008 0000000000000001 00 01 0001 0006 010000000001 00000004 00000041")
				if !bytes.Equal(encoded, want) {
					t.Fatalf("encoded=%X want=%X", encoded, want)
				}
				if err := ValidateEncoded(encoded); err != nil {
					t.Fatalf("ValidateEncoded: %v", err)
				}
				return
			}
			encoded, err := EncodePart(nil, tt.part)
			if err != nil {
				t.Fatal(err)
			}
			want := mustDecodeHex(t, tt.want)
			if !bytes.Equal(encoded, want) {
				t.Fatalf("encoded=%X want=%X", encoded, want)
			}
			if err := ValidateEncoded(encoded); err != nil {
				t.Fatalf("ValidateEncoded: %v", err)
			}
		})
	}
}

func TestEqualUsesSameNormalizationAsEncoding(t *testing.T) {
	domain := generalDomain(0)
	for _, pair := range [][2]string{{"Alpha", "alpha"}, {"Alpha", "Alpha "}, {"É", "é"}} {
		equal, err := Equal(domain, []byte(pair[0]), []byte(pair[1]))
		if err != nil || !equal {
			t.Fatalf("Equal(%q,%q) = %v, %v", pair[0], pair[1], equal, err)
		}
		a, err := EncodePart(nil, Part{Domain: domain, Value: []byte(pair[0])})
		if err != nil {
			t.Fatal(err)
		}
		b, err := EncodePart(nil, Part{Domain: domain, Value: []byte(pair[1])})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("equal values have different keys: %X != %X", a, b)
		}
	}
	equal, err := Equal(domain, []byte("a"), []byte("b"))
	if err != nil || equal {
		t.Fatalf("unequal values reported equal: %v, %v", equal, err)
	}
	if equal, err := Equal(binTextDomain(0), []byte("Alpha"), []byte("alpha")); err != nil || equal {
		t.Fatalf("_bin values reported equal: %v, %v", equal, err)
	}
	if equal, err := Equal(binaryDomain(0), []byte("Alpha"), []byte("Alpha ")); err != nil || equal {
		t.Fatalf("binary PAD values reported equal: %v, %v", equal, err)
	}
}

func TestPrefixAndNullBoundaries(t *testing.T) {
	domain := generalDomain(2)
	a, err := EncodePart(nil, Part{Domain: domain, Value: []byte("A😀tail")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodePart(nil, Part{Domain: domain, Value: []byte("a😀other")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("character prefix was not applied before normalization")
	}
	if _, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte{0xff}}); err == nil {
		t.Fatal("invalid UTF-8 was accepted")
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: Text, Charset: CharsetLegacy, Unit: PrefixCharacters}, Value: []byte("a")}); err == nil {
		t.Fatal("legacy text was silently enabled as v2")
	}
}

func TestNumericAndDecimalBoundaries(t *testing.T) {
	integer := Domain{Type: SignedInteger, Width: 2}
	if _, err := EncodePart(nil, Part{Domain: integer, Value: []byte{1}}); err == nil {
		t.Fatal("short integer was accepted")
	}
	decimal := Domain{Type: Decimal, Width: 8, Scale: 2}
	a, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("1.20")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("1.2")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("decimal equivalent values differ: %X != %X", a, b)
	}
	if _, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("1.234")}); err == nil {
		t.Fatal("decimal precision overflow was accepted")
	}
	negative, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("-1.20")})
	if err != nil {
		t.Fatal(err)
	}
	negativeEquivalent, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("-1.2")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(negative, negativeEquivalent) {
		t.Fatalf("negative decimal equivalents differ: %X != %X", negative, negativeEquivalent)
	}
	zero, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("-0.00")})
	if err != nil {
		t.Fatal(err)
	}
	positiveZero, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("0")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(zero, positiveZero) {
		t.Fatalf("zero signs/scales were not canonicalized: %X != %X", zero, positiveZero)
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: SignedInteger, Width: 33}, Value: make([]byte, 33)}); err == nil {
		t.Fatal("oversized integer domain was accepted")
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: Decimal, Width: 8, Scale: -1}, Value: []byte("1")}); !errors.Is(err, ErrUnsupportedDomain) {
		t.Fatalf("negative decimal scale error = %v", err)
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixBytes}, Value: []byte("a")}); err == nil {
		t.Fatal("text byte prefix domain was accepted")
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters, Collation: 1}, Value: []byte("a")}); err == nil {
		t.Fatal("unregistered explicit collation was accepted")
	}
}

func TestHasNullPartDistinguishesNullableAndEmptyKeys(t *testing.T) {
	withNull, err := EncodeComposite(nil, []Part{
		{Domain: generalDomain(0), Null: true},
		{Domain: generalDomain(0), Value: []byte("a")},
	})
	if err != nil {
		t.Fatal(err)
	}
	hasNull, err := HasNullPart(withNull)
	if err != nil || !hasNull {
		t.Fatalf("HasNullPart(nullable) = %v, %v", hasNull, err)
	}
	empty, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	hasNull, err = HasNullPart(empty)
	if err != nil || hasNull {
		t.Fatalf("HasNullPart(empty) = %v, %v", hasNull, err)
	}
	if _, err := HasNullPart([]byte("not-a-key")); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("malformed HasNullPart error = %v", err)
	}
}

func TestValidateEncodedRejectsMalformedInput(t *testing.T) {
	valid, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte("Alpha")})
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{
		valid[:6],
		append([]byte("NOPE"), valid[4:]...),
		append(append([]byte(nil), valid...), 0),
	} {
		if err := ValidateEncoded(malformed); err == nil {
			t.Fatalf("malformed key accepted: %X", malformed)
		}
	}
	badFlag := append([]byte(nil), valid...)
	badFlag[7] = 2
	if err := ValidateEncoded(badFlag); err == nil {
		t.Fatal("invalid NULL flag accepted")
	}
	badPayload := append([]byte(nil), valid...)
	badPayload[19] = 0xff
	if err := ValidateEncoded(badPayload); err == nil {
		t.Fatal("invalid payload length accepted")
	}
	badFamily := append([]byte(nil), valid...)
	badFamily[9] = 0x7f
	if err := ValidateEncoded(badFamily); err == nil {
		t.Fatal("unknown registry family accepted")
	}
	badType := append([]byte(nil), valid...)
	badType[8] = byte(Binary)
	if err := ValidateEncoded(badType); err == nil {
		t.Fatal("registry type mismatch accepted")
	}
	badParams := append([]byte(nil), valid...)
	badParams[13] = 2
	if err := ValidateEncoded(badParams); err == nil {
		t.Fatal("non-canonical parameters accepted")
	}
	badWeight := append([]byte(nil), valid...)
	badWeight[len(badWeight)-4] = 1
	if err := ValidateEncoded(badWeight); err == nil {
		t.Fatal("non-canonical general-ci weight accepted")
	}
	badNullPayload := append([]byte(nil), valid...)
	badNullPayload[7] = 1
	if err := ValidateEncoded(badNullPayload); err == nil {
		t.Fatal("NULL part with payload accepted")
	}
	if _, err := EncodeComposite(nil, make([]Part, MaxParts+1)); err == nil {
		t.Fatal("too many parts accepted")
	}
	if _, err := EncodeComposite(make([]byte, MaxKeyBytes-1), []Part{{Domain: generalDomain(0), Value: []byte("a")}}); err == nil {
		t.Fatal("key exceeding maximum size accepted")
	}
	validDecimal, err := EncodePart(nil, Part{Domain: Domain{Type: Decimal, Width: 8, Scale: 2}, Value: []byte("1.20")})
	if err != nil {
		t.Fatal(err)
	}
	badDecimalSign := append([]byte(nil), validDecimal...)
	badDecimalSign[22] = 2
	if err := ValidateEncoded(badDecimalSign); err == nil {
		t.Fatal("invalid decimal sign accepted")
	}
	badDecimalScale := append([]byte(nil), validDecimal...)
	badDecimalScale[23] = 0xff
	if err := ValidateEncoded(badDecimalScale); err == nil {
		t.Fatal("out-of-range decimal scale accepted")
	}
	badDecimalCoeff := append([]byte(nil), validDecimal...)
	badDecimalCoeff[31] = 0
	if err := ValidateEncoded(badDecimalCoeff); err == nil {
		t.Fatal("non-minimal decimal coefficient accepted")
	}
}

func TestRegistryDigestIsStableAndCopied(t *testing.T) {
	first := RegistryDigest()
	if len(first) != 32 {
		t.Fatalf("digest length=%d", len(first))
	}
	first[0]++
	second := RegistryDigest()
	if bytes.Equal(first, second) {
		t.Fatal("RegistryDigest exposed mutable storage")
	}
	if !bytes.Equal(second, RegistryDigest()) {
		t.Fatal("registry digest is not stable")
	}
	if got, want := hex.EncodeToString(second), "c69a5959e49d4fb193d8c8252e76eb5ea92e53be9594354d4e8befd4575db41a"; got != want {
		t.Fatalf("registry digest=%s want=%s", got, want)
	}
}

func TestConcurrentEncodingIsDeterministic(t *testing.T) {
	part := Part{Domain: generalDomain(0), Value: []byte("Éclair 😀")}
	want, err := EncodePart(nil, part)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				got, err := EncodePart(nil, part)
				if err != nil || !bytes.Equal(got, want) {
					t.Errorf("concurrent encoding mismatch: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	copyOfWant := append([]byte(nil), want...)
	if HashEncoded(want) != HashEncoded(copyOfWant) {
		t.Fatal("hash of identical encoded keys changed")
	}
}

func FuzzEncodeRejectsMalformedOrReturnsValid(f *testing.F) {
	f.Add([]byte("Alpha"))
	f.Add([]byte{0xff, 0x00, 0x01})
	f.Add([]byte("😀 tail"))
	domain := generalDomain(0)
	f.Fuzz(func(t *testing.T, value []byte) {
		encoded, err := EncodePart(nil, Part{Domain: domain, Value: value})
		if err != nil {
			return
		}
		if err := ValidateEncoded(encoded); err != nil {
			t.Fatalf("encoder produced invalid key: %v", err)
		}
	})
}

func BenchmarkEncodeGeneralCI(b *testing.B) {
	part := Part{Domain: generalDomain(0), Value: []byte("The Quick Brown Fox jumps over the lazy dog")}
	for i := 0; i < b.N; i++ {
		if _, err := EncodePart(nil, part); err != nil {
			b.Fatal(err)
		}
	}
}
