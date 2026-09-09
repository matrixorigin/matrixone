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

package collationkey

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestLocatorRoundTripCopiesPrimaryKey(t *testing.T) {
	primaryKey := []byte("Alpha\x00")
	encoded, err := EncodeLocator([]byte("prefix"), RowLocator{
		RelationID:  17,
		PartitionID: 23,
		PrimaryKey:  primaryKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:6], []byte("prefix")) {
		t.Fatalf("destination prefix was not preserved: %X", encoded)
	}
	decoded, err := DecodeLocator(encoded[6:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RelationID != 17 || decoded.PartitionID != 23 || !bytes.Equal(decoded.PrimaryKey, primaryKey) {
		t.Fatalf("decoded locator = %+v", decoded)
	}
	primaryKey[0] = 'x'
	if decoded.PrimaryKey[0] != 'A' {
		t.Fatal("decoded primary key aliases caller input")
	}
}

func TestLocatorAllowsEmptyPrimaryKey(t *testing.T) {
	encoded, err := EncodeLocator(nil, RowLocator{RelationID: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := "4D4F4B4C010000000000000001000000000000000000000000"
	if got := fmtHex(encoded); got != want {
		t.Fatalf("encoded empty locator = %s, want %s", got, want)
	}
	decoded, err := DecodeLocator(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.PrimaryKey) != 0 {
		t.Fatalf("decoded empty key has length %d", len(decoded.PrimaryKey))
	}
}

func TestLocatorRejectsMalformedInput(t *testing.T) {
	valid, err := EncodeLocator(nil, RowLocator{RelationID: 1, PrimaryKey: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"truncated", valid[:len(valid)-1]},
		{"magic", append([]byte("NOPE"), valid[4:]...)},
		{"version", func() []byte { out := append([]byte(nil), valid...); out[4] = 2; return out }()},
		{"relation zero", func() []byte {
			out := append([]byte(nil), valid...)
			for i := 5; i < 13; i++ {
				out[i] = 0
			}
			return out
		}()},
		{"length mismatch", func() []byte {
			out := append([]byte(nil), valid...)
			binary.BigEndian.PutUint32(out[21:25], 2)
			return out
		}()},
		{"trailing bytes", append(append([]byte(nil), valid...), 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeLocator(test.data); !errors.Is(err, ErrMalformedKey) {
				t.Fatalf("error = %v, want ErrMalformedKey", err)
			}
		})
	}
	if _, err := EncodeLocator(nil, RowLocator{}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("zero relation encode error = %v, want ErrInvalidValue", err)
	}
}

func TestLocatorRejectsOversizedKeyAndDestination(t *testing.T) {
	tooLarge := make([]byte, MaxKeyBytes-locatorHeader+1)
	if _, err := EncodeLocator(nil, RowLocator{RelationID: 1, PrimaryKey: tooLarge}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("oversized key error = %v, want ErrInvalidValue", err)
	}
	if _, err := EncodeLocator(make([]byte, MaxKeyBytes-locatorHeader+1), RowLocator{RelationID: 1}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("oversized destination error = %v, want ErrInvalidValue", err)
	}
}

func fmtHex(data []byte) string {
	const hex = "0123456789ABCDEF"
	out := make([]byte, len(data)*2)
	for i, b := range data {
		out[2*i], out[2*i+1] = hex[b>>4], hex[b&0x0F]
	}
	return string(out)
}
