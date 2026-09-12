// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

func sidecarTestKey(t *testing.T) []byte {
	t.Helper()
	key, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
		Value:  []byte("Alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestSidecarEntryRoundTripCopiesBytes(t *testing.T) {
	key := sidecarTestKey(t)
	encoded, err := EncodeSidecarEntry([]byte("prefix"), SidecarEntry{
		Key: key,
		Locator: RowLocator{
			RelationID:  42,
			PartitionID: 7,
			PrimaryKey:  []byte("pk\x00"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len("prefix")], []byte("prefix")) {
		t.Fatalf("destination prefix was not preserved: %X", encoded)
	}
	decoded, err := DecodeSidecarEntry(encoded[len("prefix"):])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Key, key) || decoded.Locator.RelationID != 42 ||
		decoded.Locator.PartitionID != 7 || !bytes.Equal(decoded.Locator.PrimaryKey, []byte("pk\x00")) {
		t.Fatalf("decoded sidecar entry = %+v", decoded)
	}
	key[0] = 'x'
	if decoded.Key[0] != 'M' {
		t.Fatal("decoded key aliases caller input")
	}
}

func TestSidecarEntryRejectsMalformedComponents(t *testing.T) {
	key := sidecarTestKey(t)
	valid, err := EncodeSidecarEntry(nil, SidecarEntry{Key: key, Locator: RowLocator{RelationID: 1}})
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
		{"zero key length", func() []byte {
			out := append([]byte(nil), valid...)
			binary.BigEndian.PutUint32(out[5:9], 0)
			return out
		}()},
		{"wrong key length", func() []byte {
			out := append([]byte(nil), valid...)
			binary.BigEndian.PutUint32(out[5:9], binary.BigEndian.Uint32(out[5:9])+1)
			return out
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeSidecarEntry(test.data); !errors.Is(err, ErrMalformedKey) {
				t.Fatalf("error = %v, want ErrMalformedKey", err)
			}
		})
	}
	if _, err := EncodeSidecarEntry(nil, SidecarEntry{Key: []byte("not-a-v2-key"), Locator: RowLocator{RelationID: 1}}); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("invalid key error = %v, want ErrMalformedKey", err)
	}
	if _, err := EncodeSidecarEntry(nil, SidecarEntry{Key: key, Locator: RowLocator{}}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("invalid locator error = %v, want ErrInvalidValue", err)
	}
}

func TestSidecarEntryRejectsTrailingOrOversizedInput(t *testing.T) {
	key := sidecarTestKey(t)
	valid, err := EncodeSidecarEntry(nil, SidecarEntry{Key: key, Locator: RowLocator{RelationID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSidecarEntry(append(valid, 0)); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("trailing input error = %v, want ErrMalformedKey", err)
	}
	if _, err := EncodeSidecarEntry(make([]byte, MaxSidecarEntryBytes), SidecarEntry{Key: key, Locator: RowLocator{RelationID: 1}}); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("oversized destination error = %v, want ErrMalformedKey", err)
	}
}
