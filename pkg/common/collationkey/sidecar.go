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

import "encoding/binary"

const (
	sidecarMagic         = "MOKS"
	SidecarEntryVersion  = uint8(1)
	sidecarEntryHeader   = 4 + 1 + 4 + 4
	MaxSidecarEntryBytes = 2*MaxKeyBytes + sidecarEntryHeader
)

// SidecarEntry is the storage-owned value associated with one encoded unique
// key. The key is the complete MOKY envelope and the locator is the original
// physical row identity. Neither field is a user-visible value and neither is
// allowed to depend on a transient __mo_rowid.
type SidecarEntry struct {
	Key     []byte
	Locator RowLocator
}

// EncodeSidecarEntry serializes one key-to-row mapping using a fixed,
// length-delimited envelope. It validates both the v2 key and locator before
// appending anything to dst, so callers never publish a partial mapping after
// malformed input. The relation's codec metadata and activation generation
// are checked by the storage owner; this package only validates the bytes.
func EncodeSidecarEntry(dst []byte, entry SidecarEntry) ([]byte, error) {
	if err := ValidateEncoded(entry.Key); err != nil {
		return dst, err
	}
	if hasNull, err := HasNullPart(entry.Key); err != nil {
		return dst, err
	} else if hasNull {
		return dst, wrapCodecError(ErrUnsupportedDomain, "NULL-bearing unique key has no sidecar identity")
	}
	locator, err := EncodeLocator(nil, entry.Locator)
	if err != nil {
		return dst, err
	}
	if len(entry.Key) > MaxKeyBytes || len(locator) > MaxKeyBytes {
		return dst, wrapCodecError(ErrMalformedKey, "sidecar component exceeds key limit")
	}
	if len(dst) > MaxSidecarEntryBytes-(sidecarEntryHeader+len(entry.Key)+len(locator)) {
		return dst, wrapCodecError(ErrMalformedKey, "sidecar destination exceeds maximum size")
	}

	result := append(dst, sidecarMagic...)
	result = append(result, SidecarEntryVersion)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(entry.Key)))
	result = append(result, u32[:]...)
	binary.BigEndian.PutUint32(u32[:], uint32(len(locator)))
	result = append(result, u32[:]...)
	result = append(result, entry.Key...)
	result = append(result, locator...)
	return result, nil
}

// DecodeSidecarEntry validates a complete sidecar mapping and copies both
// variable-length fields out of the input buffer. This is the boundary a
// replay or restore reader should use before exposing a locator to a base-row
// lookup.
func DecodeSidecarEntry(encoded []byte) (SidecarEntry, error) {
	if len(encoded) < sidecarEntryHeader || len(encoded) > MaxSidecarEntryBytes {
		return SidecarEntry{}, wrapCodecError(ErrMalformedKey, "sidecar entry length %d", len(encoded))
	}
	if string(encoded[:4]) != sidecarMagic {
		return SidecarEntry{}, wrapCodecError(ErrMalformedKey, "sidecar magic mismatch")
	}
	if encoded[4] != SidecarEntryVersion {
		return SidecarEntry{}, wrapCodecError(ErrMalformedKey, "sidecar version %d", encoded[4])
	}
	keyLen := uint64(binary.BigEndian.Uint32(encoded[5:9]))
	locatorLen := uint64(binary.BigEndian.Uint32(encoded[9:13]))
	if keyLen == 0 || keyLen > MaxKeyBytes || locatorLen == 0 || locatorLen > MaxKeyBytes {
		return SidecarEntry{}, wrapCodecError(ErrMalformedKey, "sidecar component lengths %d/%d", keyLen, locatorLen)
	}
	if keyLen+locatorLen != uint64(len(encoded)-sidecarEntryHeader) {
		return SidecarEntry{}, wrapCodecError(ErrMalformedKey, "sidecar component lengths do not cover entry")
	}
	keyEnd := sidecarEntryHeader + int(keyLen)
	key := encoded[sidecarEntryHeader:keyEnd]
	if err := ValidateEncoded(key); err != nil {
		return SidecarEntry{}, err
	}
	locator, err := DecodeLocator(encoded[keyEnd:])
	if err != nil {
		return SidecarEntry{}, err
	}
	return SidecarEntry{
		Key:     append([]byte(nil), key...),
		Locator: locator,
	}, nil
}
