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
)

const (
	locatorMagic   = "MOKL"
	LocatorVersion = uint8(1)
	locatorHeader  = 4 + 1 + 8 + 8 + 4
)

// RowLocator identifies a base-table row without depending on a transient
// storage row id. RelationID and PartitionID identify the physical relation;
// PrimaryKey is the original, type-converted primary-key byte image. The
// locator is intentionally separate from the encoded unique-key identity:
// callers must not decode a collation key into a user-visible primary key.
type RowLocator struct {
	RelationID  uint64
	PartitionID uint64
	PrimaryKey  []byte
}

// EncodeLocator appends the stable locator envelope to dst. The wire layout is
// fixed-width big-endian and contains no Go representation. A zero relation id
// is rejected because it cannot identify a persisted source relation. Empty
// primary-key bytes remain valid and distinct from a malformed/truncated
// envelope.
func EncodeLocator(dst []byte, locator RowLocator) ([]byte, error) {
	if locator.RelationID == 0 {
		return nil, wrapCodecError(ErrInvalidValue, "locator relation id is zero")
	}
	if uint64(len(locator.PrimaryKey)) > uint64(MaxKeyBytes-locatorHeader) {
		return nil, wrapCodecError(ErrInvalidValue, "locator primary key is too large: %d", len(locator.PrimaryKey))
	}
	if len(dst) > MaxKeyBytes-locatorHeader-len(locator.PrimaryKey) {
		return nil, wrapCodecError(ErrInvalidValue, "locator destination exceeds maximum size")
	}

	result := append(dst, locatorMagic...)
	result = append(result, LocatorVersion)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], locator.RelationID)
	result = append(result, u64[:]...)
	binary.BigEndian.PutUint64(u64[:], locator.PartitionID)
	result = append(result, u64[:]...)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(locator.PrimaryKey)))
	result = append(result, u32[:]...)
	result = append(result, locator.PrimaryKey...)
	return result, nil
}

// DecodeLocator validates and decodes one complete locator envelope. The
// returned primary-key bytes are copied so the result does not alias the input
// buffer, which may be a pooled storage or network buffer.
func DecodeLocator(encoded []byte) (RowLocator, error) {
	if len(encoded) < locatorHeader {
		return RowLocator{}, wrapCodecError(ErrMalformedKey, "locator is truncated")
	}
	if !bytes.Equal(encoded[:4], []byte(locatorMagic)) {
		return RowLocator{}, wrapCodecError(ErrMalformedKey, "locator magic mismatch")
	}
	if encoded[4] != LocatorVersion {
		return RowLocator{}, wrapCodecError(ErrMalformedKey, "locator version %d", encoded[4])
	}
	relationID := binary.BigEndian.Uint64(encoded[5:13])
	if relationID == 0 {
		return RowLocator{}, wrapCodecError(ErrMalformedKey, "locator relation id is zero")
	}
	partitionID := binary.BigEndian.Uint64(encoded[13:21])
	keyLength := uint64(binary.BigEndian.Uint32(encoded[21:25]))
	if keyLength > uint64(MaxKeyBytes-locatorHeader) || keyLength != uint64(len(encoded)-locatorHeader) {
		return RowLocator{}, wrapCodecError(ErrMalformedKey, "locator key length %d", keyLength)
	}
	return RowLocator{
		RelationID:  relationID,
		PartitionID: partitionID,
		PrimaryKey:  append([]byte(nil), encoded[locatorHeader:]...),
	}, nil
}
