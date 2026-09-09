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
	"crypto/sha256"
	"encoding/binary"
)

const (
	sidecarSnapshotMagic   = "MOKP"
	sidecarSnapshotVersion = uint8(1)
	sidecarSnapshotHeader  = 4 + 1 + 4 + 4 + 4 + 4 + 8 + 8 + 8 + 4
	sidecarSnapshotDigest  = sha256.Size
	// A snapshot is an interchange/checkpoint boundary, so the decoder must
	// have a finite allocation ceiling even when the owning storage layer has
	// not yet supplied a page-level limit.
	MaxSidecarSnapshotBytes   = 256 << 20
	MaxSidecarSnapshotEntries = 1 << 20
)

// EncodeSidecarSnapshot serializes a complete, deterministic sidecar image.
// The metadata and relation identity are stored with the entries; a reader
// therefore cannot accidentally restore key bytes under a different codec or
// table. The trailing SHA-256 covers every byte before it and detects torn or
// corrupted checkpoint data before any entry is exposed.
//
// The format is deliberately independent of Go struct layout:
//
//	MOKP | version | codec metadata | relation id | revision | entry count |
//	length-delimited MOKS entries | sha256(body)
//
// Entries must be strictly ordered by their complete encoded key, as produced
// by SidecarStore.Snapshot. A caller supplying an unsorted snapshot receives a
// fail-closed error rather than a nondeterministic physical image.
func EncodeSidecarSnapshot(dst []byte, snapshot SidecarSnapshot) ([]byte, error) {
	if err := validateSidecarSnapshotShape(snapshot); err != nil {
		return dst, err
	}
	start := len(dst)
	if start > MaxSidecarSnapshotBytes {
		return dst, wrapCodecError(ErrMalformedKey, "snapshot destination exceeds maximum size")
	}
	result := append(dst, sidecarSnapshotMagic...)
	result = append(result, sidecarSnapshotVersion)
	result = appendU32(result, snapshot.Metadata.Version)
	result = appendU32(result, snapshot.Metadata.RegistryVersion)
	result = appendU32(result, uint32(len(snapshot.Metadata.RegistryDigest)))
	result = append(result, snapshot.Metadata.RegistryDigest...)
	result = appendU32(result, snapshot.Metadata.MaxEncodedKeyBytes)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], snapshot.Metadata.ActivationGeneration)
	result = append(result, u64[:]...)
	binary.BigEndian.PutUint64(u64[:], snapshot.RelationID)
	result = append(result, u64[:]...)
	binary.BigEndian.PutUint64(u64[:], snapshot.Revision)
	result = append(result, u64[:]...)
	result = appendU32(result, uint32(len(snapshot.Entries)))
	if len(result)-start+sidecarSnapshotDigest > MaxSidecarSnapshotBytes {
		return dst, wrapCodecError(ErrMalformedKey, "sidecar snapshot exceeds maximum size")
	}
	for _, entry := range snapshot.Entries {
		encoded, err := EncodeSidecarEntry(nil, entry)
		if err != nil {
			return dst, err
		}
		if len(encoded) > MaxSidecarEntryBytes {
			return dst, wrapCodecError(ErrMalformedKey, "sidecar entry exceeds maximum size")
		}
		result = appendU32(result, uint32(len(encoded)))
		result = append(result, encoded...)
		if len(result)-start+sidecarSnapshotDigest > MaxSidecarSnapshotBytes {
			return dst, wrapCodecError(ErrMalformedKey, "sidecar snapshot exceeds maximum size")
		}
	}
	digest := sha256.Sum256(result[start:])
	result = append(result, digest[:]...)
	return result, nil
}

// DecodeSidecarSnapshot verifies the complete snapshot checksum and all nested
// MOKS/MOKL/MOKY identities before returning a deep-copied image. It performs
// no partial publication: callers can pass the returned value to
// RestoreSidecarStore only after this function succeeds.
func DecodeSidecarSnapshot(encoded []byte) (SidecarSnapshot, error) {
	if len(encoded) < sidecarSnapshotHeader+sidecarSnapshotDigest || len(encoded) > MaxSidecarSnapshotBytes {
		return SidecarSnapshot{}, wrapCodecError(ErrMalformedKey, "sidecar snapshot length %d", len(encoded))
	}
	bodyEnd := len(encoded) - sidecarSnapshotDigest
	want := encoded[bodyEnd:]
	got := sha256.Sum256(encoded[:bodyEnd])
	if !bytes.Equal(want, got[:]) {
		return SidecarSnapshot{}, wrapCodecError(ErrMalformedKey, "sidecar snapshot checksum mismatch")
	}
	body := encoded[:bodyEnd]
	if !bytes.Equal(body[:4], []byte(sidecarSnapshotMagic)) || body[4] != sidecarSnapshotVersion {
		return SidecarSnapshot{}, wrapCodecError(ErrMalformedKey, "sidecar snapshot header mismatch")
	}
	off := 5
	codecVersion, ok := readU32(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	registryVersion, ok := readU32(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	digestLen, ok := readU32(body, &off)
	if !ok || digestLen > uint32(len(body)-off) {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	registryDigest := append([]byte(nil), body[off:off+int(digestLen)]...)
	off += int(digestLen)
	maxKeyBytes, ok := readU32(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	activationGeneration, ok := readU64(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	relationID, ok := readU64(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	revision, ok := readU64(body, &off)
	if !ok {
		return SidecarSnapshot{}, ErrMalformedKey
	}
	entryCount, ok := readU32(body, &off)
	if !ok || entryCount > MaxSidecarSnapshotEntries {
		return SidecarSnapshot{}, wrapCodecError(ErrMalformedKey, "sidecar snapshot entry count %d", entryCount)
	}
	snapshot := SidecarSnapshot{
		Metadata: RelationMetadata{
			Version:              codecVersion,
			RegistryVersion:      registryVersion,
			RegistryDigest:       registryDigest,
			MaxEncodedKeyBytes:   maxKeyBytes,
			ActivationGeneration: activationGeneration,
		},
		RelationID: relationID,
		Revision:   revision,
		Entries:    make([]SidecarEntry, 0, int(entryCount)),
	}
	for i := uint32(0); i < entryCount; i++ {
		entryLen, ok := readU32(body, &off)
		if !ok || entryLen == 0 || entryLen > MaxSidecarEntryBytes || uint64(entryLen) > uint64(len(body)-off) {
			return SidecarSnapshot{}, ErrMalformedKey
		}
		entry, err := DecodeSidecarEntry(body[off : off+int(entryLen)])
		if err != nil {
			return SidecarSnapshot{}, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
		off += int(entryLen)
	}
	if off != len(body) {
		return SidecarSnapshot{}, wrapCodecError(ErrMalformedKey, "sidecar snapshot has trailing body bytes")
	}
	if err := validateSidecarSnapshotShape(snapshot); err != nil {
		return SidecarSnapshot{}, err
	}
	if _, err := RestoreSidecarStore(snapshot); err != nil {
		return SidecarSnapshot{}, err
	}
	return snapshot, nil
}

func validateSidecarSnapshotShape(snapshot SidecarSnapshot) error {
	if err := snapshot.Metadata.Validate(); err != nil {
		return err
	}
	if !snapshot.Metadata.IsV2() {
		return wrapCodecError(ErrUnsupportedDomain, "sidecar snapshot requires v2 metadata")
	}
	if snapshot.RelationID == 0 {
		return wrapCodecError(ErrMalformedKey, "sidecar snapshot relation id is zero")
	}
	if len(snapshot.Entries) > MaxSidecarSnapshotEntries {
		return wrapCodecError(ErrMalformedKey, "sidecar snapshot entry count %d", len(snapshot.Entries))
	}
	if snapshot.Revision == 0 && len(snapshot.Entries) != 0 {
		return wrapCodecError(ErrMalformedKey, "non-empty sidecar snapshot has zero revision")
	}
	var previous []byte
	for _, entry := range snapshot.Entries {
		if len(previous) != 0 && bytes.Compare(previous, entry.Key) >= 0 {
			return wrapCodecError(ErrMalformedKey, "sidecar snapshot keys are not strictly ordered")
		}
		if entry.Locator.RelationID != snapshot.RelationID {
			return ErrSidecarRelation
		}
		if err := ValidateEncoded(entry.Key); err != nil {
			return err
		}
		if _, err := EncodeLocator(nil, entry.Locator); err != nil {
			return err
		}
		previous = append(previous[:0], entry.Key...)
	}
	return nil
}

func readU32(buf []byte, off *int) (uint32, bool) {
	if off == nil || *off < 0 || len(buf)-*off < 4 {
		return 0, false
	}
	v := binary.BigEndian.Uint32(buf[*off : *off+4])
	*off += 4
	return v, true
}

func readU64(buf []byte, off *int) (uint64, bool) {
	if off == nil || *off < 0 || len(buf)-*off < 8 {
		return 0, false
	}
	v := binary.BigEndian.Uint64(buf[*off : *off+8])
	*off += 8
	return v, true
}
