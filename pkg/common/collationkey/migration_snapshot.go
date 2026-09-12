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
	"sort"
)

const (
	migrationSnapshotMagic   = "MOKG"
	migrationSnapshotVersion = uint8(1)
	migrationSnapshotDigest  = sha256.Size

	// A migration gate is small in practice (it contains node and transaction
	// identities, not table data), but the decoder still has to be bounded when
	// reading a checkpoint supplied by a storage/recovery layer.
	MaxMigrationSnapshotBytes = 16 << 20
	MaxMigrationGateEntries   = 1 << 20
	MaxMigrationIdentityBytes = 1 << 20
	migrationSnapshotMinimum  = 5 + 8*8 + 1 + 4 + 8 + 4 + 4*4 + migrationSnapshotDigest
)

// EncodeMigrationGate serializes one durable migration state.  It is a
// checkpoint/replication envelope, not a Go struct dump: integer fields are
// big-endian, map keys are sorted, and the checksum covers the complete body.
// The caller must persist the resulting bytes atomically with the catalog
// state that names the relation; this package never publishes a partial gate.
func EncodeMigrationGate(dst []byte, gate MigrationGate) ([]byte, error) {
	if err := validateMigrationGateWireShape(gate); err != nil {
		return dst, err
	}
	if len(dst) > MaxMigrationSnapshotBytes {
		return dst, wrapMigrationError("migration snapshot destination exceeds maximum size")
	}
	// Build the body separately and append it to dst only after every field has
	// passed validation. This keeps the caller's destination untouched on an
	// error, even when it has spare capacity in its backing array.
	body := make([]byte, 0, migrationSnapshotMinimum-migrationSnapshotDigest)
	body = append(body, migrationSnapshotMagic...)
	body = append(body, migrationSnapshotVersion)
	body = appendMigrationU64(body, gate.RelationID)
	body = appendMigrationU64(body, gate.MigrationEpoch)
	body = append(body, byte(gate.Phase))
	body = appendMigrationU64(body, uint64(gate.PhaseDeadlineNanos))
	body = appendMigrationU64(body, gate.SourceSchemaEpoch)
	body = appendMigrationU64(body, gate.SourceSnapshotID)
	body = appendMigrationU64(body, gate.TempRelationID)
	body = appendMigrationU64(body, gate.PublicationTxnID)
	body = appendMigrationU64(body, gate.ReplayGeneration)
	body = appendMigrationString(body, gate.Owner.OwnerID)
	body = appendMigrationU64(body, gate.Owner.Incarnation)
	body = appendMigrationBytes(body, gate.Owner.ClaimToken)
	var err error
	for _, entries := range []map[string]uint64{
		gate.WritePermits,
		gate.ReplayTargets,
		gate.ReplayAcknowledged,
		gate.RetiredReplayTarget,
	} {
		body, err = appendMigrationMap(body, entries)
		if err != nil {
			return dst, err
		}
	}
	if len(body)+migrationSnapshotDigest > MaxMigrationSnapshotBytes {
		return dst, wrapMigrationError("migration snapshot exceeds maximum size")
	}
	digest := sha256.Sum256(body)
	body = append(body, digest[:]...)
	return append(dst, body...), nil
}

// DecodeMigrationGate verifies the checksum and every length-delimited field
// before returning a gate.  No decoded state is exposed to a caller until the
// state machine's own Validate method also accepts it.
func DecodeMigrationGate(encoded []byte) (MigrationGate, error) {
	if len(encoded) < migrationSnapshotMinimum || len(encoded) > MaxMigrationSnapshotBytes {
		return MigrationGate{}, wrapMigrationError("migration snapshot length %d", len(encoded))
	}
	bodyEnd := len(encoded) - migrationSnapshotDigest
	want := encoded[bodyEnd:]
	got := sha256.Sum256(encoded[:bodyEnd])
	if !bytes.Equal(want, got[:]) {
		return MigrationGate{}, wrapMigrationError("migration snapshot checksum mismatch")
	}
	body := encoded[:bodyEnd]
	if !bytes.Equal(body[:4], []byte(migrationSnapshotMagic)) || body[4] != migrationSnapshotVersion {
		return MigrationGate{}, wrapMigrationError("migration snapshot header mismatch")
	}
	off := 5
	read := func() (uint64, bool) { return readU64(body, &off) }
	relationID, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	epoch, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	if off >= len(body) {
		return MigrationGate{}, ErrMigrationGate
	}
	phase := MigrationPhase(body[off])
	off++
	deadline, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	schemaEpoch, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	snapshotID, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	tempRelationID, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	publicationTxnID, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	replayGeneration, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	ownerID, ok := readMigrationString(body, &off)
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	ownerIncarnation, ok := read()
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	claimToken, ok := readMigrationBytes(body, &off)
	if !ok {
		return MigrationGate{}, ErrMigrationGate
	}
	gate := MigrationGate{
		RelationID:          relationID,
		MigrationEpoch:      epoch,
		Owner:               MigrationOwner{OwnerID: ownerID, Incarnation: ownerIncarnation, ClaimToken: claimToken},
		Phase:               phase,
		PhaseDeadlineNanos:  int64(deadline),
		SourceSchemaEpoch:   schemaEpoch,
		SourceSnapshotID:    snapshotID,
		TempRelationID:      tempRelationID,
		PublicationTxnID:    publicationTxnID,
		ReplayGeneration:    replayGeneration,
		WritePermits:        make(map[string]uint64),
		ReplayTargets:       make(map[string]uint64),
		ReplayAcknowledged:  make(map[string]uint64),
		RetiredReplayTarget: make(map[string]uint64),
	}
	maps := []*map[string]uint64{
		&gate.WritePermits,
		&gate.ReplayTargets,
		&gate.ReplayAcknowledged,
		&gate.RetiredReplayTarget,
	}
	for _, target := range maps {
		decoded, err := readMigrationMap(body, &off)
		if err != nil {
			return MigrationGate{}, err
		}
		*target = decoded
	}
	if off != len(body) {
		return MigrationGate{}, wrapMigrationError("migration snapshot has trailing body bytes")
	}
	if err := validateMigrationGateWireShape(gate); err != nil {
		return MigrationGate{}, err
	}
	return gate, nil
}

func validateMigrationGateWireShape(gate MigrationGate) error {
	if err := gate.Validate(); err != nil {
		return err
	}
	if len(gate.Owner.OwnerID) > MaxMigrationIdentityBytes || len(gate.Owner.ClaimToken) > MaxMigrationIdentityBytes {
		return wrapMigrationError("owner identity exceeds maximum size")
	}
	total := 0
	for _, entries := range []map[string]uint64{
		gate.WritePermits,
		gate.ReplayTargets,
		gate.ReplayAcknowledged,
		gate.RetiredReplayTarget,
	} {
		if len(entries) > MaxMigrationGateEntries {
			return wrapMigrationError("migration map has %d entries", len(entries))
		}
		total += len(entries)
		if total > MaxMigrationGateEntries {
			return wrapMigrationError("migration gate has %d entries", total)
		}
		for key := range entries {
			if key == "" || len(key) > MaxMigrationIdentityBytes {
				return wrapMigrationError("migration map identity has invalid length")
			}
		}
	}
	return nil
}

func appendMigrationU64(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendMigrationString(dst []byte, value string) []byte {
	return appendMigrationBytes(dst, []byte(value))
}

func appendMigrationBytes(dst, value []byte) []byte {
	dst = appendU32(dst, uint32(len(value)))
	return append(dst, value...)
}

func appendMigrationMap(dst []byte, entries map[string]uint64) ([]byte, error) {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0 })
	dst = appendU32(dst, uint32(len(keys)))
	for _, key := range keys {
		if len(key) == 0 || len(key) > MaxMigrationIdentityBytes {
			return dst, wrapMigrationError("migration map identity has invalid length")
		}
		dst = appendMigrationString(dst, key)
		dst = appendMigrationU64(dst, entries[key])
	}
	return dst, nil
}

func readMigrationString(buf []byte, off *int) (string, bool) {
	value, ok := readMigrationBytes(buf, off)
	return string(value), ok
}

func readMigrationBytes(buf []byte, off *int) ([]byte, bool) {
	length, ok := readU32(buf, off)
	if !ok || length > MaxMigrationIdentityBytes || uint64(length) > uint64(len(buf)-*off) {
		return nil, false
	}
	value := append([]byte(nil), buf[*off:*off+int(length)]...)
	*off += int(length)
	return value, true
}

func readMigrationMap(buf []byte, off *int) (map[string]uint64, error) {
	count, ok := readU32(buf, off)
	if !ok || count > MaxMigrationGateEntries {
		return nil, wrapMigrationError("migration map entry count %d", count)
	}
	entries := make(map[string]uint64, int(count))
	var previous []byte
	for i := uint32(0); i < count; i++ {
		keyBytes, ok := readMigrationBytes(buf, off)
		if !ok || len(keyBytes) == 0 {
			return nil, ErrMigrationGate
		}
		if len(previous) != 0 && bytes.Compare(previous, keyBytes) >= 0 {
			return nil, wrapMigrationError("migration map keys are not strictly ordered")
		}
		value, ok := readU64(buf, off)
		if !ok {
			return nil, ErrMigrationGate
		}
		key := string(keyBytes)
		if _, exists := entries[key]; exists {
			return nil, wrapMigrationError("duplicate migration map identity")
		}
		entries[key] = value
		previous = append(previous[:0], keyBytes...)
	}
	return entries, nil
}
