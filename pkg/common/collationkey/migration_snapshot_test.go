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
	"errors"
	"reflect"
	"strings"
	"testing"
)

func makePublishedMigrationGate(t *testing.T) MigrationGate {
	t.Helper()
	gate, err := NewMigrationGate(91, 12)
	if err != nil {
		t.Fatal(err)
	}
	owner := MigrationOwner{OwnerID: "owner-a", Incarnation: 7, ClaimToken: []byte{1, 2, 3}}
	if err := gate.BeginDraining(owner, 100); err != nil {
		t.Fatal(err)
	}
	if err := gate.EnterExclusive(owner, 200); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 12, 300, 400); err != nil {
		t.Fatal(err)
	}
	targets := map[string]uint64{"cn-a": 11, "tn-a": 13, "tn-b": 14}
	if err := gate.Publish(owner, 500, 600, targets, 700); err != nil {
		t.Fatal(err)
	}
	if err := gate.AcknowledgeReplay(owner, 600, "cn-a", 11); err != nil {
		t.Fatal(err)
	}
	if err := gate.RetireReplayTarget(owner, 600, "tn-a", 13); err != nil {
		t.Fatal(err)
	}
	if err := gate.AcknowledgeReplay(owner, 600, "tn-b", 14); err != nil {
		t.Fatal(err)
	}
	return gate
}

func TestMigrationGateSnapshotRoundTripAndOwnership(t *testing.T) {
	gate := makePublishedMigrationGate(t)
	wire, err := EncodeMigrationGate(nil, gate)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMigrationGate(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, gate) {
		t.Fatalf("decoded migration gate differs:\n got=%+v\nwant=%+v", decoded, gate)
	}
	decoded.Owner.ClaimToken[0] ^= 1
	decoded.ReplayTargets["cn-a"] = 99
	if bytes.Equal(decoded.Owner.ClaimToken, gate.Owner.ClaimToken) || gate.ReplayTargets["cn-a"] != 11 {
		t.Fatal("decoded migration gate aliases the source")
	}

	// Map ordering is part of the persistence identity, not an implementation
	// detail. Re-encoding a map populated in a different order must be stable.
	gate2 := makePublishedMigrationGate(t)
	gate2.ReplayTargets = map[string]uint64{"tn-b": 14, "cn-a": 11, "tn-a": 13}
	gate2.ReplayAcknowledged = map[string]uint64{"tn-b": 14, "cn-a": 11, "tn-a": 13}
	gate2.RetiredReplayTarget = map[string]uint64{"tn-a": 13}
	wire2, err := EncodeMigrationGate(nil, gate2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, wire2) {
		t.Fatal("migration gate encoding depends on map insertion order")
	}
}

func TestMigrationGateSnapshotPreservesOpenWritePermits(t *testing.T) {
	gate, err := NewMigrationGate(92, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.AcquireWrite("tx-before-drain"); err != nil {
		t.Fatal(err)
	}
	wire, err := EncodeMigrationGate(nil, gate)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMigrationGate(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Phase != MigrationOpen || decoded.WritePermits["tx-before-drain"] != 0 {
		t.Fatalf("decoded open permits = %+v", decoded)
	}
}

func TestMigrationGateSnapshotRejectsChecksumAndMalformedState(t *testing.T) {
	wire, err := EncodeMigrationGate(nil, makePublishedMigrationGate(t))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeMigrationGate(corrupt); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("checksum error = %v", err)
	}
	if _, err := DecodeMigrationGate(wire[:len(wire)-1]); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("truncated error = %v", err)
	}

	// Change the phase and recompute the checksum so the decoder reaches the
	// state-machine validation rather than stopping at the integrity check.
	badPhase := append([]byte(nil), wire...)
	badPhase[5+8+8] = 99
	digest := sha256.Sum256(badPhase[:len(badPhase)-migrationSnapshotDigest])
	copy(badPhase[len(badPhase)-migrationSnapshotDigest:], digest[:])
	if _, err := DecodeMigrationGate(badPhase); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("unknown phase error = %v", err)
	}
}

func TestMigrationGateSnapshotRejectsOversizedIdentity(t *testing.T) {
	gate, err := NewMigrationGate(93, 1)
	if err != nil {
		t.Fatal(err)
	}
	owner := MigrationOwner{
		OwnerID:     strings.Repeat("x", MaxMigrationIdentityBytes+1),
		Incarnation: 1,
		ClaimToken:  []byte{1},
	}
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeMigrationGate(nil, gate); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("oversized owner error = %v", err)
	}
}
