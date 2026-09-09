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
	"errors"
	"testing"
)

func migrationTestOwner(id string, incarnation byte) MigrationOwner {
	return MigrationOwner{OwnerID: id, Incarnation: uint64(incarnation), ClaimToken: []byte{id[0], incarnation}}
}

func TestMigrationGateDrainsExistingWritersBeforeExclusive(t *testing.T) {
	gate, err := NewMigrationGate(17, 4)
	if err != nil {
		t.Fatal(err)
	}
	oldEpoch, err := gate.AcquireWrite("txn-before-drain")
	if err != nil || oldEpoch != 0 {
		t.Fatalf("pre-drain permit = %d, %v", oldEpoch, err)
	}
	owner := migrationTestOwner("owner-a", 1)
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	if gate.MigrationEpoch != 1 || gate.Phase != MigrationDraining {
		t.Fatalf("gate after draining = %+v", gate)
	}
	if _, err := gate.AcquireWrite("txn-after-drain"); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("post-drain write error = %v, want ErrMigrationGate", err)
	}
	if err := gate.EnterExclusive(owner, 20); err == nil {
		t.Fatal("exclusive entered with an outstanding pre-drain permit")
	}
	if err := gate.CompleteWrite("txn-before-drain", oldEpoch); err != nil {
		t.Fatal(err)
	}
	if err := gate.EnterExclusive(owner, 20); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 4, 55, 66); err != nil {
		t.Fatal(err)
	}
	if err := gate.Validate(); err != nil {
		t.Fatalf("exclusive gate invalid: %v", err)
	}
}

func TestMigrationGatePublishRequiresReplayAndCannotAbort(t *testing.T) {
	gate, err := NewMigrationGate(17, 4)
	if err != nil {
		t.Fatal(err)
	}
	owner := migrationTestOwner("owner-a", 1)
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	if err := gate.EnterExclusive(owner, 20); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 4, 55, 66); err != nil {
		t.Fatal(err)
	}
	targets := map[string]uint64{"cn-1": 3, "tn-1": 8}
	if err := gate.Publish(owner, 91, 7, targets, 30); err != nil {
		t.Fatal(err)
	}
	if gate.Phase != MigrationPublished || gate.PublicationTxnID != 91 || gate.ReplayGeneration != 7 {
		t.Fatalf("published gate = %+v", gate)
	}
	if err := gate.AbortBeforePublication(owner, 40); err == nil {
		t.Fatal("published migration was aborted")
	}
	if err := gate.ReleaseAfterReplay(owner); err == nil {
		t.Fatal("released migration before replay acknowledgements")
	}
	if err := gate.AcknowledgeReplay(owner, 6, "cn-1", 3); err == nil {
		t.Fatal("stale replay generation accepted")
	}
	if err := gate.AcknowledgeReplay(owner, 7, "cn-1", 2); err == nil {
		t.Fatal("stale replay incarnation accepted")
	}
	if err := gate.AcknowledgeReplay(owner, 7, "cn-1", 3); err != nil {
		t.Fatal(err)
	}
	if err := gate.RetireReplayTarget(owner, 7, "tn-1", 8); err != nil {
		t.Fatal(err)
	}
	if err := gate.ReleaseAfterReplay(owner); err != nil {
		t.Fatal(err)
	}
	if gate.Phase != MigrationOpen || gate.MigrationEpoch != 1 || len(gate.ReplayTargets) != 0 {
		t.Fatalf("released gate = %+v", gate)
	}
	if _, err := gate.AcquireWrite("txn-after-release"); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationGateRecoveryFencesStaleOwner(t *testing.T) {
	gate, err := NewMigrationGate(17, 4)
	if err != nil {
		t.Fatal(err)
	}
	owner := migrationTestOwner("owner-a", 1)
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	if err := gate.EnterExclusive(owner, 20); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 4, 55, 66); err != nil {
		t.Fatal(err)
	}
	if err := gate.ClaimExpired(20, owner); err == nil {
		t.Fatal("same owner reclaimed expired gate")
	}
	recovery := migrationTestOwner("recovery", 2)
	if err := gate.ClaimExpired(20, recovery); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 4, 56, 67); err == nil {
		t.Fatal("stale owner changed build identity")
	}
	if err := gate.SetBuildIdentity(recovery, 4, 56, 67); err != nil {
		t.Fatal(err)
	}
	clone := gate.Clone()
	clone.Owner.ClaimToken[0] ^= 1
	clone.WritePermits["x"] = 1
	if bytes.Equal(clone.Owner.ClaimToken, gate.Owner.ClaimToken) || len(gate.WritePermits) != 0 {
		t.Fatal("gate clone aliases mutable state")
	}
}

func TestMigrationGateRejectsMalformedPersistedState(t *testing.T) {
	gate, err := NewMigrationGate(17, 4)
	if err != nil {
		t.Fatal(err)
	}
	gate.Phase = MigrationExclusive
	if err := gate.Validate(); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("malformed exclusive error = %v, want ErrMigrationGate", err)
	}
	gate, _ = NewMigrationGate(17, 4)
	owner := migrationTestOwner("owner-a", 1)
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	gate.WritePermits["new"] = gate.MigrationEpoch
	if err := gate.Validate(); !errors.Is(err, ErrMigrationGate) {
		t.Fatalf("post-drain permit error = %v, want ErrMigrationGate", err)
	}
}
