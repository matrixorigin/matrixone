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
	"testing"
)

func testV2Capability(read, write bool) Capability {
	var readable, writable uint32
	if read {
		readable = uint32(1) << CollationAwareVersion
	}
	if write {
		writable = uint32(1) << CollationAwareVersion
	}
	return Capability{
		ReadableVersions:   readable,
		WritableVersions:   writable,
		RegistryVersion:    uint32(RegistryVersion),
		RegistryDigest:     RegistryDigest(),
		MaxEncodedKeyBytes: MaxKeyBytes,
	}
}

func TestActivationRequiresCompleteDurableIdentity(t *testing.T) {
	activation := NewActivationRequest(7, map[string]uint64{"cn-1": 3}, map[string]uint64{"tn-1": 9})
	if err := activation.Validate(); err != nil {
		t.Fatalf("valid activation rejected: %v", err)
	}
	activation.RegistryDigest[0] ^= 1
	if err := activation.Validate(); err == nil {
		t.Fatal("registry digest mismatch accepted")
	}
	activation = NewActivationRequest(7, map[string]uint64{"cn-1": 3}, nil)
	if err := activation.Validate(); err == nil {
		t.Fatal("incomplete target set accepted")
	}
	if _, err := (Activation{Phase: ActivationDisabled, Generation: 1}).Advance(ActivationPreparing); err == nil {
		t.Fatal("disabled state with hidden generation accepted")
	}
}

func TestActivationReadyRequiresExactIncarnationsAndBothDirections(t *testing.T) {
	activation := NewActivationRequest(11, map[string]uint64{"cn-1": 4}, map[string]uint64{"tn-1": 8})
	cn := NodeAcknowledgement{NodeID: "cn-1", Incarnation: 4, Capability: testV2Capability(true, true)}
	tn := NodeAcknowledgement{NodeID: "tn-1", Incarnation: 8, Capability: testV2Capability(true, true)}
	if !activation.NodeReady(NodeCN, cn) || !activation.NodeReady(NodeTN, tn) {
		t.Fatal("matching node acknowledgement was not ready")
	}
	if !activation.Ready([]NodeAcknowledgement{cn}, []NodeAcknowledgement{tn}) {
		t.Fatal("complete acknowledgement set was not ready")
	}
	if activation.Ready([]NodeAcknowledgement{{NodeID: "cn-1", Incarnation: 3, Capability: testV2Capability(true, true)}}, []NodeAcknowledgement{tn}) {
		t.Fatal("stale CN incarnation accepted")
	}
	if activation.NodeReady(NodeCN, NodeAcknowledgement{NodeID: "cn-1", Incarnation: 4, Capability: testV2Capability(true, false)}) {
		t.Fatal("read-only CN accepted for activation")
	}
	if activation.NodeReady(NodeKind(99), cn) {
		t.Fatal("unknown node kind accepted")
	}
}

func TestActivationTransitionsCannotDowngradeEnabledGeneration(t *testing.T) {
	activation := NewActivationRequest(13, map[string]uint64{"cn": 1}, map[string]uint64{"tn": 1})
	cn := NodeAcknowledgement{NodeID: "cn", Incarnation: 1, Capability: testV2Capability(true, true)}
	tn := NodeAcknowledgement{NodeID: "tn", Incarnation: 1, Capability: testV2Capability(true, true)}
	enabled, err := activation.Enable([]NodeAcknowledgement{cn}, []NodeAcknowledgement{tn})
	if err != nil {
		t.Fatalf("preparing -> enabled rejected: %v", err)
	}
	if enabled.Phase != ActivationEnabled {
		t.Fatalf("phase = %s", enabled.Phase)
	}
	if _, err := activation.Advance(ActivationEnabled); err == nil {
		t.Fatal("preparing -> enabled transition bypassed acknowledgements")
	}
	if _, err := enabled.Advance(ActivationDisabled); err == nil {
		t.Fatal("enabled -> disabled transition accepted")
	}
	if _, err := enabled.Advance(ActivationAborted); err == nil {
		t.Fatal("enabled -> aborted transition accepted")
	}
	aborted, err := activation.Advance(ActivationAborted)
	if err != nil {
		t.Fatalf("preparing -> aborted rejected: %v", err)
	}
	if _, err := aborted.Advance(ActivationPreparing); err == nil {
		t.Fatal("aborted -> preparing transition accepted")
	}
}

func TestActivationCopiesTargetsAndDigest(t *testing.T) {
	cn := map[string]uint64{"cn": 2}
	tn := map[string]uint64{"tn": 3}
	activation := NewActivationRequest(17, cn, tn)
	cn["cn"] = 99
	tn["tn"] = 99
	if activation.CnTargets["cn"] != 2 || activation.TnTargets["tn"] != 3 {
		t.Fatal("activation targets alias caller maps")
	}
	want := RegistryDigest()
	if !bytes.Equal(activation.RegistryDigest, want) {
		t.Fatal("activation did not carry registry digest")
	}
}
