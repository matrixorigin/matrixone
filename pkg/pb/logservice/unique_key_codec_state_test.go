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

package logservice

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
)

func TestStoreStateCopiesUniqueKeyCodecCapabilities(t *testing.T) {
	capability := &UniqueKeyCodecCapability{
		ReadableVersions:   1 << 2,
		WritableVersions:   1 << 2,
		RegistryVersion:    1,
		RegistryDigest:     []byte{1, 2, 3},
		MaxEncodedKeyBytes: 64 << 20,
		Incarnation:        17,
	}
	cn := NewCNState()
	heartbeat := CNStoreHeartbeat{UUID: "cn-1", UniqueKeyCodecCapability: capability}
	cn.Update(heartbeat, 1)
	capability.RegistryDigest[0] = 9
	if got := cn.Stores["cn-1"].UniqueKeyCodecCapability.RegistryDigest[0]; got != 1 {
		t.Fatalf("CN state retained heartbeat digest alias: %d", got)
	}
	if got := cn.Stores["cn-1"].UniqueKeyCodecCapability.Incarnation; got != 17 {
		t.Fatalf("CN state lost capability incarnation: %d", got)
	}
	cn.Update(CNStoreHeartbeat{UUID: "cn-1"}, 2)
	if cn.Stores["cn-1"].UniqueKeyCodecCapability != nil {
		t.Fatal("missing CN capability did not clear stale state")
	}

	capability = &UniqueKeyCodecCapability{RegistryDigest: []byte{4, 5}}
	tn := NewTNState()
	tn.Update(TNStoreHeartbeat{UUID: "tn-1", UniqueKeyCodecCapability: capability}, 1)
	capability.RegistryDigest[0] = 8
	if got := tn.Stores["tn-1"].UniqueKeyCodecCapability.RegistryDigest[0]; got != 4 {
		t.Fatalf("TN state retained heartbeat digest alias: %d", got)
	}
}

func TestUniqueKeyCodecWireAdaptersFailClosed(t *testing.T) {
	var missing *UniqueKeyCodecCapability
	if missing.ToCollationKeyCapability().Supports(collationkey.NewCollationAwareMetadata(), true) {
		t.Fatal("missing capability advertised v2 support")
	}
	activation, err := (*UniqueKeyCodecActivation)(nil).ToCollationKeyActivation()
	if err != nil || activation.Phase != collationkey.ActivationDisabled {
		t.Fatalf("missing activation = %+v, %v", activation, err)
	}
	valid := &UniqueKeyCodecActivation{
		RequestedVersion: 2,
		RegistryVersion:  1,
		RegistryDigest:   collationkey.RegistryDigest(),
		Generation:       4,
		Phase:            ENABLED,
		CnTargets:        map[string]uint64{"cn": 0},
		TnTargets:        map[string]uint64{"tn": 2},
	}
	if _, err := valid.ToCollationKeyActivation(); !errors.Is(err, collationkey.ErrMalformedKey) {
		t.Fatalf("incomplete enabled activation error = %v, want malformed", err)
	}
	valid.Phase = PREPARING
	valid.CnTargets["cn"] = 1
	converted, err := valid.ToCollationKeyActivation()
	if err != nil {
		t.Fatal(err)
	}
	valid.CnTargets["cn"] = 9
	if converted.CnTargets["cn"] != 1 {
		t.Fatal("activation adapter aliases target map")
	}
}

func TestUniqueKeyCodecCapabilityBuildsGenerationAcknowledgement(t *testing.T) {
	capability := &UniqueKeyCodecCapability{
		ReadableVersions:   1 << 2,
		WritableVersions:   1 << 2,
		RegistryVersion:    1,
		RegistryDigest:     collationkey.RegistryDigest(),
		MaxEncodedKeyBytes: collationkey.MaxKeyBytes,
		Incarnation:        27,
	}
	ack := capability.ToCollationKeyAcknowledgement("cn-1")
	if ack.NodeID != "cn-1" || ack.Incarnation != 27 ||
		!ack.Capability.Supports(collationkey.NewCollationAwareMetadata(), true) {
		t.Fatalf("acknowledgement = %+v", ack)
	}
	missing := (*UniqueKeyCodecCapability)(nil).ToCollationKeyAcknowledgement("cn-2")
	if missing.NodeID != "cn-2" || missing.Incarnation != 0 ||
		missing.Capability.Supports(collationkey.NewCollationAwareMetadata(), true) {
		t.Fatalf("missing acknowledgement = %+v", missing)
	}
}
