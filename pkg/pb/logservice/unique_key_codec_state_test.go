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

import "testing"

func TestStoreStateCopiesUniqueKeyCodecCapabilities(t *testing.T) {
	capability := &UniqueKeyCodecCapability{
		ReadableVersions:   1 << 2,
		WritableVersions:   1 << 2,
		RegistryVersion:    1,
		RegistryDigest:     []byte{1, 2, 3},
		MaxEncodedKeyBytes: 64 << 20,
	}
	cn := NewCNState()
	heartbeat := CNStoreHeartbeat{UUID: "cn-1", UniqueKeyCodecCapability: capability}
	cn.Update(heartbeat, 1)
	capability.RegistryDigest[0] = 9
	if got := cn.Stores["cn-1"].UniqueKeyCodecCapability.RegistryDigest[0]; got != 1 {
		t.Fatalf("CN state retained heartbeat digest alias: %d", got)
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
