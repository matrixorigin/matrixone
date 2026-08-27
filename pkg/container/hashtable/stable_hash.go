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

package hashtable

import "unsafe"

// These constants are part of the MORPCVersion33 ownership contract. Changing
// either one changes distributed owner assignment and requires a new rollout
// gate.
const (
	stableBytesHashSeed   = uint64(0x4d4f535441424c45)
	stableBytesHashSecret = uint64(0x9e3779b97f4a7c15)
)

// StableBytesHash generates a deterministic hash for a complete byte string.
// The mapping is identical across processes and does not depend on optional CPU
// instructions. It performs no allocation and hashes each logical byte exactly
// once. Empty strings retain the legacy owner-zero contract.
func StableBytesHash(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}
	return wyhashWithSecret(
		unsafe.Pointer(unsafe.SliceData(data)),
		stableBytesHashSeed,
		uint64(len(data)),
		stableBytesHashSecret)
}
