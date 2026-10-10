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

package types

// MembershipKeyTypeCompatible reports whether a runtime membership vector can
// be compared byte-for-byte with the probe key type. The runtime-filter
// producer normalizes CHAR primary keys to VARCHAR while retaining the same
// varlena bytes; no other implicit type pairing is part of this contract.
func MembershipKeyTypeCompatible(keyOID, expectedOID T) bool {
	return keyOID == expectedOID ||
		(expectedOID == T_char && keyOID == T_varchar)
}
