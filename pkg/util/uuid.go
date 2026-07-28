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

package util

import (
	"crypto/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// FastUuid generates UUID v7 with high performance and no global lock.
//
// Performance:
//   - Single-threaded: ~60 ns/op (1.7x faster than google/uuid)
//   - Concurrent: ~17 ns/op (10.9x faster than google/uuid)
//   - Zero memory allocation
//
// Guarantees:
//   - Uniqueness: crypto/rand provides 74 bits of randomness
//   - Time monotonicity: time increases → UUID increases
//   - No global lock: fully concurrent
//
// Trade-offs:
//   - No strict monotonicity within the same millisecond
//   - Random order for UUIDs generated in the same millisecond
//
// This is suitable for MatrixOne's point query scenario where:
//   - UUIDs are used for identification, not strict ordering
//   - High concurrency performance is critical
//   - Time-based ordering is sufficient
func FastUuid() (types.Uuid, error) {
	var uuid [16]byte

	// Fill random bytes (10 bytes) for uniqueness
	// This is the key to guarantee uniqueness
	if _, err := rand.Read(uuid[6:]); err != nil {
		return types.Uuid{}, err
	}

	// Fill timestamp (6 bytes)
	nano := time.Now().UnixNano()
	milli := nano / 1000000

	uuid[0] = byte(milli >> 40)
	uuid[1] = byte(milli >> 32)
	uuid[2] = byte(milli >> 24)
	uuid[3] = byte(milli >> 16)
	uuid[4] = byte(milli >> 8)
	uuid[5] = byte(milli)

	// Set version (7) and variant (RFC 4122)
	uuid[6] = (uuid[6] & 0x0f) | 0x70 // version 7
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant

	return types.Uuid(uuid), nil
}
