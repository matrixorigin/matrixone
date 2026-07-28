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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestFastUuid_Uniqueness tests that generated UUIDs are unique
func TestFastUuid_Uniqueness(t *testing.T) {
	const count = 100000
	seen := make(map[[16]byte]bool, count)

	for i := 0; i < count; i++ {
		u, err := FastUuid()
		require.NoError(t, err)

		// Check uniqueness
		require.False(t, seen[u], "UUID collision detected: %v", u)
		seen[u] = true
	}
}

// TestFastUuid_TimeMonotonicity tests time-based monotonicity
func TestFastUuid_TimeMonotonicity(t *testing.T) {
	// Test that UUIDs from different milliseconds are ordered by time
	u1, err := FastUuid()
	require.NoError(t, err)

	// Wait for time to advance
	time.Sleep(2 * time.Millisecond)

	u2, err := FastUuid()
	require.NoError(t, err)

	// u2 should be greater than u1 (time has advanced)
	require.True(t, u2.Gt(u1), "UUID should increase with time")

	// Test multiple iterations
	for i := 0; i < 100; i++ {
		prev, err := FastUuid()
		require.NoError(t, err)

		time.Sleep(time.Millisecond)

		current, err := FastUuid()
		require.NoError(t, err)

		require.True(t, current.Gt(prev), "UUID should increase when time advances")
	}
}

// TestFastUuid_Concurrent tests concurrent UUID generation
func TestFastUuid_Concurrent(t *testing.T) {
	const goroutines = 100
	const perGoroutine = 1000

	var wg sync.WaitGroup
	results := make([][][16]byte, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			uuids := make([][16]byte, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				u, err := FastUuid()
				require.NoError(t, err)
				uuids[i] = u
			}
			results[idx] = uuids
		}(g)
	}

	wg.Wait()

	// Check uniqueness across all goroutines
	seen := make(map[[16]byte]bool, goroutines*perGoroutine)
	for _, uuids := range results {
		for _, u := range uuids {
			require.False(t, seen[u], "UUID collision in concurrent test")
			seen[u] = true
		}
	}
}

// TestFastUuid_Format tests UUID v7 format compliance
func TestFastUuid_Format(t *testing.T) {
	u, err := FastUuid()
	require.NoError(t, err)

	// Check version (should be 7)
	version := (u[6] >> 4) & 0x0f
	require.Equal(t, byte(7), version, "UUID version should be 7")

	// Check variant (should be RFC 4122)
	variant := (u[8] >> 6) & 0x03
	require.Equal(t, byte(2), variant, "UUID variant should be RFC 4122 (10b)")
}

// BenchmarkGoogleUUID benchmarks google/uuid.NewV7()
func BenchmarkGoogleUUID(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = uuid.NewV7()
	}
}

// BenchmarkFastUuid benchmarks our implementation
func BenchmarkFastUuid(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FastUuid()
	}
}

// BenchmarkGoogleUUID_Parallel benchmarks google/uuid in parallel
func BenchmarkGoogleUUID_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = uuid.NewV7()
		}
	})
}

// BenchmarkFastUuid_Parallel benchmarks our implementation in parallel
func BenchmarkFastUuid_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = FastUuid()
		}
	})
}

// TestFastUuid_Compatibility tests compatibility with google/uuid
func TestFastUuid_Compatibility(t *testing.T) {
	// Generate UUID using FastUuid
	fastUuid, err := FastUuid()
	require.NoError(t, err)

	// Convert to google/uuid.UUID
	googleUuid := uuid.UUID(fastUuid)

	// Test 1: String format should be valid
	str := googleUuid.String()
	require.Len(t, str, 36, "UUID string should be 36 characters")
	require.Contains(t, str, "-", "UUID string should contain hyphens")

	// Test 2: Parse back should work
	parsed, err := uuid.Parse(str)
	require.NoError(t, err)
	require.Equal(t, googleUuid, parsed, "Parsed UUID should match original")

	// Test 3: Version should be 7
	require.Equal(t, uuid.Version(7), googleUuid.Version(), "UUID version should be 7")

	// Test 4: Variant should be RFC 4122
	require.Equal(t, uuid.RFC4122, googleUuid.Variant(), "UUID variant should be RFC 4122")

	// Test 5: Can be used in google/uuid functions
	require.NotEqual(t, uuid.Nil, googleUuid, "UUID should not be nil")
	require.True(t, googleUuid != uuid.Nil, "UUID should not equal Nil")
}

// TestFastUuid_InteroperabilityWithGoogleUuid tests that FastUuid and google/uuid can be mixed
func TestFastUuid_InteroperabilityWithGoogleUuid(t *testing.T) {
	// Generate UUIDs from both implementations
	fastUuid1, err := FastUuid()
	require.NoError(t, err)

	googleUuid1, err := uuid.NewV7()
	require.NoError(t, err)

	fastUuid2, err := FastUuid()
	require.NoError(t, err)

	// Convert to same type for comparison
	fast1 := uuid.UUID(fastUuid1)
	google1 := googleUuid1
	fast2 := uuid.UUID(fastUuid2)

	// All should be unique
	require.NotEqual(t, fast1, google1)
	require.NotEqual(t, fast1, fast2)
	require.NotEqual(t, google1, fast2)

	// All should be valid UUID v7
	require.Equal(t, uuid.Version(7), fast1.Version())
	require.Equal(t, uuid.Version(7), google1.Version())
	require.Equal(t, uuid.Version(7), fast2.Version())

	// All should have RFC 4122 variant
	require.Equal(t, uuid.RFC4122, fast1.Variant())
	require.Equal(t, uuid.RFC4122, google1.Variant())
	require.Equal(t, uuid.RFC4122, fast2.Variant())
}

// TestFastUuid_DatabaseCompatibility tests that FastUuid can be stored/retrieved like google/uuid
func TestFastUuid_DatabaseCompatibility(t *testing.T) {
	fastUuid, err := FastUuid()
	require.NoError(t, err)

	// Test binary format (16 bytes)
	require.Len(t, fastUuid, 16, "UUID should be 16 bytes")

	// Test string format
	googleUuid := uuid.UUID(fastUuid)
	str := googleUuid.String()

	// Parse back from string
	parsed, err := uuid.Parse(str)
	require.NoError(t, err)
	require.Equal(t, fastUuid[:], parsed[:], "Binary representation should match")

	// Test MarshalText/UnmarshalText (used by JSON)
	text, err := googleUuid.MarshalText()
	require.NoError(t, err)

	var unmarshaled uuid.UUID
	err = unmarshaled.UnmarshalText(text)
	require.NoError(t, err)
	require.Equal(t, googleUuid, unmarshaled, "Marshaled UUID should unmarshal correctly")
}
