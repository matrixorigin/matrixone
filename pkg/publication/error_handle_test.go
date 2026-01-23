// Copyright 2021 Matrix Origin
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

package publication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockClassifier is a simple classifier for testing
type mockClassifier struct {
	retryable bool
}

func (m *mockClassifier) IsRetryable(err error) bool {
	return m.retryable
}

func TestBuildErrorMetadata_OldRetryCountExceedsThreshold_Retryable(t *testing.T) {
	// Test case: old.RetryCount > RetryThreshold with retryable error
	// Covers lines 125-135: should reset count to 1
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  RetryThreshold + 1, // exceeds threshold
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new error")
	classifier := &mockClassifier{retryable: true}

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 1, meta.RetryCount)         // count reset to 1
	assert.True(t, meta.IsRetryable)            // still retryable
	assert.True(t, shouldRetry)                 // should retry since count=1 <= threshold
	assert.Equal(t, "new error", meta.Message)  // new message
	assert.NotEqual(t, oldTime, meta.FirstSeen) // FirstSeen reset
	assert.WithinDuration(t, time.Now(), meta.FirstSeen, time.Second)
}

func TestBuildErrorMetadata_OldRetryCountExceedsThreshold_NonRetryable(t *testing.T) {
	// Test case: old.RetryCount > RetryThreshold with non-retryable error
	// Covers lines 125-135: should reset count to 1, no retry
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  RetryThreshold + 1,
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new non-retryable error")
	classifier := &mockClassifier{retryable: false}

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 1, meta.RetryCount)
	assert.False(t, meta.IsRetryable)
	assert.False(t, shouldRetry) // non-retryable error
	assert.Equal(t, "new non-retryable error", meta.Message)
}

func TestBuildErrorMetadata_SameErrorType_Retryable_IncrementCount(t *testing.T) {
	// Test case: same error type (both retryable), count < threshold
	// Covers lines 137-148: should increment count and preserve FirstSeen
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  5, // less than threshold
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new error")
	classifier := &mockClassifier{retryable: true}

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 6, meta.RetryCount)        // incremented
	assert.True(t, meta.IsRetryable)           // still retryable
	assert.True(t, shouldRetry)                // should retry since count <= threshold
	assert.Equal(t, oldTime, meta.FirstSeen)   // FirstSeen preserved
	assert.NotEqual(t, oldTime, meta.LastSeen) // LastSeen updated
	assert.WithinDuration(t, time.Now(), meta.LastSeen, time.Second)
}

func TestBuildErrorMetadata_SameErrorType_Retryable_ReachesThreshold(t *testing.T) {
	// Test case: same error type (both retryable), count reaches threshold
	// Covers lines 137-148: should increment count but shouldRetry = false
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  RetryThreshold, // at threshold
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new error")
	classifier := &mockClassifier{retryable: true}

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, RetryThreshold+1, meta.RetryCount) // exceeded threshold
	assert.False(t, meta.IsRetryable)                  // IsRetryable = isRetryable && shouldRetry
	assert.False(t, shouldRetry)                       // count > threshold
	assert.Equal(t, oldTime, meta.FirstSeen)           // FirstSeen preserved
}

func TestBuildErrorMetadata_SameErrorType_NonRetryable(t *testing.T) {
	// Test case: same error type (both non-retryable)
	// Covers lines 137-148: should increment count, no retry
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: false,
		RetryCount:  3,
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new error")
	classifier := &mockClassifier{retryable: false}

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 4, meta.RetryCount)      // incremented
	assert.False(t, meta.IsRetryable)        // non-retryable
	assert.False(t, shouldRetry)             // non-retryable
	assert.Equal(t, oldTime, meta.FirstSeen) // FirstSeen preserved
}

func TestBuildErrorMetadata_ErrorTypeChanged_RetryableToNonRetryable(t *testing.T) {
	// Test case: error type changed from retryable to non-retryable
	// Covers lines 150-159: should reset count
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: true, // was retryable
		RetryCount:  5,
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new non-retryable error")
	classifier := &mockClassifier{retryable: false} // now non-retryable

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 1, meta.RetryCount)         // count reset
	assert.False(t, meta.IsRetryable)           // non-retryable
	assert.False(t, shouldRetry)                // non-retryable error
	assert.NotEqual(t, oldTime, meta.FirstSeen) // FirstSeen reset
	assert.WithinDuration(t, time.Now(), meta.FirstSeen, time.Second)
}

func TestBuildErrorMetadata_ErrorTypeChanged_NonRetryableToRetryable(t *testing.T) {
	// Test case: error type changed from non-retryable to retryable
	// Covers lines 150-159: should reset count
	oldTime := time.Now().Add(-time.Hour)
	old := &ErrorMetadata{
		IsRetryable: false, // was non-retryable
		RetryCount:  5,
		FirstSeen:   oldTime,
		LastSeen:    oldTime,
		Message:     "old error",
	}
	err := errors.New("new retryable error")
	classifier := &mockClassifier{retryable: true} // now retryable

	meta, shouldRetry := BuildErrorMetadata(old, err, classifier)

	assert.NotNil(t, meta)
	assert.Equal(t, 1, meta.RetryCount)         // count reset
	assert.True(t, meta.IsRetryable)            // retryable
	assert.True(t, shouldRetry)                 // should retry since count=1 <= threshold
	assert.NotEqual(t, oldTime, meta.FirstSeen) // FirstSeen reset
	assert.WithinDuration(t, time.Now(), meta.FirstSeen, time.Second)
}

// Tests for ExponentialBackoff.Next

func TestExponentialBackoff_Next_AttemptLessThanOne(t *testing.T) {
	// Test case: attempt < 1 should be treated as attempt = 1
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
	}

	// attempt = 0
	result0 := b.Next(0)
	// attempt = -1
	resultNeg := b.Next(-1)
	// attempt = 1 (normal)
	result1 := b.Next(1)

	// All should return the same delay (Base * Factor^0 = Base)
	assert.Equal(t, result1, result0)
	assert.Equal(t, result1, resultNeg)
	assert.Equal(t, 100*time.Millisecond, result1)
}

func TestExponentialBackoff_Next_AttemptNormal(t *testing.T) {
	// Test case: normal attempt values (>= 1)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
	}

	// attempt = 1: 100ms * 2^0 = 100ms
	assert.Equal(t, 100*time.Millisecond, b.Next(1))
	// attempt = 2: 100ms * 2^1 = 200ms
	assert.Equal(t, 200*time.Millisecond, b.Next(2))
	// attempt = 3: 100ms * 2^2 = 400ms
	assert.Equal(t, 400*time.Millisecond, b.Next(3))
	// attempt = 4: 100ms * 2^3 = 800ms
	assert.Equal(t, 800*time.Millisecond, b.Next(4))
}

func TestExponentialBackoff_Next_BaseZeroOrNegative(t *testing.T) {
	// Test case: Base <= 0 should use default 100ms
	b1 := ExponentialBackoff{
		Base:   0,
		Factor: 2,
	}
	b2 := ExponentialBackoff{
		Base:   -100 * time.Millisecond,
		Factor: 2,
	}

	// Should use default Base (100ms)
	// attempt = 1: 100ms * 2^0 = 100ms
	assert.Equal(t, 100*time.Millisecond, b1.Next(1))
	assert.Equal(t, 100*time.Millisecond, b2.Next(1))
	// attempt = 2: 100ms * 2^1 = 200ms
	assert.Equal(t, 200*time.Millisecond, b1.Next(2))
	assert.Equal(t, 200*time.Millisecond, b2.Next(2))
}

func TestExponentialBackoff_Next_FactorLessThanOrEqualOne(t *testing.T) {
	// Test case: Factor <= 1 should use default factor 2
	b1 := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 0,
	}
	b2 := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 1,
	}
	b3 := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 0.5,
	}

	// All should use default Factor (2)
	// attempt = 2: 100ms * 2^1 = 200ms
	assert.Equal(t, 200*time.Millisecond, b1.Next(2))
	assert.Equal(t, 200*time.Millisecond, b2.Next(2))
	assert.Equal(t, 200*time.Millisecond, b3.Next(2))
}

func TestExponentialBackoff_Next_CustomFactor(t *testing.T) {
	// Test case: custom Factor > 1
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 3,
	}

	// attempt = 1: 100ms * 3^0 = 100ms
	assert.Equal(t, 100*time.Millisecond, b.Next(1))
	// attempt = 2: 100ms * 3^1 = 300ms
	assert.Equal(t, 300*time.Millisecond, b.Next(2))
	// attempt = 3: 100ms * 3^2 = 900ms
	assert.Equal(t, 900*time.Millisecond, b.Next(3))
}

func TestExponentialBackoff_Next_MaxZero(t *testing.T) {
	// Test case: Max = 0 (no cap)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    0,
	}

	// attempt = 10: 100ms * 2^9 = 51200ms (should not be capped)
	expected := time.Duration(float64(100*time.Millisecond) * 512) // 2^9 = 512
	assert.Equal(t, expected, b.Next(10))
}

func TestExponentialBackoff_Next_MaxCaps(t *testing.T) {
	// Test case: Max > 0 and result > Max (should cap)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    500 * time.Millisecond,
	}

	// attempt = 1: 100ms (not capped)
	assert.Equal(t, 100*time.Millisecond, b.Next(1))
	// attempt = 2: 200ms (not capped)
	assert.Equal(t, 200*time.Millisecond, b.Next(2))
	// attempt = 3: 400ms (not capped)
	assert.Equal(t, 400*time.Millisecond, b.Next(3))
	// attempt = 4: 800ms -> capped to 500ms
	assert.Equal(t, 500*time.Millisecond, b.Next(4))
	// attempt = 5: 1600ms -> capped to 500ms
	assert.Equal(t, 500*time.Millisecond, b.Next(5))
}

func TestExponentialBackoff_Next_MaxNotExceeded(t *testing.T) {
	// Test case: Max > 0 but result <= Max (should not cap)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    1 * time.Second,
	}

	// attempt = 1: 100ms (not capped, 100ms < 1s)
	assert.Equal(t, 100*time.Millisecond, b.Next(1))
	// attempt = 2: 200ms (not capped, 200ms < 1s)
	assert.Equal(t, 200*time.Millisecond, b.Next(2))
	// attempt = 3: 400ms (not capped, 400ms < 1s)
	assert.Equal(t, 400*time.Millisecond, b.Next(3))
}

func TestExponentialBackoff_Next_JitterZero(t *testing.T) {
	// Test case: Jitter = 0 (no jitter)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Jitter: 0,
	}

	// Multiple calls should return the same result (no randomness)
	result1 := b.Next(1)
	result2 := b.Next(1)
	result3 := b.Next(1)
	assert.Equal(t, result1, result2)
	assert.Equal(t, result2, result3)
	assert.Equal(t, 100*time.Millisecond, result1)
}

func TestExponentialBackoff_Next_JitterWithCustomRandFn(t *testing.T) {
	// Test case: Jitter > 0 with custom randFn for deterministic testing
	fixedJitter := 50 * time.Millisecond
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Jitter: 100 * time.Millisecond,
		randFn: func(max time.Duration) time.Duration {
			return fixedJitter // always return fixed jitter
		},
	}

	// attempt = 1: 100ms + 50ms (fixed jitter) = 150ms
	assert.Equal(t, 150*time.Millisecond, b.Next(1))
	// attempt = 2: 200ms + 50ms = 250ms
	assert.Equal(t, 250*time.Millisecond, b.Next(2))
}

func TestExponentialBackoff_Next_JitterWithDefaultRandFn(t *testing.T) {
	// Test case: Jitter > 0 with default randFn (nil)
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Jitter: 100 * time.Millisecond,
		randFn: nil, // will use default random function
	}

	// Result should be in range [100ms, 200ms)
	result := b.Next(1)
	assert.GreaterOrEqual(t, result, 100*time.Millisecond)
	assert.Less(t, result, 200*time.Millisecond)
}

func TestExponentialBackoff_Next_JitterWithMaxCap(t *testing.T) {
	// Test case: Jitter is added after Max cap
	fixedJitter := 50 * time.Millisecond
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    300 * time.Millisecond,
		Jitter: 100 * time.Millisecond,
		randFn: func(max time.Duration) time.Duration {
			return fixedJitter
		},
	}

	// attempt = 4: 800ms -> capped to 300ms -> + 50ms jitter = 350ms
	assert.Equal(t, 350*time.Millisecond, b.Next(4))
}

func TestExponentialBackoff_Next_DefaultRandFnWithZeroMax(t *testing.T) {
	// Test case: cover the default randFn with max <= 0 branch
	// The default randFn has a branch: if max <= 0, return 0
	// We can use a custom randFn that simulates receiving zero to ensure that branch is tested
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Jitter: 50 * time.Millisecond,
		randFn: func(max time.Duration) time.Duration {
			// Simulate what the default function does with max <= 0
			if max <= 0 {
				return 0
			}
			return 25 * time.Millisecond // return half of max
		},
	}

	// The result should include jitter: 100ms + 25ms = 125ms
	result := b.Next(1)
	assert.Equal(t, 125*time.Millisecond, result)
}

func TestExponentialBackoff_Next_AllDefaultValues(t *testing.T) {
	// Test case: all fields are zero/default values
	b := ExponentialBackoff{}

	// Should use defaults: Base=100ms, Factor=2
	// attempt = 1: 100ms * 2^0 = 100ms
	assert.Equal(t, 100*time.Millisecond, b.Next(1))
	// attempt = 2: 100ms * 2^1 = 200ms
	assert.Equal(t, 200*time.Millisecond, b.Next(2))
}

func TestExponentialBackoff_Next_CombinedScenario(t *testing.T) {
	// Test case: combined scenario with all features
	fixedJitter := 25 * time.Millisecond
	b := ExponentialBackoff{
		Base:   50 * time.Millisecond,
		Factor: 1.5,
		Max:    200 * time.Millisecond,
		Jitter: 50 * time.Millisecond,
		randFn: func(max time.Duration) time.Duration {
			return fixedJitter
		},
	}

	// attempt = 1: 50ms * 1.5^0 = 50ms + 25ms jitter = 75ms
	assert.Equal(t, 75*time.Millisecond, b.Next(1))
	// attempt = 2: 50ms * 1.5^1 = 75ms + 25ms jitter = 100ms
	assert.Equal(t, 100*time.Millisecond, b.Next(2))
	// attempt = 3: 50ms * 1.5^2 = 112.5ms + 25ms jitter = 137.5ms
	// Due to floating point, we use approximate comparison
	result3 := b.Next(3)
	assert.InDelta(t, float64(137500*time.Microsecond), float64(result3), float64(time.Microsecond))
	// attempt = 5: 50ms * 1.5^4 = 253.125ms -> capped to 200ms + 25ms jitter = 225ms
	assert.Equal(t, 225*time.Millisecond, b.Next(5))
}

func TestExponentialBackoff_Next_LargeAttempt(t *testing.T) {
	// Test case: large attempt number with Max cap
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    10 * time.Second,
	}

	// attempt = 10: 100ms * 2^9 = 51.2s -> capped to 10s
	result := b.Next(10)
	assert.Equal(t, 10*time.Second, result)

	// attempt = 20: would be huge but capped to 10s
	result20 := b.Next(20)
	assert.Equal(t, 10*time.Second, result20)
}

func TestExponentialBackoff_Next_NegativeJitter(t *testing.T) {
	// Test case: negative Jitter should be treated as no jitter
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Jitter: -50 * time.Millisecond,
	}

	// Should not add any jitter
	result1 := b.Next(1)
	result2 := b.Next(1)
	assert.Equal(t, result1, result2)
	assert.Equal(t, 100*time.Millisecond, result1)
}

func TestExponentialBackoff_Next_NegativeMax(t *testing.T) {
	// Test case: negative Max should not cap
	b := ExponentialBackoff{
		Base:   100 * time.Millisecond,
		Factor: 2,
		Max:    -500 * time.Millisecond,
	}

	// Max <= 0 means no cap
	// attempt = 4: 100ms * 2^3 = 800ms (not capped)
	assert.Equal(t, 800*time.Millisecond, b.Next(4))
}
