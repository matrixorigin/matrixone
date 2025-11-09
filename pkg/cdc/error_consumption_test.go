package cdc

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorConsumption_ShouldRetryScenarios verifies all error consumption scenarios
func TestErrorConsumption_ShouldRetryScenarios(t *testing.T) {
	testCases := []struct {
		name        string
		errMsg      string
		shouldRetry bool
		description string
	}{
		{
			name:        "No Error (empty)",
			errMsg:      "",
			shouldRetry: false, // Will be nil metadata, ShouldRetry returns false
			description: "Empty error message means no error, don't retry",
		},
		{
			name:        "Retryable - Count 1",
			errMsg:      fmt.Sprintf("R:1:%d:%d:connection timeout", time.Now().Unix(), time.Now().Unix()),
			shouldRetry: true,
			description: "First retry attempt should continue",
		},
		{
			name:        "Retryable - Count 2",
			errMsg:      fmt.Sprintf("R:2:%d:%d:connection timeout", time.Now().Unix(), time.Now().Unix()),
			shouldRetry: true,
			description: "Second retry attempt should continue",
		},
		{
			name:        "Retryable - Count 3 (at limit)",
			errMsg:      fmt.Sprintf("R:3:%d:%d:connection timeout", time.Now().Unix(), time.Now().Unix()),
			shouldRetry: true,
			description: "Third retry attempt (at MaxRetryCount) should continue",
		},
		{
			name:        "Retryable - Count 4 (exceeded)",
			errMsg:      fmt.Sprintf("R:4:%d:%d:connection timeout", time.Now().Unix(), time.Now().Unix()),
			shouldRetry: false,
			description: "Fourth retry attempt (exceeded MaxRetryCount=3) should stop",
		},
		{
			name:        "Non-Retryable",
			errMsg:      fmt.Sprintf("N:%d:type mismatch error", time.Now().Unix()),
			shouldRetry: false,
			description: "Non-retryable error should stop immediately",
		},
		{
			name:        "Max Retry Exceeded Message",
			errMsg:      fmt.Sprintf("N:%d:max retry exceeded (4): connection timeout", time.Now().Unix()),
			shouldRetry: false,
			description: "Auto-converted non-retryable error should stop",
		},
		{
			name:        "Old Non-Retryable Error",
			errMsg:      fmt.Sprintf("N:%d:old error", time.Now().Add(-2*time.Hour).Unix()),
			shouldRetry: false,
			description: "Non-retryable error should permanently block (no auto-expiration)",
		},
		{
			name:        "Legacy Retryable",
			errMsg:      "retryable error:connection timeout",
			shouldRetry: true,
			description: "Legacy retryable format should continue",
		},
		{
			name:        "Legacy Plain",
			errMsg:      "some random error",
			shouldRetry: false,
			description: "Legacy plain format (no prefix) should stop",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.errMsg == "" {
				// Empty error message
				metadata := ParseErrorMetadata(tc.errMsg)
				assert.Nil(t, metadata, "Empty error should return nil metadata")
				return
			}

			metadata := ParseErrorMetadata(tc.errMsg)
			require.NotNil(t, metadata, "Failed to parse: %s", tc.errMsg)

			result := ShouldRetry(metadata)
			assert.Equal(t, tc.shouldRetry, result, tc.description)

			// Additional assertions
			if result {
				// If should retry, must be retryable and count <= MaxRetryCount
				assert.True(t, metadata.IsRetryable, "Only retryable errors should allow retry")
				assert.LessOrEqual(t, metadata.RetryCount, MaxRetryCount,
					"Retry count should not exceed MaxRetryCount")
			} else {
				// If should not retry, either:
				// 1. Non-retryable (permanently blocked), OR
				// 2. Retryable but exceeded MaxRetryCount
				if metadata.IsRetryable {
					assert.Greater(t, metadata.RetryCount, MaxRetryCount,
						"Retryable error that shouldn't retry must have exceeded MaxRetryCount")
				}
				// Non-retryable errors always block (no assertion needed)
			}
		})
	}
}

// TestErrorConsumption_GetTableErrMsgLogic simulates GetTableErrMsg behavior
func TestErrorConsumption_GetTableErrMsgLogic(t *testing.T) {
	testCases := []struct {
		name        string
		errMsg      string
		hasError    bool // What GetTableErrMsg should return
		description string
	}{
		{
			name:        "No Error",
			errMsg:      "",
			hasError:    false,
			description: "Empty error -> hasError=false (table continues)",
		},
		{
			name:        "Retryable Count 1",
			errMsg:      fmt.Sprintf("R:1:%d:%d:error", time.Now().Unix(), time.Now().Unix()),
			hasError:    false,
			description: "Retryable (count=1) -> hasError=false (table continues)",
		},
		{
			name:        "Retryable Count 3",
			errMsg:      fmt.Sprintf("R:3:%d:%d:error", time.Now().Unix(), time.Now().Unix()),
			hasError:    false,
			description: "Retryable (count=3) -> hasError=false (table continues)",
		},
		{
			name:        "Retryable Count 4",
			errMsg:      fmt.Sprintf("R:4:%d:%d:error", time.Now().Unix(), time.Now().Unix()),
			hasError:    true,
			description: "Retryable (count=4, exceeded) -> hasError=true (table stops)",
		},
		{
			name:        "Non-Retryable",
			errMsg:      fmt.Sprintf("N:%d:error", time.Now().Unix()),
			hasError:    true,
			description: "Non-retryable -> hasError=true (table stops)",
		},
		{
			name:        "Max Retry Exceeded",
			errMsg:      fmt.Sprintf("N:%d:max retry exceeded (4): error", time.Now().Unix()),
			hasError:    true,
			description: "Max retry exceeded -> hasError=true (table stops)",
		},
		{
			name:        "Old Non-Retryable",
			errMsg:      fmt.Sprintf("N:%d:old error", time.Now().Add(-2*time.Hour).Unix()),
			hasError:    true,
			description: "Old non-retryable error -> hasError=true (permanently blocked)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate GetTableErrMsg logic
			if tc.errMsg == "" {
				// No error
				assert.False(t, tc.hasError, "Empty error should mean hasError=false")
				return
			}

			metadata := ParseErrorMetadata(tc.errMsg)
			require.NotNil(t, metadata)

			// GetTableErrMsg logic:
			// if ShouldRetry(metadata) { return false, nil } (continue)
			// else { return true, nil } (stop)
			shouldRetry := ShouldRetry(metadata)
			expectedHasError := !shouldRetry

			assert.Equal(t, tc.hasError, expectedHasError, tc.description)
		})
	}
}

// TestErrorConsumption_CompleteLifecycle tests the complete error lifecycle
func TestErrorConsumption_CompleteLifecycle(t *testing.T) {
	firstSeen := time.Now().Unix()

	// Stage 1: First error (retry 1) -> should continue
	errMsg1 := fmt.Sprintf("R:1:%d:%d:connection timeout", firstSeen, firstSeen)
	metadata1 := ParseErrorMetadata(errMsg1)
	require.NotNil(t, metadata1)
	assert.True(t, metadata1.IsRetryable)
	assert.Equal(t, 1, metadata1.RetryCount)
	assert.True(t, ShouldRetry(metadata1), "Stage 1: Should continue")

	// Stage 2: Second error (retry 2) -> should continue
	lastSeen2 := time.Now().Unix()
	errMsg2 := fmt.Sprintf("R:2:%d:%d:connection timeout", firstSeen, lastSeen2)
	metadata2 := ParseErrorMetadata(errMsg2)
	require.NotNil(t, metadata2)
	assert.True(t, metadata2.IsRetryable)
	assert.Equal(t, 2, metadata2.RetryCount)
	assert.True(t, ShouldRetry(metadata2), "Stage 2: Should continue")

	// Stage 3: Third error (retry 3) -> should continue
	lastSeen3 := time.Now().Unix()
	errMsg3 := fmt.Sprintf("R:3:%d:%d:connection timeout", firstSeen, lastSeen3)
	metadata3 := ParseErrorMetadata(errMsg3)
	require.NotNil(t, metadata3)
	assert.True(t, metadata3.IsRetryable)
	assert.Equal(t, 3, metadata3.RetryCount)
	assert.True(t, ShouldRetry(metadata3), "Stage 3: Should continue (at limit)")

	// Stage 4: Fourth error (retry 4) -> should STOP
	lastSeen4 := time.Now().Unix()
	errMsg4 := fmt.Sprintf("R:4:%d:%d:connection timeout", firstSeen, lastSeen4)
	metadata4 := ParseErrorMetadata(errMsg4)
	require.NotNil(t, metadata4)
	assert.True(t, metadata4.IsRetryable)
	assert.Equal(t, 4, metadata4.RetryCount)
	assert.False(t, ShouldRetry(metadata4), "Stage 4: Should STOP (exceeded limit)")

	// Stage 5: Auto-converted to non-retryable -> should STOP
	errMsg5 := fmt.Sprintf("N:%d:max retry exceeded (4): connection timeout", firstSeen)
	metadata5 := ParseErrorMetadata(errMsg5)
	require.NotNil(t, metadata5)
	assert.False(t, metadata5.IsRetryable)
	assert.Equal(t, 0, metadata5.RetryCount, "Non-retryable doesn't track count")
	assert.False(t, ShouldRetry(metadata5), "Stage 5: Should STOP (non-retryable)")
}

// TestErrorConsumption_EdgeCases tests edge cases
func TestErrorConsumption_EdgeCases(t *testing.T) {
	t.Run("Nil Metadata", func(t *testing.T) {
		result := ShouldRetry(nil)
		// ShouldRetry returns true for nil (no error = can retry)
		assert.True(t, result, "Nil metadata should return true (no error = can continue)")
	})

	t.Run("Malformed Error Message", func(t *testing.T) {
		// Invalid format should be parsed as legacy plain format (non-retryable)
		metadata := ParseErrorMetadata("R:invalid:format")
		require.NotNil(t, metadata)
		assert.False(t, metadata.IsRetryable, "Malformed message treated as non-retryable")
		assert.False(t, ShouldRetry(metadata))
	})

	t.Run("Boundary - Exactly MaxRetryCount", func(t *testing.T) {
		errMsg := fmt.Sprintf("R:%d:%d:%d:error", MaxRetryCount, time.Now().Unix(), time.Now().Unix())
		metadata := ParseErrorMetadata(errMsg)
		require.NotNil(t, metadata)
		assert.True(t, ShouldRetry(metadata), "Exactly at MaxRetryCount should continue")
	})

	t.Run("Boundary - MaxRetryCount + 1", func(t *testing.T) {
		errMsg := fmt.Sprintf("R:%d:%d:%d:error", MaxRetryCount+1, time.Now().Unix(), time.Now().Unix())
		metadata := ParseErrorMetadata(errMsg)
		require.NotNil(t, metadata)
		assert.False(t, ShouldRetry(metadata), "MaxRetryCount+1 should stop")
	})

	t.Run("Old Non-Retryable Error", func(t *testing.T) {
		// More than 1 hour ago
		firstSeen := time.Now().Add(-2 * time.Hour)
		errMsg := fmt.Sprintf("N:%d:error", firstSeen.Unix())
		metadata := ParseErrorMetadata(errMsg)
		require.NotNil(t, metadata)

		// Verify metadata
		assert.Equal(t, firstSeen.Unix(), metadata.FirstSeen.Unix(), "FirstSeen should match")
		assert.False(t, metadata.IsRetryable, "Should be non-retryable")

		// Note: IsErrorExpired is deprecated but still functional for backward compatibility
		isExpired := IsErrorExpired(metadata)
		assert.True(t, isExpired, "IsErrorExpired still works (deprecated)")

		// New behavior: Non-retryable errors permanently block (no auto-expiration)
		result := ShouldRetry(metadata)
		assert.False(t, result, "Non-retryable errors permanently block (no auto-clear)")
	})
}
