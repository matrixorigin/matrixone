// Copyright 2024 Matrix Origin
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

package cdc

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsPauseOrCancelError(t *testing.T) {
	tests := []struct {
		name     string
		errMsg   string
		expected bool
	}{
		{"empty", "", false},
		{"paused", "paused", true},
		{"cancelled", "cancelled", true},
		{"context canceled", "context canceled", true},
		{"normal error", "table not found", false},
		{"pause in message", "task was paused by user", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPauseOrCancelError(tt.errMsg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseErrorMetadata(t *testing.T) {
	tests := []struct {
		name     string
		errMsg   string
		expected *ErrorMetadata
	}{
		{
			name:     "empty",
			errMsg:   "",
			expected: nil,
		},
		{
			name:   "new retryable format",
			errMsg: "R:2:1704528000:1704528100:table not found",
			expected: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  2,
				FirstSeen:   time.Unix(1704528000, 0),
				LastSeen:    time.Unix(1704528100, 0),
				Message:     "table not found",
			},
		},
		{
			name:   "new non-retryable format",
			errMsg: "N:1704528000:type mismatch",
			expected: &ErrorMetadata{
				IsRetryable: false,
				RetryCount:  0,
				FirstSeen:   time.Unix(1704528000, 0),
				Message:     "type mismatch",
			},
		},
		{
			name:   "legacy retryable format",
			errMsg: "retryable error:table not found",
			expected: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  1,
				Message:     "table not found",
			},
		},
		{
			name:   "legacy non-retryable format",
			errMsg: "some error message",
			expected: &ErrorMetadata{
				IsRetryable: false,
				RetryCount:  0,
				Message:     "some error message",
			},
		},
		{
			name:   "message with colons",
			errMsg: "N:1704528000:error: connection failed: timeout",
			expected: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Unix(1704528000, 0),
				Message:     "error: connection failed: timeout",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseErrorMetadata(tt.errMsg)

			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}

			assert.NotNil(t, result)
			assert.Equal(t, tt.expected.IsRetryable, result.IsRetryable)
			assert.Equal(t, tt.expected.RetryCount, result.RetryCount)
			assert.Equal(t, tt.expected.Message, result.Message)

			// For new formats, check timestamps
			if strings.HasPrefix(tt.errMsg, "R:") || strings.HasPrefix(tt.errMsg, "N:") {
				assert.Equal(t, tt.expected.FirstSeen.Unix(), result.FirstSeen.Unix())
				if strings.HasPrefix(tt.errMsg, "R:") {
					assert.Equal(t, tt.expected.LastSeen.Unix(), result.LastSeen.Unix())
				}
			}
		})
	}
}

func TestFormatErrorMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata *ErrorMetadata
		expected string
	}{
		{
			name: "retryable error",
			metadata: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  2,
				FirstSeen:   time.Unix(1704528000, 0),
				LastSeen:    time.Unix(1704528100, 0),
				Message:     "table not found",
			},
			expected: "R:2:1704528000:1704528100:table not found",
		},
		{
			name: "non-retryable error",
			metadata: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Unix(1704528000, 0),
				Message:     "type mismatch",
			},
			expected: "N:1704528000:type mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatErrorMetadata(tt.metadata)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildErrorMetadata(t *testing.T) {
	baseTime := time.Unix(1704528000, 0)
	laterTime := time.Unix(1704528100, 0)

	tests := []struct {
		name     string
		old      *ErrorMetadata
		record   *ErrorRecord
		expected *ErrorMetadata
	}{
		{
			name: "new retryable error",
			old:  nil,
			record: &ErrorRecord{
				Error:       errors.New("table not found"),
				IsRetryable: true,
				Timestamp:   baseTime,
			},
			expected: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  1,
				FirstSeen:   baseTime,
				LastSeen:    baseTime,
				Message:     "table not found",
			},
		},
		{
			name: "increment retry count",
			old: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  1,
				FirstSeen:   baseTime,
				LastSeen:    baseTime,
				Message:     "table not found",
			},
			record: &ErrorRecord{
				Error:       errors.New("table not found"),
				IsRetryable: true,
				Timestamp:   laterTime,
			},
			expected: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  2,
				FirstSeen:   baseTime,  // Preserved
				LastSeen:    laterTime, // Updated
				Message:     "table not found",
			},
		},
		{
			name: "error type changed",
			old: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  2,
				FirstSeen:   baseTime,
				LastSeen:    baseTime,
			},
			record: &ErrorRecord{
				Error:       errors.New("different error"),
				IsRetryable: false,
				Timestamp:   laterTime,
			},
			expected: &ErrorMetadata{
				IsRetryable: false,
				RetryCount:  1,         // Reset
				FirstSeen:   laterTime, // Reset
				LastSeen:    laterTime,
				Message:     "different error",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildErrorMetadata(tt.old, tt.record)
			assert.Equal(t, tt.expected.IsRetryable, result.IsRetryable)
			assert.Equal(t, tt.expected.RetryCount, result.RetryCount)
			assert.Equal(t, tt.expected.FirstSeen.Unix(), result.FirstSeen.Unix())
			assert.Equal(t, tt.expected.LastSeen.Unix(), result.LastSeen.Unix())
			assert.Equal(t, tt.expected.Message, result.Message)
		})
	}
}

func TestIsErrorExpired(t *testing.T) {
	tests := []struct {
		name     string
		metadata *ErrorMetadata
		expected bool
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			expected: false,
		},
		{
			name: "retryable error",
			metadata: &ErrorMetadata{
				IsRetryable: true,
				FirstSeen:   time.Now().Add(-2 * time.Hour),
			},
			expected: false, // Retryable errors don't expire
		},
		{
			name: "fresh non-retryable error",
			metadata: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Now().Add(-30 * time.Minute),
			},
			expected: false,
		},
		{
			name: "expired non-retryable error",
			metadata: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Now().Add(-2 * time.Hour),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsErrorExpired(tt.metadata)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name     string
		metadata *ErrorMetadata
		expected bool
	}{
		{
			name:     "no error",
			metadata: nil,
			expected: true,
		},
		{
			name: "retryable under limit",
			metadata: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  2,
			},
			expected: true,
		},
		{
			name: "retryable at limit",
			metadata: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  MaxRetryCount,
			},
			expected: true,
		},
		{
			name: "retryable over limit",
			metadata: &ErrorMetadata{
				IsRetryable: true,
				RetryCount:  MaxRetryCount + 1,
			},
			expected: false,
		},
		{
			name: "non-retryable fresh",
			metadata: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Now().Add(-30 * time.Minute),
			},
			expected: false,
		},
		{
			name: "non-retryable expired",
			metadata: &ErrorMetadata{
				IsRetryable: false,
				FirstSeen:   time.Now().Add(-2 * time.Hour),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldRetry(tt.metadata)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseAndFormat_RoundTrip(t *testing.T) {
	original := &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  3,
		FirstSeen:   time.Unix(1704528000, 0),
		LastSeen:    time.Unix(1704528300, 0),
		Message:     "table not found",
	}

	// Format
	formatted := FormatErrorMetadata(original)
	assert.Equal(t, "R:3:1704528000:1704528300:table not found", formatted)

	// Parse back
	parsed := ParseErrorMetadata(formatted)
	assert.NotNil(t, parsed)
	assert.Equal(t, original.IsRetryable, parsed.IsRetryable)
	assert.Equal(t, original.RetryCount, parsed.RetryCount)
	assert.Equal(t, original.FirstSeen.Unix(), parsed.FirstSeen.Unix())
	assert.Equal(t, original.LastSeen.Unix(), parsed.LastSeen.Unix())
	assert.Equal(t, original.Message, parsed.Message)
}

func TestBuildErrorMetadata_MultipleRetries(t *testing.T) {
	baseTime := time.Unix(1704528000, 0)

	// First error
	record1 := &ErrorRecord{
		Error:       errors.New("table not found"),
		IsRetryable: true,
		Timestamp:   baseTime,
	}
	meta1 := BuildErrorMetadata(nil, record1)
	assert.Equal(t, 1, meta1.RetryCount)
	assert.Equal(t, baseTime.Unix(), meta1.FirstSeen.Unix())

	// Second error (1 minute later)
	record2 := &ErrorRecord{
		Error:       errors.New("table not found"),
		IsRetryable: true,
		Timestamp:   baseTime.Add(1 * time.Minute),
	}
	meta2 := BuildErrorMetadata(meta1, record2)
	assert.Equal(t, 2, meta2.RetryCount)
	assert.Equal(t, baseTime.Unix(), meta2.FirstSeen.Unix()) // Preserved
	assert.Equal(t, baseTime.Add(1*time.Minute).Unix(), meta2.LastSeen.Unix())

	// Third error (2 minutes later)
	record3 := &ErrorRecord{
		Error:       errors.New("table not found"),
		IsRetryable: true,
		Timestamp:   baseTime.Add(2 * time.Minute),
	}
	meta3 := BuildErrorMetadata(meta2, record3)
	assert.Equal(t, 3, meta3.RetryCount)
	assert.Equal(t, baseTime.Unix(), meta3.FirstSeen.Unix()) // Still preserved
	assert.Equal(t, baseTime.Add(2*time.Minute).Unix(), meta3.LastSeen.Unix())
}
