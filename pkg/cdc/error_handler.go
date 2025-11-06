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
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Error handling constants
const (
	MaxRetryCount           = 3
	ErrorExpirationDuration = 1 * time.Hour
)

// ErrorRecord represents a CDC error with metadata for recording
type ErrorRecord struct {
	Error           error     // The original error
	IsRetryable     bool      // Whether this error is retryable
	IsPauseOrCancel bool      // Whether this is a pause/cancel control signal
	Timestamp       time.Time // When the error occurred
}

// ErrorMetadata stores error metadata parsed from database
type ErrorMetadata struct {
	IsRetryable bool      // Whether this error is retryable
	RetryCount  int       // Number of retry attempts
	FirstSeen   time.Time // When first seen
	LastSeen    time.Time // When last seen
	Message     string    // Error message
}

// IsPauseOrCancelError checks if the error is a control signal (pause/cancel)
func IsPauseOrCancelError(errMsg string) bool {
	if errMsg == "" {
		return false
	}
	return strings.Contains(errMsg, "paused") ||
		strings.Contains(errMsg, "cancelled") ||
		strings.Contains(errMsg, "context canceled")
}

// ParseErrorMetadata parses error message to metadata
// Format:
//   - Retryable: "R:count:firstSeen:lastSeen:message"
//   - Non-retryable: "N:firstSeen:message"
//   - Legacy retryable: "retryable error:message"
//   - Legacy non-retryable: "message"
func ParseErrorMetadata(errMsg string) *ErrorMetadata {
	if errMsg == "" {
		return nil
	}

	parts := strings.SplitN(errMsg, ":", 5)

	// New retryable format: "R:count:firstSeen:lastSeen:message"
	if len(parts) >= 5 && parts[0] == "R" {
		retryCount, _ := strconv.Atoi(parts[1])
		firstSeen, _ := strconv.ParseInt(parts[2], 10, 64)
		lastSeen, _ := strconv.ParseInt(parts[3], 10, 64)

		return &ErrorMetadata{
			IsRetryable: true,
			RetryCount:  retryCount,
			FirstSeen:   time.Unix(firstSeen, 0),
			LastSeen:    time.Unix(lastSeen, 0),
			Message:     parts[4],
		}
	}

	// New non-retryable format: "N:firstSeen:message"
	if len(parts) >= 3 && parts[0] == "N" {
		firstSeen, _ := strconv.ParseInt(parts[1], 10, 64)
		message := strings.Join(parts[2:], ":")

		return &ErrorMetadata{
			IsRetryable: false,
			RetryCount:  0,
			FirstSeen:   time.Unix(firstSeen, 0),
			Message:     message,
		}
	}

	// Legacy retryable format: "retryable error:message"
	if strings.HasPrefix(errMsg, RetryableErrorPrefix) {
		return &ErrorMetadata{
			IsRetryable: true,
			RetryCount:  1,
			FirstSeen:   time.Now(), // Unknown, use current time
			LastSeen:    time.Now(),
			Message:     strings.TrimPrefix(errMsg, RetryableErrorPrefix),
		}
	}

	// Legacy non-retryable format: just the message
	return &ErrorMetadata{
		IsRetryable: false,
		RetryCount:  0,
		FirstSeen:   time.Now(), // Unknown, use current time
		Message:     errMsg,
	}
}

// FormatErrorMetadata formats error metadata to string
func FormatErrorMetadata(meta *ErrorMetadata) string {
	if meta.IsRetryable {
		return fmt.Sprintf("R:%d:%d:%d:%s",
			meta.RetryCount,
			meta.FirstSeen.Unix(),
			meta.LastSeen.Unix(),
			meta.Message,
		)
	}
	return fmt.Sprintf("N:%d:%s",
		meta.FirstSeen.Unix(),
		meta.Message,
	)
}

// BuildErrorMetadata builds new metadata based on old metadata and new error record
func BuildErrorMetadata(old *ErrorMetadata, record *ErrorRecord) *ErrorMetadata {
	now := record.Timestamp
	if now.IsZero() {
		now = time.Now()
	}

	message := record.Error.Error()

	// New error (no previous metadata)
	if old == nil {
		return &ErrorMetadata{
			IsRetryable: record.IsRetryable,
			RetryCount:  1,
			FirstSeen:   now,
			LastSeen:    now,
			Message:     message,
		}
	}

	// Same error type, increment retry count
	if old.IsRetryable == record.IsRetryable {
		return &ErrorMetadata{
			IsRetryable: record.IsRetryable,
			RetryCount:  old.RetryCount + 1,
			FirstSeen:   old.FirstSeen, // Preserve first seen time
			LastSeen:    now,           // Update last seen time
			Message:     message,
		}
	}

	// Error type changed, reset count
	return &ErrorMetadata{
		IsRetryable: record.IsRetryable,
		RetryCount:  1,
		FirstSeen:   now,
		LastSeen:    now,
		Message:     message,
	}
}

// IsErrorExpired checks if a non-retryable error has expired
func IsErrorExpired(meta *ErrorMetadata) bool {
	if meta == nil || meta.IsRetryable {
		return false
	}
	return time.Since(meta.FirstSeen) > ErrorExpirationDuration
}

// ShouldRetry determines if an error should allow retry
func ShouldRetry(meta *ErrorMetadata) bool {
	if meta == nil {
		return true // No error, allow
	}

	// Retryable error: check retry count
	if meta.IsRetryable {
		return meta.RetryCount <= MaxRetryCount
	}

	// Non-retryable error: check if expired
	return IsErrorExpired(meta)
}
