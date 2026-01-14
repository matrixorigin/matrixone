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
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// ErrorMetadata stores error metadata
type ErrorMetadata struct {
	IsRetryable bool      // Whether this error is retryable
	RetryCount  int       // Number of retry attempts
	FirstSeen   time.Time // When first seen
	LastSeen    time.Time // When last seen
	Message     string    // Error message
}

// Parse parses error metadata from string
// Format:
//   - Retryable: "R:count:firstSeen:lastSeen:message"
//   - Non-retryable: "N:firstSeen:message"
func Parse(errMsg string) *ErrorMetadata {
	if errMsg == "" {
		return nil
	}

	parts := strings.SplitN(errMsg, ":", 5)

	// Retryable format: "R:count:firstSeen:lastSeen:message"
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

	// Non-retryable format: "N:firstSeen:message"
	if len(parts) >= 3 && parts[0] == "N" {
		firstSeen, _ := strconv.ParseInt(parts[1], 10, 64)
		message := strings.Join(parts[2:], ":")

		return &ErrorMetadata{
			IsRetryable: false,
			RetryCount:  0,
			FirstSeen:   time.Unix(firstSeen, 0),
			LastSeen:    time.Unix(firstSeen, 0),
			Message:     message,
		}
	}
	// Legacy format: just the message (assume non-retryable)
	return &ErrorMetadata{
		IsRetryable: false,
		RetryCount:  0,
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
		Message:     errMsg,
	}
}

// BuildErrorMetadata builds new metadata based on old metadata and new error
func BuildErrorMetadata(old *ErrorMetadata, err error, isRetryable bool) *ErrorMetadata {
	now := time.Now()
	message := err.Error()

	// New error (no previous metadata)
	if old == nil {
		return &ErrorMetadata{
			IsRetryable: isRetryable,
			RetryCount:  1,
			FirstSeen:   now,
			LastSeen:    now,
			Message:     message,
		}
	}

	// Same error type, increment retry count
	if old.IsRetryable == isRetryable {
		return &ErrorMetadata{
			IsRetryable: isRetryable,
			RetryCount:  old.RetryCount + 1,
			FirstSeen:   old.FirstSeen, // Preserve first seen time
			LastSeen:    now,           // Update last seen time
			Message:     message,
		}
	}

	// Error type changed, reset count
	return &ErrorMetadata{
		IsRetryable: isRetryable,
		RetryCount:  1,
		FirstSeen:   now,
		LastSeen:    now,
		Message:     message,
	}
}

// Format formats error metadata to string
func (m *ErrorMetadata) Format() string {
	if m == nil {
		return ""
	}
	if m.IsRetryable {
		return fmt.Sprintf("R:%d:%d:%d:%s",
			m.RetryCount,
			m.FirstSeen.Unix(),
			m.LastSeen.Unix(),
			m.Message,
		)
	}
	return fmt.Sprintf("N:%d:%s",
		m.FirstSeen.Unix(),
		m.Message,
	)
}

// IsRetryableError determines if an error is retryable
// Returns true if the error is a transient error that may succeed on retry,
// such as network errors, timeouts, or temporary system unavailability
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Check for unexpected EOF
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network timeout errors
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Check for temporary network errors
	if netErr, ok := err.(net.Error); ok {
		type temporary interface {
			Temporary() bool
		}
		if tmp, ok := netErr.(temporary); ok && tmp.Temporary() {
			return true
		}
	}

	// Check error message for common retryable patterns
	errMsgLower := strings.ToLower(errMsg)
	retryablePatterns := []string{
		"connection reset",
		"connection timed out",
		"connection timeout",
		"dial tcp",
		"i/o timeout",
		"broken pipe",
		"tls handshake timeout",
		"use of closed network connection",
		"temporary failure",
		"service unavailable",
		"timeout",
		"network error",
		"rpc error",
		"backend",
		"unavailable",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsgLower, pattern) {
			return true
		}
	}

	return false
}
