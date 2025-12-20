// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// StatusCategory classifies RPC errors into semantic categories.
// This allows callers to make informed decisions about error handling
// without checking individual error codes.
type StatusCategory int

const (
	// StatusOK indicates no error (success).
	StatusOK StatusCategory = iota

	// StatusTransient indicates temporary failures that may succeed on immediate retry.
	// Examples: timeout waiting for response, temporary network hiccup.
	// These errors suggest the system is working but the specific request failed.
	StatusTransient

	// StatusUnavailable indicates the service/backend is currently unavailable.
	// Examples: backend closed, cannot connect, no available backend.
	// These errors suggest a connection-level problem that may recover over time.
	// Note: By the time caller receives this, morpc has already exhausted internal retries.
	StatusUnavailable

	// StatusCancelled indicates the operation was cancelled by the client side.
	// Examples: client closed, context cancelled.
	// Whether to retry depends on the caller's shutdown logic.
	StatusCancelled

	// StatusUnknown indicates the error category cannot be determined.
	// Callers should handle these conservatively based on their specific requirements.
	StatusUnknown
)

// String returns the string representation of the status category.
func (s StatusCategory) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusTransient:
		return "transient"
	case StatusUnavailable:
		return "unavailable"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// GetStatusCategory classifies an error into a semantic category.
// This provides a unified way to understand RPC errors without checking
// individual error codes.
//
// Usage patterns:
//   - lockservice: Use IsUnavailable() to detect definitive failures
//   - CDC: Use IsTransient() || IsUnavailable() to detect retryable errors
func GetStatusCategory(err error) StatusCategory {
	if err == nil {
		return StatusOK
	}

	// Check moerr codes first (most specific)
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) {
		return StatusTransient
	}
	if moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable) ||
		moerr.IsMoErrCode(err, moerr.ErrConnectionReset) {
		return StatusTransient
	}

	// Connection-level unavailability
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return StatusUnavailable
	}

	// Client-side cancellation
	if moerr.IsMoErrCode(err, moerr.ErrClientClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrStreamClosed) {
		return StatusCancelled
	}

	// Check standard context errors
	if errors.Is(err, context.Canceled) {
		return StatusCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return StatusTransient
	}

	// Check standard io errors (connection issues)
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, os.ErrDeadlineExceeded) {
		return StatusTransient
	}

	// Check net.Error for timeout
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return StatusTransient
		}
		// Other net errors are unavailable
		return StatusUnavailable
	}

	// Check for connection reset by peer (common pattern)
	if strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "broken pipe") {
		return StatusTransient
	}

	return StatusUnknown
}

// IsTransient returns true if the error is a transient failure.
// Transient errors are temporary and may succeed on immediate retry.
// Examples: timeout, temporary network issues.
func IsTransient(err error) bool {
	return GetStatusCategory(err) == StatusTransient
}

// IsUnavailable returns true if the error indicates service unavailability.
// This means the backend/connection is currently down.
// By the time caller receives this, morpc has already exhausted internal retries.
//
// Usage:
//   - lockservice: Use this to detect when lock table bindings should be refreshed
//   - txn/rpc: Use this to detect when to switch to a different TN
func IsUnavailable(err error) bool {
	return GetStatusCategory(err) == StatusUnavailable
}

// IsCancelled returns true if the error indicates client-side cancellation.
// Examples: client closed, context cancelled.
//
// Usage:
//   - During shutdown, this is expected and should not trigger retries
//   - CDC may choose to retry by creating a new client
func IsCancelled(err error) bool {
	return GetStatusCategory(err) == StatusCancelled
}

// IsConnectionError returns true if the error is related to connection issues.
// This includes both transient network problems and unavailable backends.
// Convenience function combining transient and unavailable categories.
func IsConnectionError(err error) bool {
	cat := GetStatusCategory(err)
	return cat == StatusTransient || cat == StatusUnavailable
}

// IsRetryableForCDC returns true if the error should trigger retry in CDC-style callers.
// This is a convenience function for callers that want to retry on any
// connection-related error, including client cancellation (for reconnection).
//
// Note: This differs from lockservice semantics where ErrClientClosed means
// the local client is shutting down and should not retry.
func IsRetryableForCDC(err error) bool {
	cat := GetStatusCategory(err)
	return cat == StatusTransient || cat == StatusUnavailable || cat == StatusCancelled
}

// IsDefinitiveFailure returns true if the error indicates a definitive failure
// where the remote service is considered unavailable.
// This is the opposite of "transient" - the failure is not temporary.
//
// This is a BROAD definition that includes:
//   - StatusUnavailable: ErrBackendClosed, ErrBackendCannotConnect, ErrNoAvailableBackend
//   - StatusCancelled: ErrClientClosed, context.Canceled
//
// Usage (CDC/general style):
//   - If true: The remote is definitely gone, take action
//   - If false: Be conservative, assume things might still be OK
//
// For lockservice-specific semantics (more conservative), use IsRemoteUnavailable().
func IsDefinitiveFailure(err error) bool {
	cat := GetStatusCategory(err)
	return cat == StatusUnavailable || cat == StatusCancelled
}

// IsRemoteUnavailable returns true ONLY if the specific remote backend is
// definitely unreachable. This matches lockservice's original isRetryError semantics.
//
// Only these errors qualify:
//   - ErrBackendClosed: The specific backend connection is closed
//   - ErrBackendCannotConnect: Cannot connect to the specific backend
//
// This is MORE CONSERVATIVE than IsDefinitiveFailure() because:
//   - ErrNoAvailableBackend is NOT included (could be temporary routing issue)
//   - ErrClientClosed is NOT included (local shutdown, not remote failure)
//
// Usage (lockservice style):
//   - If true: The SPECIFIC remote backend is gone, refresh bindings immediately
//   - If false: Be conservative, the remote might still be there
func IsRemoteUnavailable(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect)
}
