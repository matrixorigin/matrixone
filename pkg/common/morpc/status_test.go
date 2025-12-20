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
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestStatusCategoryString(t *testing.T) {
	tests := []struct {
		cat    StatusCategory
		expect string
	}{
		{StatusOK, "ok"},
		{StatusTransient, "transient"},
		{StatusUnavailable, "unavailable"},
		{StatusCancelled, "cancelled"},
		{StatusUnknown, "unknown"},
		{StatusCategory(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			assert.Equal(t, tt.expect, tt.cat.String())
		})
	}
}

func TestGetStatusCategory(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected StatusCategory
	}{
		// Nil error
		{"nil error", nil, StatusOK},

		// Transient errors (moerr)
		{"ErrRPCTimeout", moerr.NewRPCTimeoutNoCtx(), StatusTransient},
		{"ErrServiceUnavailable", moerr.NewServiceUnavailableNoCtx("test"), StatusTransient},
		{"ErrConnectionReset", moerr.NewConnectionReset(context.Background()), StatusTransient},

		// Unavailable errors (moerr)
		{"ErrBackendClosed", moerr.NewBackendClosedNoCtx(), StatusUnavailable},
		{"ErrNoAvailableBackend", moerr.NewNoAvailableBackendNoCtx(), StatusUnavailable},
		{"ErrBackendCannotConnect", moerr.NewBackendCannotConnectNoCtx(errors.New("conn failed")), StatusUnavailable},

		// Cancelled errors (moerr)
		{"ErrClientClosed", moerr.NewClientClosedNoCtx(), StatusCancelled},
		{"ErrStreamClosed", moerr.NewStreamClosedNoCtx(), StatusCancelled},

		// Context errors
		{"context.Canceled", context.Canceled, StatusCancelled},
		{"context.DeadlineExceeded", context.DeadlineExceeded, StatusTransient},

		// IO errors
		{"io.EOF", io.EOF, StatusTransient},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, StatusTransient},
		{"os.ErrDeadlineExceeded", os.ErrDeadlineExceeded, StatusTransient},

		// String pattern matching
		{"connection reset by peer", errors.New("connection reset by peer"), StatusTransient},
		{"broken pipe", errors.New("write: broken pipe"), StatusTransient},

		// Unknown errors
		{"generic error", errors.New("some random error"), StatusUnknown},
		{"wrapped unknown", fmt.Errorf("wrapped: %w", errors.New("inner")), StatusUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetStatusCategory(tt.err)
			assert.Equal(t, tt.expected, got, "error: %v", tt.err)
		})
	}
}

// mockNetError implements net.Error for testing
type mockNetError struct {
	timeout   bool
	temporary bool
}

func (e *mockNetError) Error() string   { return "mock net error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return e.temporary }

var _ net.Error = (*mockNetError)(nil)

func TestGetStatusCategoryNetError(t *testing.T) {
	tests := []struct {
		name     string
		timeout  bool
		expected StatusCategory
	}{
		{"net timeout error", true, StatusTransient},
		{"net non-timeout error", false, StatusUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &mockNetError{timeout: tt.timeout}
			got := GetStatusCategory(err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsTransient(t *testing.T) {
	assert.True(t, IsTransient(moerr.NewRPCTimeoutNoCtx()))
	assert.True(t, IsTransient(context.DeadlineExceeded))
	assert.True(t, IsTransient(io.EOF))

	assert.False(t, IsTransient(nil))
	assert.False(t, IsTransient(moerr.NewBackendClosedNoCtx()))
	assert.False(t, IsTransient(moerr.NewClientClosedNoCtx()))
}

func TestIsUnavailable(t *testing.T) {
	assert.True(t, IsUnavailable(moerr.NewBackendClosedNoCtx()))
	assert.True(t, IsUnavailable(moerr.NewNoAvailableBackendNoCtx()))
	assert.True(t, IsUnavailable(moerr.NewBackendCannotConnectNoCtx(errors.New("test"))))
	assert.True(t, IsUnavailable(&mockNetError{timeout: false}))

	assert.False(t, IsUnavailable(nil))
	assert.False(t, IsUnavailable(moerr.NewRPCTimeoutNoCtx()))
	assert.False(t, IsUnavailable(moerr.NewClientClosedNoCtx()))
}

func TestIsCancelled(t *testing.T) {
	assert.True(t, IsCancelled(moerr.NewClientClosedNoCtx()))
	assert.True(t, IsCancelled(moerr.NewStreamClosedNoCtx()))
	assert.True(t, IsCancelled(context.Canceled))

	assert.False(t, IsCancelled(nil))
	assert.False(t, IsCancelled(moerr.NewRPCTimeoutNoCtx()))
	assert.False(t, IsCancelled(moerr.NewBackendClosedNoCtx()))
}

func TestIsConnectionError(t *testing.T) {
	// Should be true for both transient and unavailable
	assert.True(t, IsConnectionError(moerr.NewRPCTimeoutNoCtx()))
	assert.True(t, IsConnectionError(moerr.NewBackendClosedNoCtx()))
	assert.True(t, IsConnectionError(io.EOF))

	// Should be false for cancelled and unknown
	assert.False(t, IsConnectionError(nil))
	assert.False(t, IsConnectionError(moerr.NewClientClosedNoCtx()))
	assert.False(t, IsConnectionError(errors.New("random error")))
}

func TestIsRetryableForCDC(t *testing.T) {
	// CDC retries on transient, unavailable, AND cancelled
	assert.True(t, IsRetryableForCDC(moerr.NewRPCTimeoutNoCtx()))
	assert.True(t, IsRetryableForCDC(moerr.NewBackendClosedNoCtx()))
	assert.True(t, IsRetryableForCDC(moerr.NewClientClosedNoCtx()))
	assert.True(t, IsRetryableForCDC(context.Canceled))

	// Not retryable: nil and unknown
	assert.False(t, IsRetryableForCDC(nil))
	assert.False(t, IsRetryableForCDC(errors.New("unknown error")))
}

func TestIsDefinitiveFailure(t *testing.T) {
	// Definitive: unavailable and cancelled
	assert.True(t, IsDefinitiveFailure(moerr.NewBackendClosedNoCtx()))
	assert.True(t, IsDefinitiveFailure(moerr.NewClientClosedNoCtx()))
	assert.True(t, IsDefinitiveFailure(moerr.NewNoAvailableBackendNoCtx()))

	// Not definitive: transient, ok, unknown
	assert.False(t, IsDefinitiveFailure(nil))
	assert.False(t, IsDefinitiveFailure(moerr.NewRPCTimeoutNoCtx()))
	assert.False(t, IsDefinitiveFailure(io.EOF))
	assert.False(t, IsDefinitiveFailure(errors.New("unknown")))
}

// TestLockserviceCompatibility verifies that the new API is compatible with
// lockservice's isRetryError semantics.
// lockservice: isRetryError returns FALSE for ErrBackendClosed/ErrBackendCannotConnect
// Our equivalent: IsDefinitiveFailure returns TRUE for these errors
func TestLockserviceCompatibility(t *testing.T) {
	// These should be "definitive" (lockservice: !isRetryError)
	definitiveErrors := []error{
		moerr.NewBackendClosedNoCtx(),
		moerr.NewBackendCannotConnectNoCtx(errors.New("test")),
	}

	for _, err := range definitiveErrors {
		assert.True(t, IsDefinitiveFailure(err),
			"lockservice expects this to be definitive: %v", err)
		assert.True(t, IsUnavailable(err),
			"should be unavailable: %v", err)
	}

	// These should be "transient" (lockservice: isRetryError returns true)
	transientErrors := []error{
		moerr.NewRPCTimeoutNoCtx(),
		context.DeadlineExceeded,
		io.EOF,
	}

	for _, err := range transientErrors {
		assert.False(t, IsDefinitiveFailure(err),
			"lockservice expects this to be transient: %v", err)
		assert.True(t, IsTransient(err),
			"should be transient: %v", err)
	}
}

// TestCDCCompatibility verifies that the new API is compatible with
// CDC's determineRetryable semantics.
func TestCDCCompatibility(t *testing.T) {
	// These are all retryable in CDC
	retryableErrors := []error{
		moerr.NewRPCTimeoutNoCtx(),
		moerr.NewNoAvailableBackendNoCtx(),
		moerr.NewBackendCannotConnectNoCtx(errors.New("test")),
		moerr.NewClientClosedNoCtx(),
		moerr.NewBackendClosedNoCtx(),
		moerr.NewServiceUnavailableNoCtx("test"),
		context.DeadlineExceeded,
	}

	for _, err := range retryableErrors {
		assert.True(t, IsRetryableForCDC(err),
			"CDC expects this to be retryable: %v", err)
	}
}

func TestWrappedErrors(t *testing.T) {
	// Test that wrapped errors are properly classified
	wrapped := fmt.Errorf("operation failed: %w", context.DeadlineExceeded)
	assert.Equal(t, StatusTransient, GetStatusCategory(wrapped))
	assert.True(t, IsTransient(wrapped))

	wrapped2 := fmt.Errorf("connection issue: %w", io.EOF)
	assert.Equal(t, StatusTransient, GetStatusCategory(wrapped2))
	assert.True(t, IsTransient(wrapped2))

	wrapped3 := fmt.Errorf("cancelled: %w", context.Canceled)
	assert.Equal(t, StatusCancelled, GetStatusCategory(wrapped3))
	assert.True(t, IsCancelled(wrapped3))
}

// Benchmark to ensure classification is fast
func BenchmarkGetStatusCategory(b *testing.B) {
	err := moerr.NewRPCTimeoutNoCtx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStatusCategory(err)
	}
}

// TestRealWorldTimeout tests with a real timeout scenario
func TestRealWorldTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // Ensure timeout

	err := ctx.Err()
	assert.Equal(t, StatusTransient, GetStatusCategory(err))
	assert.True(t, IsTransient(err))
}

// TestRealWorldCancel tests with a real cancel scenario
func TestRealWorldCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := ctx.Err()
	assert.Equal(t, StatusCancelled, GetStatusCategory(err))
	assert.True(t, IsCancelled(err))
}
