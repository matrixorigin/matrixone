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

package moerr

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsConnectionRelatedRPCError tests IsConnectionRelatedRPCError with various error types
func TestIsConnectionRelatedRPCError(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "ErrBackendClosed",
			err:      NewBackendClosedNoCtx(),
			expected: true,
		},
		{
			name:     "ErrNoAvailableBackend",
			err:      NewNoAvailableBackendNoCtx(),
			expected: true,
		},
		{
			name:     "ErrBackendCannotConnect",
			err:      NewBackendCannotConnectNoCtx(),
			expected: true,
		},
		{
			name:     "ErrServiceUnavailable",
			err:      NewServiceUnavailable(ctx, "test service unavailable"),
			expected: true,
		},
		{
			name:     "ErrConnectionReset",
			err:      NewConnectionReset(ctx),
			expected: true,
		},
		{
			name:     "non-connection error - ErrClientClosed",
			err:      NewClientClosedNoCtx(),
			expected: false,
		},
		{
			name:     "non-connection error - standard error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "non-connection error - ErrInternal",
			err:      NewInternalErrorNoCtx("internal error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsConnectionRelatedRPCError(tt.err)
			assert.Equal(t, tt.expected, result, "IsConnectionRelatedRPCError(%v) = %v, expected %v", tt.err, result, tt.expected)
		})
	}
}

// TestNewServiceUnavailable tests NewServiceUnavailable function
func TestNewServiceUnavailable(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		reason string
	}{
		{
			name:   "with background context",
			ctx:    context.Background(),
			reason: "database connection lost",
		},
		{
			name:   "with canceled context",
			ctx:    func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }(),
			reason: "operation timeout",
		},
		{
			name:   "empty reason",
			ctx:    context.Background(),
			reason: "",
		},
		{
			name:   "long reason",
			ctx:    context.Background(),
			reason: "service is temporarily unavailable due to maintenance and will be restored soon",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewServiceUnavailable(tt.ctx, tt.reason)
			assert.NotNil(t, err)
			assert.True(t, IsMoErrCode(err, ErrServiceUnavailable))
			errMsg := err.Error()
			assert.Contains(t, errMsg, "service unavailable")
			if tt.reason != "" {
				assert.Contains(t, errMsg, tt.reason)
			}
		})
	}
}

// TestIsRPCClientClosed tests IsRPCClientClosed with various error types
func TestIsRPCClientClosed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "ErrClientClosed",
			err:      NewClientClosedNoCtx(),
			expected: true,
		},
		{
			name:     "non-client-closed error - ErrBackendClosed",
			err:      NewBackendClosedNoCtx(),
			expected: false,
		},
		{
			name:     "non-client-closed error - standard error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "non-client-closed error - ErrNoAvailableBackend",
			err:      NewNoAvailableBackendNoCtx(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRPCClientClosed(tt.err)
			assert.Equal(t, tt.expected, result, "IsRPCClientClosed(%v) = %v, expected %v", tt.err, result, tt.expected)
		})
	}
}

// TestConnectionRelatedErrorsCoexistence tests that multiple connection errors are properly identified
func TestConnectionRelatedErrorsCoexistence(t *testing.T) {
	ctx := context.Background()

	connectionErrors := []error{
		NewBackendClosedNoCtx(),
		NewNoAvailableBackendNoCtx(),
		NewBackendCannotConnectNoCtx(),
		NewServiceUnavailable(ctx, "test"),
		NewConnectionReset(ctx),
	}

	for i, err := range connectionErrors {
		t.Run("connection_error_"+string(rune('A'+i)), func(t *testing.T) {
			assert.True(t, IsConnectionRelatedRPCError(err))
			assert.False(t, IsRPCClientClosed(err))
		})
	}

	// Test that ErrClientClosed is distinct
	clientClosedErr := NewClientClosedNoCtx()
	assert.False(t, IsConnectionRelatedRPCError(clientClosedErr))
	assert.True(t, IsRPCClientClosed(clientClosedErr))
}

// TestErrorTypeDistinction tests that different error types are properly distinguished
func TestErrorTypeDistinction(t *testing.T) {
	ctx := context.Background()

	// Connection errors should not be identified as client closed
	serviceErr := NewServiceUnavailable(ctx, "test")
	assert.True(t, IsConnectionRelatedRPCError(serviceErr))
	assert.False(t, IsRPCClientClosed(serviceErr))

	backendErr := NewBackendClosedNoCtx()
	assert.True(t, IsConnectionRelatedRPCError(backendErr))
	assert.False(t, IsRPCClientClosed(backendErr))

	// Client closed should not be identified as connection error
	clientErr := NewClientClosedNoCtx()
	assert.False(t, IsConnectionRelatedRPCError(clientErr))
	assert.True(t, IsRPCClientClosed(clientErr))
}
