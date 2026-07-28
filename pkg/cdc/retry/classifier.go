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

package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// DefaultClassifier recognises common transient network errors.
type DefaultClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (DefaultClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Unwrap recursively.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		// Some drivers implement Temporary to signal transient failures; prefer Timeout up top.
		type temporary interface {
			Temporary() bool
		}
		if tmp, ok := netErr.(temporary); ok && tmp.Temporary() {
			return true
		}
	}

	return false
}
