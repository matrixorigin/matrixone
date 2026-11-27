// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package morpc

import (
	"context"
	"time"
)

type backendWaitDeadlineKey struct{}

// WithBackendWaitDeadline bounds waiting for an unavailable backend without
// shortening the lifetime of a request already sent to an available backend.
func WithBackendWaitDeadline(ctx context.Context, deadline time.Time) context.Context {
	if deadline.IsZero() {
		return ctx
	}
	if existing, ok := BackendWaitDeadline(ctx); ok && !deadline.Before(existing) {
		return ctx
	}
	return context.WithValue(ctx, backendWaitDeadlineKey{}, deadline)
}

// BackendWaitDeadline returns the optional backend-availability deadline.
func BackendWaitDeadline(ctx context.Context) (time.Time, bool) {
	deadline, ok := ctx.Value(backendWaitDeadlineKey{}).(time.Time)
	return deadline, ok && !deadline.IsZero()
}

func backendWaitContext(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	if deadline, ok := BackendWaitDeadline(ctx); ok {
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		return waitCtx, cancel, true
	}
	return ctx, func() {}, false
}
