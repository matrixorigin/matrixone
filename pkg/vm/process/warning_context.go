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

package process

import "context"

type warningSinkContextKey struct{}
type warningSinkContextValue struct{ sink any }

type warningRetentionLimitContextKey struct{}

// ContextWithWarningSink captures an execution generation for internal SQL
// that creates a new top-level Process. Pass WarningSink, not Session fallback.
// An explicit nil masks a binding inherited from an older execution.
func ContextWithWarningSink(ctx context.Context, sink any) context.Context {
	if ctx == nil {
		if sink == nil {
			return nil
		}
		ctx = context.Background()
	}
	if sink == nil && ctx.Value(warningSinkContextKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, warningSinkContextKey{}, warningSinkContextValue{sink: sink})
}

// WarningSinkFromContext returns the captured destination, never a live Process.
func WarningSinkFromContext(ctx context.Context) any {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(warningSinkContextKey{}).(warningSinkContextValue)
	return v.sink
}

// ContextWithWarningRetentionLimit captures the immutable diagnostic capacity
// for one statement generation. An explicit zero is retained by the context
// and is distinguished from a missing value by WarningRetentionLimitFromContext.
func ContextWithWarningRetentionLimit(ctx context.Context, limit int) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, warningRetentionLimitContextKey{}, clampWarningRetentionLimit(limit))
}

// WarningRetentionLimitFromContext returns the capacity captured for the
// current statement generation. The boolean distinguishes an explicit zero
// from a context that has no statement snapshot.
func WarningRetentionLimitFromContext(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	limit, ok := ctx.Value(warningRetentionLimitContextKey{}).(int)
	if !ok {
		return 0, false
	}
	return clampWarningRetentionLimit(limit), true
}
