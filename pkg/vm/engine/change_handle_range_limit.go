// Copyright 2026 Matrix Origin
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

package engine

import "context"

// ChangeRangeLimit is an opt-in bound for callers whose public contract can
// reject a range that would retain too much in-memory replay state. A zero
// value means no limit, preserving existing CollectChanges callers.
type ChangeRangeLimit struct {
	MaxInMemoryRows  int
	MaxInMemoryBytes int
}

// Enabled reports whether the caller opted into either bound.
func (l ChangeRangeLimit) Enabled() bool {
	return l.MaxInMemoryRows > 0 || l.MaxInMemoryBytes > 0
}

type changeRangeLimitContextKey struct{}

// WithChangeRangeLimit attaches an explicit caller-owned recovery bound.
func WithChangeRangeLimit(ctx context.Context, limit ChangeRangeLimit) context.Context {
	if ctx == nil || !limit.Enabled() {
		return ctx
	}
	return context.WithValue(ctx, changeRangeLimitContextKey{}, limit)
}

// ChangeRangeLimitFromContext returns the caller-owned recovery bound, or its
// zero value when the caller did not opt in.
func ChangeRangeLimitFromContext(ctx context.Context) ChangeRangeLimit {
	if ctx == nil {
		return ChangeRangeLimit{}
	}
	limit, _ := ctx.Value(changeRangeLimitContextKey{}).(ChangeRangeLimit)
	return limit
}
