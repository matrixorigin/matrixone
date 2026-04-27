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

type retainRowIDKey struct{}

// WithRetainRowID requests that CollectChanges keep rowid in its internal batch
// shape for callers that need row-level provenance. Callers that don't opt in
// keep the existing output contract.
func WithRetainRowID(ctx context.Context, retain bool) context.Context {
	if ctx == nil || !retain {
		return ctx
	}
	return context.WithValue(ctx, retainRowIDKey{}, true)
}

// RetainRowIDFromContext reports whether the caller requested rowid retention.
// The default remains false so existing callers are unchanged.
func RetainRowIDFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	retain, _ := ctx.Value(retainRowIDKey{}).(bool)
	return retain
}
