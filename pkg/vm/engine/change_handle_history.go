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

type collectChangesPreserveAllVersionsKey struct{}

// WithCollectChangesPreserveAllVersions requests that CollectChanges retain
// every row version while replaying a non-empty change range instead of
// coalescing operations on the same primary key to their net effect. This is
// intended for metadata consumers that need an historical boundary, such as
// the first commit of a catalog row.
func WithCollectChangesPreserveAllVersions(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, collectChangesPreserveAllVersionsKey{}, true)
}

// CollectChangesPreserveAllVersionsFromContext reports whether CollectChanges
// must retain every row version in the requested range.
func CollectChangesPreserveAllVersionsFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	preserve, _ := ctx.Value(collectChangesPreserveAllVersionsKey{}).(bool)
	return preserve
}
