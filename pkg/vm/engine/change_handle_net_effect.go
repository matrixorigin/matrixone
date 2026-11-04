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

type collectChangesKeepTrailingDeletesKey struct{}

// WithCollectChangesKeepTrailingDeletes requests that CollectChanges keep the
// trailing DELETE of a primary key whose operations inside the requested range
// start with an INSERT and end with a DELETE, instead of coalescing the pair
// away as "no net change".
//
// Dropping the pair is only sound for a consumer whose materialized state
// equals the table state at the range start, because the INSERT then proves the
// key was absent before the range. A consumer seeded from an EARLIER generation
// breaks that precondition: CREATE TABLE ... CLONE / DATA BRANCH / RESTORE copy
// an index's built state, and the clone's own row appends fall inside the first
// change range the copy is maintained from, so a key deleted in that same range
// is both re-inserted and deleted there while the copied state still holds it.
// Losing the tombstone leaves the copied entry queryable forever, while an extra
// DELETE of a key the consumer does not hold is a no-op.
func WithCollectChangesKeepTrailingDeletes(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, collectChangesKeepTrailingDeletesKey{}, true)
}

// CollectChangesKeepTrailingDeletesFromContext reports whether CollectChanges
// must keep the trailing DELETE of an insert-then-delete primary key.
func CollectChangesKeepTrailingDeletesFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	keep, _ := ctx.Value(collectChangesKeepTrailingDeletesKey{}).(bool)
	return keep
}
