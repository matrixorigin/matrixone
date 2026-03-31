// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// PKFilter carries a sorted vector of primary key values that
// CollectChanges implementations can use to prune objects, blocks,
// and individual rows.  Only the DATA BRANCH PICK path attaches
// this to the context; CDC, ISCP and other callers leave it nil
// so their behavior is completely unchanged.
type PKFilter struct {
	// Vec is a sorted vector of PK values (typed to match the table's
	// sort-key column).  Engine code can pass this to ZoneMap.AnyIn()
	// for object-level and block-level pruning.
	Vec *vector.Vector

	// PrimarySeqnum is the sequence number of the primary key column
	// in the table schema, used for block-level zonemap access.
	PrimarySeqnum int
}

type pkFilterContextKey struct{}

// WithPKFilter attaches a PK filter to the context so that
// CollectChanges can apply object/block/row pruning.
// Passing nil is a no-op and returns the original context.
func WithPKFilter(ctx context.Context, filter *PKFilter) context.Context {
	if ctx == nil || filter == nil || filter.Vec == nil {
		return ctx
	}
	return context.WithValue(ctx, pkFilterContextKey{}, filter)
}

// PKFilterFromContext extracts the PK filter attached by WithPKFilter.
// Returns nil when no filter was set (the common case for CDC/ISCP).
func PKFilterFromContext(ctx context.Context) *PKFilter {
	if ctx == nil {
		return nil
	}
	f, _ := ctx.Value(pkFilterContextKey{}).(*PKFilter)
	return f
}
