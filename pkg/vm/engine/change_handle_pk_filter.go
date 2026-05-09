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
)

// PKFilter carries a set of ZoneMap segments that CollectChanges
// implementations use to prune objects and blocks whose sort-key
// range does not overlap with the requested PK values.
//
// Only the DATA BRANCH PICK path attaches this to the context;
// CDC, ISCP and other callers leave it nil so their behavior is
// completely unchanged.
type PKFilter struct {
	// Segments is a sorted, non-overlapping list of ZoneMap ranges.
	// Each element is a 64-byte ZoneMap (index.ZM encoding) stored as
	// raw []byte to avoid an import cycle between the engine and index
	// packages.
	//
	// Constructed by the streaming PK materializer during PICK key
	// ingestion.  The gap-based segmentation algorithm ensures high
	// density (few false-positive object/block matches) while keeping
	// the list compact (one ZM per cluster of nearby PKs).
	//
	// Used by logtailreplay/change_handle.go via index.AnySegmentOverlaps()
	// for object-level and block-level pruning.
	Segments [][]byte

	// PrimarySeqnum is the sequence number of the primary key column
	// in the table schema, used for block-level ZoneMap access.
	PrimarySeqnum int
}

type pkFilterContextKey struct{}

// WithPKFilter attaches a PK filter to the context so that
// CollectChanges can apply object/block pruning via ZoneMap segments.
// Passing nil or a filter with no segments is a no-op and returns
// the original context.
func WithPKFilter(ctx context.Context, filter *PKFilter) context.Context {
	if ctx == nil || filter == nil || len(filter.Segments) == 0 {
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
