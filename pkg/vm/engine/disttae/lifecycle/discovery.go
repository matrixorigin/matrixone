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

package lifecycle

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type objectPageSource interface {
	ScanVisibleDataObjectsPage(
		context.Context,
		types.TS,
		*objectio.ObjectNameShort,
		int,
		uint64,
	) (logtailreplay.VisibleDataObjectPage, error)
}

// DiscoverObjectPage performs one bounded scan over the existing
// PartitionState Object-name index. Crash recovery intentionally rediscoveries
// candidates; only persisted roots/datasets own side effects.
func DiscoverObjectPage(
	ctx context.Context,
	source objectPageSource,
	request DiscoveryRequest,
) (DiscoveryPage, error) {
	if source == nil {
		return DiscoveryPage{}, moerr.NewInvalidInput(ctx, "Lifecycle discovery source is nil")
	}
	if err := request.Limits.validate(ctx); err != nil {
		return DiscoveryPage{}, err
	}
	if request.Now.IsZero() {
		return DiscoveryPage{}, moerr.NewInvalidInput(ctx, "Lifecycle discovery Now is required")
	}

	scanCtx, cancel := context.WithTimeout(ctx, request.Limits.MaxDuration)
	defer cancel()

	cursor := request.Cursor
	reset := cursor.Wrapped
	// Never reset a cycle which has already made forward progress. A large
	// table can legitimately need longer than FullScanInterval to reach its
	// tail; restarting it here would repeatedly revisit the prefix and starve
	// every Object after the persisted cursor. The interval starts a new cycle
	// only when there is no in-progress position to preserve. Capacity
	// certification, rather than cursor regression, enforces the full-scan SLO.
	if !cursor.HasLastObject &&
		request.FullScanInterval > 0 &&
		!request.LastFullScanAt.IsZero() &&
		!request.Now.Before(request.LastFullScanAt.Add(request.FullScanInterval)) {
		reset = true
	}
	startCycle := request.LastFullScanAt.IsZero() || reset
	if reset {
		cursor = DiscoveryCursor{}
	}

	var after *objectio.ObjectNameShort
	if cursor.HasLastObject {
		value := cursor.LastObjectName
		after = &value
	}
	rawPage, err := source.ScanVisibleDataObjectsPage(
		scanCtx,
		request.Snapshot,
		after,
		request.Limits.MaxObjects,
		request.Limits.MaxMetaBytes,
	)
	if err != nil {
		return DiscoveryPage{}, err
	}
	if len(rawPage.Objects) > request.Limits.MaxObjects ||
		rawPage.MetaBytes > request.Limits.MaxMetaBytes {
		return DiscoveryPage{}, moerr.NewInternalError(
			ctx,
			"Lifecycle Object page source exceeded its declared limits",
		)
	}

	page := DiscoveryPage{
		Candidates: make([]Candidate, 0, len(rawPage.Objects)),
		Next: DiscoveryCursor{
			Snapshot: request.Snapshot,
		},
		EndOfCycle: rawPage.End,
		MetaBytes:  rawPage.MetaBytes,
	}
	if startCycle {
		// Persist the cycle anchor after the first successful page. Without this
		// one-shot update, an overdue scan whose first page is not End would
		// reset to the same first page on every scheduler tick.
		page.StartedFullScanAt = request.Now
	}
	for _, object := range rawPage.Objects {
		if object.GetAppendable() || !object.Visible(request.Snapshot) {
			return DiscoveryPage{}, moerr.NewInternalError(
				ctx,
				"Lifecycle Object page source returned an ineligible Object",
			)
		}
		page.Candidates = append(page.Candidates, Candidate{
			Snapshot: request.Snapshot,
			Source:   object,
		})
	}
	if rawPage.End {
		page.Next.Wrapped = true
		page.CompletedFullScanAt = request.Now
	} else if rawPage.LastObjectName != nil {
		page.Next.HasLastObject = true
		page.Next.LastObjectName = *rawPage.LastObjectName
	}
	return page, nil
}
