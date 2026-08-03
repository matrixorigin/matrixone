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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// LifecycleTable is the complete, deliberately narrow capability that the
// background Lifecycle worker may request from an engine Relation. It reuses
// the live PartitionState and Merge producer without exposing txnTable.
type LifecycleTable interface {
	LifecycleObjectReader
	LifecycleObjectRewriter

	LifecycleDiscoverObjectPage(
		context.Context,
		lifecyclepkg.DiscoveryRequest,
	) (lifecyclepkg.DiscoveryPage, error)
	LifecycleSelectProtectionSet(
		context.Context,
		types.TS,
		[]objectio.ObjectEntry,
		logtailreplay.LifecycleTombstoneSelectionLimits,
	) (lifecyclepkg.ProtectionSet, error)
	LifecycleSortKeyOrdinal() int
}

var (
	_ LifecycleTable = (*txnTable)(nil)
	_ LifecycleTable = (*txnTableDelegate)(nil)
)

// txnDatabase returns a txnTableDelegate to engine callers. Keep the
// Lifecycle capability on that existing relation handle and delegate only to
// the canonical txnTable; this does not add another reader or Merge path.
func (tbl *txnTableDelegate) LifecycleReadObject(
	ctx context.Context,
	snapshot types.TS,
	source objectio.ObjectStats,
	maxCertifiedBlockReadBytes uint64,
	consume lifecyclepkg.ExactBlockConsumer,
) (lifecyclepkg.ObjectScanReport, error) {
	return tbl.origin.LifecycleReadObject(
		ctx,
		snapshot,
		source,
		maxCertifiedBlockReadBytes,
		consume,
	)
}

func (tbl *txnTableDelegate) LifecycleRewriteObject(
	ctx context.Context,
	options LifecycleRewriteOptions,
) (LifecycleRewriteResult, error) {
	return tbl.origin.LifecycleRewriteObject(ctx, options)
}

func (tbl *txnTableDelegate) LifecycleSortKeyOrdinal() int {
	return tbl.origin.LifecycleSortKeyOrdinal()
}

func (tbl *txnTableDelegate) LifecycleDiscoverObjectPage(
	ctx context.Context,
	request lifecyclepkg.DiscoveryRequest,
) (lifecyclepkg.DiscoveryPage, error) {
	return tbl.origin.LifecycleDiscoverObjectPage(ctx, request)
}

func (tbl *txnTableDelegate) LifecycleSelectProtectionSet(
	ctx context.Context,
	snapshot types.TS,
	dataSources []objectio.ObjectEntry,
	limits logtailreplay.LifecycleTombstoneSelectionLimits,
) (lifecyclepkg.ProtectionSet, error) {
	return tbl.origin.LifecycleSelectProtectionSet(
		ctx,
		snapshot,
		dataSources,
		limits,
	)
}

func (tbl *txnTable) LifecycleSortKeyOrdinal() int {
	ordinal, _ := tbl.getSortKeyPosAndSortKeyIsPK()
	return ordinal
}

func (tbl *txnTable) LifecycleDiscoverObjectPage(
	ctx context.Context,
	request lifecyclepkg.DiscoveryRequest,
) (lifecyclepkg.DiscoveryPage, error) {
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return lifecyclepkg.DiscoveryPage{}, err
	}
	return lifecyclepkg.DiscoverObjectPage(ctx, state, request)
}

func (tbl *txnTable) LifecycleSelectProtectionSet(
	ctx context.Context,
	snapshot types.TS,
	dataSources []objectio.ObjectEntry,
	limits logtailreplay.LifecycleTombstoneSelectionLimits,
) (lifecyclepkg.ProtectionSet, error) {
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return lifecyclepkg.ProtectionSet{}, err
	}
	return lifecyclepkg.SelectProtectionSet(
		ctx,
		state,
		snapshot,
		dataSources,
		limits,
	)
}
