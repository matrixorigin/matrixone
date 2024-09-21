// Copyright 2022 Matrix Origin
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
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func consumeEntry(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	cache *cache.CatalogCache,
	state *logtailreplay.PartitionState,
	e *api.Entry,
	isSub bool,
) error {
	start := time.Now()
	defer func() {
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	var packer *types.Packer
	put := engine.packerPool.Get(&packer)
	defer put.Put()

	if state != nil {
		t0 := time.Now()
		state.HandleLogtailEntry(ctx, engine.fs, e, primarySeqnum, packer, engine.mp)
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryLogtailReplayDurationHistogram.Observe(time.Since(t0).Seconds())
	}

	// Try to handle the memory records of the three tables
	if !catalog.IsSystemTable(e.TableId) || logtailreplay.IsMetaEntry(e.TableName) {
		return nil
	}

	if engine.PushClient().dcaTryDelay(isSub, func() { applyToCatalogCache(cache, e) }) {
		return nil
	}

	applyToCatalogCache(cache, e)
	return nil
}

func applyToCatalogCache(cache *cache.CatalogCache, e *api.Entry) {
	t0 := time.Now()
	if e.EntryType == api.Entry_Insert {
		switch e.TableId {
		case catalog.MO_TABLES_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				cache.InsertTable(bat)
			}
		case catalog.MO_DATABASE_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				cache.InsertDatabase(bat)
			}
		case catalog.MO_COLUMNS_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				cache.InsertColumns(bat)
			}
		}
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryUpdateCatalogCacheDurationHistogram.Observe(time.Since(t0).Seconds())
		return
	}

	switch e.TableId {
	case catalog.MO_TABLES_ID:
		if cache != nil && !logtailreplay.IsTransferredDels(e.TableName) {
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			cache.DeleteTable(bat)
		}
	case catalog.MO_DATABASE_ID:
		if cache != nil && !logtailreplay.IsTransferredDels(e.TableName) {
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			cache.DeleteDatabase(bat)
		}
	}
	v2.LogtailUpdatePartitonConsumeLogtailOneEntryUpdateCatalogCacheDurationHistogram.Observe(time.Since(t0).Seconds())
}
