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
) error {

	var packer *types.Packer
	put := engine.packerPool.Get(&packer)
	defer put.Put()

	if state != nil {
		state.HandleLogtailEntry(ctx, engine.fs, e, primarySeqnum, packer)
	}

	if logtailreplay.IsMetaTable(e.TableName) {
		return nil
	}

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
		return nil
	}

	switch e.TableId {
	case catalog.MO_TABLES_ID:
		bat, _ := batch.ProtoBatchToBatch(e.Bat)
		if cache != nil {
			cache.DeleteTable(bat)
		}
	case catalog.MO_DATABASE_ID:
		bat, _ := batch.ProtoBatchToBatch(e.Bat)
		if cache != nil {
			cache.DeleteDatabase(bat)
		}
	}

	return nil
}
