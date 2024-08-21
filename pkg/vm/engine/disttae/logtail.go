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
	"fmt"
	"os"
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
	engine TempEngine,
	cache *cache.CatalogCache,
	state *logtailreplay.PartitionState,
	e *api.Entry,
) error {
	start := time.Now()
	defer func() {
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	var packer *types.Packer
	put := engine.Get(&packer)
	defer put.Put()

	if state != nil {
		t0 := time.Now()
		state.HandleLogtailEntry(ctx, engine.GetFS(), e, primarySeqnum, packer, engine.GetMPool())
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryLogtailReplayDurationHistogram.Observe(time.Since(t0).Seconds())
	}

	if logtailreplay.IsMetaTable(e.TableName) {
		return nil
	}

	if !engine.PushClient().receivedLogTailTime.ready.Load() {
		return nil
	}

	t0 := time.Now()
	if e.EntryType == api.Entry_Insert {
		switch e.TableId {
		case catalog.MO_TABLES_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				if engine.IsCdcEngine() {
					fmt.Fprintln(os.Stderr, "%%%%%> insert table into catalog cache")
					cache.PrintTables2("before", "test")
				}

				cache.InsertTable(bat)

				if engine.IsCdcEngine() {
					//recognize ddl
					engine.GetDDLListener().OnAction(ActionInsertTable, bat)
					cache.PrintTables2("after", "test")
				}

			}
		case catalog.MO_DATABASE_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				if engine.IsCdcEngine() {
					fmt.Fprintln(os.Stderr, "%%%%%> insert database into catalog cache")
					cache.PrintDatabases("before")
				}

				cache.InsertDatabase(bat)

				if engine.IsCdcEngine() {
					engine.GetDDLListener().OnAction(ActionInsertDatabase, bat)
					cache.PrintDatabases("after")
				}
			}
		case catalog.MO_COLUMNS_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if cache != nil {
				cache.InsertColumns(bat)
			}
		}
		v2.LogtailUpdatePartitonConsumeLogtailOneEntryUpdateCatalogCacheDurationHistogram.Observe(time.Since(t0).Seconds())
		return nil
	}

	switch e.TableId {
	case catalog.MO_TABLES_ID:
		if cache != nil && !logtailreplay.IsTransferredDels(e.TableName) {
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if engine.IsCdcEngine() {
				fmt.Fprintln(os.Stderr, "%%%%%> delete table from catalog cache")
				cache.PrintTables2("before", "test")
			}

			cache.DeleteTable(bat)

			if engine.IsCdcEngine() {
				//recognize ddl
				engine.GetDDLListener().OnAction(ActionDeleteTable, bat)
				cache.PrintTables2("after", "test")
			}
		}
	case catalog.MO_DATABASE_ID:
		if cache != nil && !logtailreplay.IsTransferredDels(e.TableName) {
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			if engine.IsCdcEngine() {
				fmt.Fprintln(os.Stderr, "%%%%%> delete database from catalog cache")
				cache.PrintDatabases("before")
			}

			cache.DeleteDatabase(bat)

			if engine.IsCdcEngine() {
				engine.GetDDLListener().OnAction(ActionDeleteDatabase, bat)
				cache.PrintDatabases("after")
			}
		}
	}
	v2.LogtailUpdatePartitonConsumeLogtailOneEntryUpdateCatalogCacheDurationHistogram.Observe(time.Since(t0).Seconds())

	return nil
}
