// Copyright 2021 Matrix Origin
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

package logtail

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func LogtailHandler(mgr *LogtailMgr, c *catalog.Catalog, req api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	verifiedCheckpoint := ""
	// TODO
	// verifiedCheckpoint, start, end = db.CheckpointMgr.check(start, end)

	// get a collector with a read only reader for txns
	collector := mgr.GetLogtailCollector(start, end, did, tid)

	var mode CollectMode
	switch tid {
	case pkgcatalog.MO_DATABASE_ID:
		mode = CollectModeDB
	case pkgcatalog.MO_TABLES_ID:
		mode = CollectModeTbl
	case pkgcatalog.MO_COLUMNS_ID:
		mode = CollectModeCol
	default:
		mode = CollectModeSegAndBlk
	}

	var respBuilder RespBuilder

	if mode == CollectModeSegAndBlk {
		var tableEntry *catalog.TableEntry
		// table logtail needs information about this table, so give it the table entry.
		if db, err := c.GetDatabaseByID(did); err != nil {
			return api.SyncLogTailResp{}, err
		} else if tableEntry, err = db.GetTableEntryByID(tid); err != nil {
			return api.SyncLogTailResp{}, err
		}
		respBuilder = NewTableLogtailRespBuilder(verifiedCheckpoint, start, end, tableEntry)
	} else {
		respBuilder = NewCatalogLogtailRespBuilder(mode, verifiedCheckpoint, start, end)
	}

	collector.BindCollectEnv(mode, c, respBuilder)
	if err := collector.Collect(); err != nil {
		return api.SyncLogTailResp{}, err
	}
	return respBuilder.BuildResp()
}
