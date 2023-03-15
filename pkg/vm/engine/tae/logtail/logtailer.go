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
	"context"
	"math"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"go.uber.org/zap"
)

// Logtailer provides logtail for the specified table.
type Logtailer interface {
	// RangeLogtail returns logtail for all tables within the range (from, to].
	// NOTE: caller should keep time range monotonous, or there would be a checkpoint.
	RangeLogtail(
		ctx context.Context, from, to timestamp.Timestamp,
	) ([]logtail.TableLogtail, error)

	// TableLogtail returns logtail for the specified table.
	//
	// NOTE: If table not exist, logtail.TableLogtail shouldn't be a simple zero value.
	TableLogtail(
		ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
	) (logtail.TableLogtail, error)

	// Now is a time getter from TxnManager. Users of Logtailer should get a timestamp
	// from Now and use the timestamp to collect logtail, in that case, all txn prepared
	// before it are visible.
	Now() (timestamp.Timestamp, timestamp.Timestamp)
}

var _ Logtailer = (*LogtailerImpl)(nil)

type LogtailerImpl struct {
	ckpClient CheckpointClient
	mgr       *Manager
	c         *catalog.Catalog
}

func NewLogtailer(
	ckpClient CheckpointClient,
	mgr *Manager,
	c *catalog.Catalog) *LogtailerImpl {
	return &LogtailerImpl{
		ckpClient: ckpClient,
		mgr:       mgr,
		c:         c,
	}
}

// Now is a time getter from TxnManager. Users of Logtailer should get a timestamp
// from Now and use the timestamp to collect logtail, in that case, all txn prepared
// before it are visible.
func (l *LogtailerImpl) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	ts := l.mgr.now() // now in logtail manager is the same with the one in TxnManager

	return ts.ToTimestamp(), timestamp.Timestamp{}
}

// TableLogtail returns logtail for the specified table.
// It boils down to calling `HandleSyncLogTailReq`
func (l *LogtailerImpl) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, error) {
	req := api.SyncLogTailReq{
		CnHave: &from,
		CnWant: &to,
		Table:  &table,
	}
	resp, err := HandleSyncLogTailReq(ctx, l.ckpClient, l.mgr, l.c, req, true)
	ret := logtail.TableLogtail{}
	if err != nil {
		return ret, err
	}
	ret.CkpLocation = resp.CkpLocation
	ret.Ts = &to
	ret.Table = &table
	ret.Commands = nonPointerEntryList(resp.Commands)
	return ret, nil
}

// RangeLogtail returns logtail for all tables that are modified within the range (from, to].
// Check out all dirty tables in the time window and collect logtails for every table
func (l *LogtailerImpl) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, error) {
	start := types.BuildTS(from.PhysicalTime, from.LogicalTime)
	end := types.BuildTS(to.PhysicalTime, to.LogicalTime)

	ckpLoc, checkpointed, err := l.ckpClient.CollectCheckpointsInRange(ctx, start, end)
	if err != nil {
		return nil, err
	}

	if checkpointed.GreaterEq(end) {
		return []logtail.TableLogtail{{
			CkpLocation: ckpLoc,
			Ts:          &to,
			Table:       &api.TableID{DbId: math.MaxUint64, TbId: math.MaxUint64},
		}}, nil
	} else if ckpLoc != "" {
		start = checkpointed.Next()
	}

	reader := l.mgr.GetReader(start, end)
	resps := make([]logtail.TableLogtail, 0, 8)

	// collect resp for the three system tables
	if reader.HasCatalogChanges() {
		for _, scope := range []Scope{ScopeDatabases, ScopeTables, ScopeColumns} {
			resp, err := l.getCatalogRespBuilder(scope, reader, ckpLoc).build()
			if err != nil {
				return nil, err
			}
			resps = append(resps, resp)
		}
	}

	// collect resp for every dirty normal table
	dirties, _ := reader.GetDirty()
	for _, table := range dirties.Tables {
		did, tid := table.DbID, table.ID
		resp, err := l.getTableRespBuilder(did, tid, reader, ckpLoc).build()
		if err != nil {
			return resps, err
		}
		resps = append(resps, resp)
	}
	return resps, nil
}

func (l *LogtailerImpl) getTableRespBuilder(did, tid uint64, reader *Reader, ckpLoc string) *tableRespBuilder {
	return &tableRespBuilder{
		did:    did,
		tid:    tid,
		scope:  ScopeUserTables,
		reader: reader,
		c:      l.c,
	}
}

func (l *LogtailerImpl) getCatalogRespBuilder(scope Scope, reader *Reader, ckpLoc string) *tableRespBuilder {
	b := &tableRespBuilder{
		did:    pkgcatalog.MO_CATALOG_ID,
		scope:  scope,
		reader: reader,
		c:      l.c,
	}
	switch scope {
	case ScopeDatabases:
		b.tid = pkgcatalog.MO_DATABASE_ID
	case ScopeTables:
		b.tid = pkgcatalog.MO_TABLES_ID
	case ScopeColumns:
		b.tid = pkgcatalog.MO_COLUMNS_ID
	}
	return b
}

type tableRespBuilder struct {
	did, tid uint64
	ckpLoc   string
	scope    Scope
	reader   *Reader
	c        *catalog.Catalog
}

func (b *tableRespBuilder) build() (logtail.TableLogtail, error) {
	resp, err := b.collect()
	if err != nil {
		return logtail.TableLogtail{}, err
	}
	ret := logtail.TableLogtail{}
	ret.CkpLocation = resp.CkpLocation
	to := b.reader.to.ToTimestamp()
	ret.Ts = &to
	ret.Table = &api.TableID{DbId: b.did, TbId: b.tid}
	ret.Commands = nonPointerEntryList(resp.Commands)
	return ret, nil
}

func (b *tableRespBuilder) collect() (api.SyncLogTailResp, error) {
	var builder RespBuilder
	if b.scope == ScopeUserTables {
		dbEntry, err := b.c.GetDatabaseByID(b.did)
		if err != nil {
			logutil.Info("[Logtail] not found", zap.Any("db_id", b.did))
			return api.SyncLogTailResp{}, nil
		}
		tableEntry, err := dbEntry.GetTableEntryByID(b.tid)
		if err != nil {
			logutil.Info("[Logtail] not found", zap.Any("t_id", b.tid))
			return api.SyncLogTailResp{}, nil
		}
		builder = NewTableLogtailRespBuilder(b.ckpLoc, b.reader.from, b.reader.to, tableEntry)
	} else {
		builder = NewCatalogLogtailRespBuilder(b.scope, b.ckpLoc, b.reader.from, b.reader.to)
	}
	op := NewBoundTableOperator(b.c, b.reader, b.scope, b.did, b.tid, builder)
	err := op.Run()
	if err != nil {
		return api.SyncLogTailResp{}, err
	}

	return builder.BuildResp()
}

// TODO: remvove this after push mode is stable
func nonPointerEntryList(src []*api.Entry) []api.Entry {
	es := make([]api.Entry, len(src))
	for i, e := range src {
		es[i] = *e
	}
	return es
}
