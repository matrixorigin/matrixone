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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

// Logtailer provides logtail for the specified table.
type Logtailer interface {
	// RangeLogtail returns logtail for all tables within the range (from, to].
	// NOTE: caller should keep time range monotonous, or there would be a checkpoint.
	RangeLogtail(
		ctx context.Context, from, to timestamp.Timestamp,
	) ([]logtail.TableLogtail, []func(), error)

	RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error)

	// TableLogtail returns logtail for the specified table.
	//
	// NOTE: If table not exist, logtail.TableLogtail shouldn't be a simple zero value.
	TableLogtail(
		ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
	) (logtail.TableLogtail, func(), error)

	// Now is a time getter from TxnManager. Users of Logtailer should get a timestamp
	// from Now and use the timestamp to collect logtail, in that case, all txn prepared
	// before it are visible.
	Now() (timestamp.Timestamp, timestamp.Timestamp)
}

var _ Logtailer = (*LogtailerImpl)(nil)

type LogtailerImpl struct {
	ctx       context.Context
	ckpClient CheckpointClient
	mgr       *Manager
	c         *catalog.Catalog
}

func NewLogtailer(
	ctx context.Context,
	ckpClient CheckpointClient,
	mgr *Manager,
	c *catalog.Catalog) *LogtailerImpl {
	return &LogtailerImpl{
		ctx:       ctx,
		ckpClient: ckpClient,
		mgr:       mgr,
		c:         c,
	}
}

// Now is a time getter from TxnManager. Users of Logtailer should get a timestamp
// from Now and use the timestamp to collect logtail, in that case, all txn prepared
// before it are visible.
func (l *LogtailerImpl) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	ts := l.mgr.nowClock() // now in logtail manager is the same with the one in TxnManager

	return ts.ToTimestamp(), timestamp.Timestamp{}
}

// TableLogtail returns logtail for the specified table.
// It boils down to calling `HandleSyncLogTailReq`
func (l *LogtailerImpl) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, func(), error) {
	req := api.SyncLogTailReq{
		CnHave: &from,
		CnWant: &to,
		Table:  &table,
	}
	resp, closeCB, err := HandleSyncLogTailReq(ctx, l.ckpClient, l.mgr, l.c, req, true)
	ret := logtail.TableLogtail{}
	if err != nil {
		return ret, closeCB, err
	}
	ret.CkpLocation = resp.CkpLocation
	ret.Ts = &to
	ret.Table = &table
	ret.Commands = nonPointerEntryList(resp.Commands)
	return ret, closeCB, nil
}
func (l *LogtailerImpl) RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error) {
	l.mgr.RegisterCallback(cb)
}

// RangeLogtail returns logtail for all tables that are modified within the range (from, to].
// Check out all dirty tables in the time window and collect logtails for every table
func (l *LogtailerImpl) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, []func(), error) {
	return nil, nil, moerr.NewNYI(ctx, "RangeLogtail is deprecated")
}

// TODO: remvove this after push mode is stable
func nonPointerEntryList(src []*api.Entry) []api.Entry {
	es := make([]api.Entry, len(src))
	for i, e := range src {
		es[i] = *e
	}
	return es
}
