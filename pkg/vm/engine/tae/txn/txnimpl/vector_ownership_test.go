// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

type rowLookupObject struct {
	data.Object
	lookup func(context.Context, containers.Vector) error
}

func (o *rowLookupObject) GetDuplicatedRows(ctx context.Context, _ txnif.TxnReader, _ containers.Vector, _ index.ZM, _ types.TS, _ types.TS, rows containers.Vector, _ *mpool.MPool) error {
	return o.lookup(ctx, rows)
}

func TestRowLookupCallersReleaseVectors(t *testing.T) {
	for _, caller := range []string{"GetByFilter", "DedupSnapByPK"} {
		for _, outcome := range []string{"error", "canceled", "deadline", "conflict", "missing", "found"} {
			t.Run(caller+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				var lookupErr error
				switch outcome {
				case "error":
					lookupErr = moerr.NewInternalErrorNoCtx("dedup read")
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					lookupErr = ctx.Err()
				case "deadline":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Time{})
					t.Cleanup(cancel)
					lookupErr = ctx.Err()
				case "conflict":
					lookupErr = moerr.NewTxnWWConflict(ctx, 2, "test conflict")
				}
				entry := catalog.MockTableEntryWithDB(catalog.MockDBEntryWithAccInfo(0, 1), 2)
				schema := entry.GetLastestSchema(false)
				require.NoError(t, schema.AppendPKCol("pk", types.T_int64.ToType(), 0))
				require.NoError(t, schema.Finalize(false))
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
				require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
				objectio.WithCNCreated()(stats)
				blk := objectio.NewBlockidWithObjectID(&id, 0)
				expectedRow := types.NewRowid(&blk, 7)
				calls := 0
				object := catalog.MockObjectEntry(entry, stats, false, func(*catalog.ObjectEntry) data.Object {
					return &rowLookupObject{lookup: func(actualCtx context.Context, rows containers.Vector) error {
						calls++
						require.Equal(t, ctx, actualCtx)
						if outcome == "conflict" || outcome == "found" {
							rows.Update(0, expectedRow, false)
						}
						return lookupErr
					}}
				}, types.BuildTS(2, 0))
				entry.AddEntryLocked(object)
				mp := mpool.MustNewZero()
				pool := containers.NewVectorPool(t.Name(), 2, containers.WithMPool(mp), containers.WithFixedSizeRatio(1))
				t.Cleanup(pool.Destory)
				txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(3, 0))
				table := &txnTable{entry: entry, store: &txnStore{ctx: ctx, txn: txn, rt: dbutils.NewRuntime(dbutils.WithRuntimeSmallPool(pool))}}
				table.dataTable = newBaseTable(schema, false, table)
				// rowIDs may use the pool or its fallback; check both checkout and allocator ownership.
				workspaceBefore := common.WorkspaceAllocator.CurrNB()
				if caller == "GetByFilter" {
					gotID, offset, err := table.GetByFilter(ctx, handle.NewEQFilter(int64(1)))
					if outcome == "conflict" || outcome == "found" {
						require.NoError(t, err)
						require.NotNil(t, gotID)
						require.Equal(t, blk, gotID.BlockID)
						require.Equal(t, uint32(7), offset)
					} else if lookupErr != nil {
						require.ErrorIs(t, err, lookupErr)
					} else {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
					}
				} else {
					keys := containers.MakeVector(types.T_int64.ToType(), mp)
					t.Cleanup(keys.Close)
					keys.Append(int64(1), false)
					err := table.DedupSnapByPK(ctx, keys, false)
					if lookupErr != nil {
						require.ErrorIs(t, err, lookupErr)
					} else if outcome == "found" {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
					} else {
						require.NoError(t, err)
					}
					keys.Close()
				}
				require.Equal(t, 1, calls)
				used, _ := pool.Used(false)
				require.Zero(t, used)
				// Returned pooled storage may remain cached; destruction must release it, including cross-pool allocations.
				pool.Destory()
				require.Zero(t, mp.CurrNB())
				require.Equal(t, workspaceBefore, common.WorkspaceAllocator.CurrNB())
			})
		}
	}
}

type ownershipAppender struct {
	data.ObjectAppender
	sameColumns              bool
	prepareErr               error
	meta                     *catalog.ObjectEntry
	locks, unlocks, prepared int
}

func (a *ownershipAppender) IsSameColumns(any) bool { return a.sameColumns }
func (a *ownershipAppender) LockFreeze()            { a.locks++ }
func (a *ownershipAppender) UnlockFreeze()          { a.unlocks++ }
func (*ownershipAppender) CheckFreeze() bool        { return false }
func (a *ownershipAppender) GetMeta() any           { return a.meta }
func (a *ownershipAppender) PrepareAppend(_ bool, _ uint32, _ txnif.AsyncTxn) (txnif.AppendNode, bool, uint32, error) {
	a.prepared++
	if a.prepareErr != nil {
		return nil, false, 0, a.prepareErr
	}
	return updates.NewAppendNode(nil, 0, 1, false, nil), true, 1, nil
}

type ownershipTableHandle struct {
	data.TableHandle
	appender *ownershipAppender
}

func (h ownershipTableHandle) GetAppender() (data.ObjectAppender, error) { return h.appender, nil }

func TestPrepareApplyANodeVectorOwnership(t *testing.T) {
	for _, scenario := range []string{"schema_error", "prepare_error", "late_error", "transfer", "borrowed_error", "borrowed_success"} {
		t.Run(scenario, func(t *testing.T) {
			schema := catalog.MockSchemaAll(4, 3)
			entry := catalog.MockStaloneTableEntry(1, schema)
			mp := mpool.MustNewZero()
			pool := containers.NewVectorPool(t.Name(), 2, containers.WithMPool(mp), containers.WithFixedSizeRatio(1))
			t.Cleanup(pool.Destory)
			seed := pool.GetVector(&objectio.RowidType)
			t.Cleanup(seed.Close)
			require.NoError(t, seed.PreExtend(1))
			seed.Close()
			table := &txnTable{entry: entry, store: &txnStore{isOffline: true, rt: dbutils.NewRuntime(dbutils.WithRuntimeSmallPool(pool))}}
			table.dataTable = newBaseTable(schema, false, table)
			bat := containers.NewBatch()
			t.Cleanup(bat.Close)
			for i, typ := range schema.AllTypes() {
				bat.AddVector(schema.ColDefs[i].Name, containers.MakeVector(typ, mp))
			}
			old := bat.Vecs[schema.PhyAddrKey.Idx]
			offset := uint32(0)
			rows := 1
			if scenario == "transfer" {
				rows = 0
			}
			if scenario == "borrowed_error" || scenario == "borrowed_success" {
				rows = 2
				offset = 1
				old.Close()
				old = pool.GetVector(&objectio.RowidType)
				bat.Vecs[schema.PhyAddrKey.Idx] = old
			}
			for _, v := range bat.Vecs {
				for range rows {
					v.Append(nil, true)
				}
			}
			if scenario == "borrowed_success" {
				offset = uint32(rows)
			}
			node := &anode{data: bat}
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
			app := &ownershipAppender{sameColumns: scenario != "schema_error" && scenario != "borrowed_error", meta: catalog.MockObjectEntry(entry, stats, false, nil, types.BuildTS(2, 0))}
			sentinel := moerr.NewInternalErrorNoCtx("prepare append")
			if scenario == "prepare_error" {
				app.prepareErr = sentinel
			}
			space := &tableSpace{table: table, tableHandle: ownershipTableHandle{appender: app}}
			err := space.prepareApplyANode(node, offset)
			switch scenario {
			case "schema_error", "borrowed_error":
				require.ErrorContains(t, err, "schema changed")
			case "prepare_error":
				require.ErrorIs(t, err, sentinel)
			case "late_error":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOfflineTxnWrite))
				require.Equal(t, 1, app.prepared)
			default:
				require.NoError(t, err)
			}
			require.Equal(t, app.locks, app.unlocks)
			used, _ := pool.Used(false)
			if scenario == "transfer" {
				require.NotSame(t, old, bat.Vecs[schema.PhyAddrKey.Idx])
				require.Equal(t, 1, used)
				require.Zero(t, bat.Vecs[schema.PhyAddrKey.Idx].Length())
			} else {
				require.Same(t, old, bat.Vecs[schema.PhyAddrKey.Idx])
				require.Equal(t, rows, old.Length())
				if offset > 0 {
					require.Equal(t, 1, used)
					require.True(t, old.IsNull(0))
				} else {
					require.Zero(t, used)
				}
			}
			bat.Close()
			used, _ = pool.Used(false)
			require.Zero(t, used)
			pool.Destory()
			require.Zero(t, mp.CurrNB())
		})
	}
}
