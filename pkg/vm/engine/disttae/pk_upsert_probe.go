// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"go.uber.org/zap"
)

// pkUpsertPersistedBetween checks changed objects without treating the amount
// of history as a conflict. The key vector is owned by the caller of this
// helper, and can be sorted before any zone-map pruning.
func (tbl *txnTable) pkUpsertPersistedBetween(
	ctx context.Context, p *logtailreplay.PartitionState,
	from, to types.TS, keys *vector.Vector,
) (changed bool, err error) {
	start := time.Now()
	var created, deleted, candidates int
	var reason string
	v2.TxnPKChangeCheckTotalCounter.Inc()
	defer func() {
		if err == nil && changed {
			v2.TxnPKChangeCheckChangedCounter.Inc()
		}
		if changed && shouldLogPKPersistedChanged(tbl.tableId) {
			logutil.Info("txn pk persisted check changed",
				zap.String("reason", reason), zap.String("table-name", tbl.tableName),
				zap.String("txn", tbl.db.op.Txn().DebugString()),
				zap.String("from", from.ToString()), zap.String("to", to.ToString()),
				zap.Int("key-count", keys.Length()), zap.Int("changed-objects", created),
				zap.Int("deleted-objects", deleted), zap.Int("candidate-blocks", candidates),
				zap.Bool("check-tombstone", false), zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		}
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := keys.InplaceSort(tbl.getTxn().proc.Mp()); err != nil {
		return false, err
	}
	def := tbl.tableDef
	pkDef := def.Cols[tbl.primaryIdx]
	pkSeq := uint16(pkDef.Seqnum)
	pkType := plan2.ExprType2Type(&pkDef.Typ)
	isFakePK := catalog.IsFakePkName(def.Pkey.PkeyColName)
	statsArePK := !isFakePK && def.ClusterBy == nil && pkDef.Name == def.Pkey.PkeyColName
	fs := tbl.getTxn().engine.fs
	mp := tbl.proc.Load().GetMPool()

	// A zone map is only an absence proof if its type and ordering match the
	// query keys. Missing/uninitialized maps deliberately fail open.
	usableZM := func(zm objectio.ZoneMap) bool {
		return keys.GetSorted() && zm.IsInited() && zm.GetType() == keys.GetType().Oid
	}

	var filter objectio.BlockReadFilter
	filterReady := false
	defer func() {
		if filter.Cleanup != nil {
			filter.Cleanup()
		}
	}()
	prepareFilter := func() error {
		if filterReady {
			return nil
		}
		data, err := keys.MarshalBinary()
		if err != nil {
			return err
		}
		col := readutil.NewColumnExpr(0, plan2.MakePlan2Type(keys.GetType()), def.Pkey.PkeyColName)
		expr := plan2.MakeInExpr(ctx, col, int32(keys.Length()), data, false)
		base, err := readutil.ConstructBasePKFilter(expr, def, mp)
		if err != nil {
			return err
		}
		filter, err = readutil.ConstructBlockPKFilter(isFakePK, base, nil)
		if err != nil {
			return err
		}
		filterReady = true
		return nil
	}
	var linearSearch func(*vector.Vector) []int64
	cacheVectors := containers.NewVectors(3)
	ioCounted := false

	err = p.VisitChangedDataObjects(ctx, from, types.MaxTs(), func(obj objectio.ObjectEntry, isDelete bool) (bool, error) {
		if isDelete {
			deleted++
		} else {
			created++
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		statsChecked := false
		if statsArePK {
			zm := obj.SortKeyZoneMap()
			if usableZM(zm) {
				if !zm.AnyIn(keys) {
					return true, nil
				}
				statsChecked = true
			}
		}

		// Include metadata and Bloom reads in the same global budget as block
		// reads. One object owns one permit; it never nests another admission.
		if err := acquirePKCheckSemaphore(ctx); err != nil {
			return false, err
		}
		defer releasePKCheckSemaphore()
		if err := ctx.Err(); err != nil {
			return false, err
		}
		location := obj.Location()
		objMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			reason = "data_object_check_error"
			return false, err
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		meta := objMeta.MustDataMeta()
		if !statsChecked {
			zm := meta.MustGetColumn(pkSeq).ZoneMap()
			if usableZM(zm) && !zm.AnyIn(keys) {
				return true, nil
			}
		}
		var bf objectio.BloomFilter
		if !isFakePK && meta.BlockHeader().BFExtent().Length() > 0 {
			bf, err = objectio.LoadBFWithMeta(ctx, meta, location, fs)
			if err != nil {
				reason = "data_object_check_error"
				return false, err
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}

		var matched bool
		objectio.ForeachBlkInObjStatsList(false, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
			if err = ctx.Err(); err != nil {
				return false
			}
			var blockZM objectio.ZoneMap
			if !blkMeta.IsEmpty() {
				blockZM = blkMeta.MustGetColumn(pkSeq).ZoneMap()
			}
			if usableZM(blockZM) && !blockZM.AnyIn(keys) {
				return true
			}
			if len(bf) > 0 {
				blkBF := bf.GetBloomFilter(uint32(blk.BlockID.Sequence()))
				// The BF index can be present while an individual block has no
				// filter. An absent filter cannot prove absence of a key.
				if len(blkBF) > 0 {
					bfIndex := index.NewEmptyBloomFilter()
					if err = index.DecodeBloomFilter(bfIndex, blkBF); err != nil {
						reason = "data_object_check_error"
						return false
					}
					lower, upper := 0, keys.Length()
					if usableZM(blockZM) {
						lower, upper = blockZM.SubVecIn(keys)
					}
					if !bfIndex.MayContainsAny(keys, lower, upper) {
						return true
					}
				}
			}
			candidates++
			if err = prepareFilter(); err != nil {
				return false
			}
			if !ioCounted {
				ioCounted = true
				v2.TxnPKChangeCheckIOCounter.Inc()
			}
			blk.SetFlagByObjStats(&obj.ObjectStats)
			search := filter.DecideSearchFunc(blk.IsSorted())
			if search == nil {
				if linearSearch == nil {
					linearSearch = LinearSearchOffsetByValFactory(keys)
				}
				search = func(vs containers.Vectors) []int64 {
					if len(vs) == 0 || vs[0].Length() == 0 {
						return nil
					}
					return linearSearch(&vs[0])
				}
			}
			var usable bool
			if filter.CachedSearch != nil {
				matched, usable, _, err = ioutil.LoadColumnDataBySearchAndCheckTS(
					ctx, pkSeq, pkType, fs, blk.MetaLocation(), filter.CachedSearch,
					blk.IsSorted() && !filter.HasFakePK, objectio.SEQNUM_COMMITTS,
					from, to, mp, fileservice.Policy(0))
			} else {
				var release func()
				release, _, err = ioutil.LoadColumns(ctx,
					[]uint16{pkSeq, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
					[]types.Type{pkType, objectio.TSType, types.T_bool.ToType()},
					fs, blk.MetaLocation(), cacheVectors, mp, fileservice.Policy(0))
				if err == nil {
					if err = ctx.Err(); err == nil {
						sels := search(cacheVectors)
						if len(sels) == 0 {
							matched, usable = false, true
						} else {
							matched, usable = pkCommitTSMatchedInRange(&cacheVectors[1], &cacheVectors[2], sels, from, to)
						}
					}
					release()
				}
			}
			if err != nil {
				reason = "data_block_read_error"
				return false
			}
			if err = ctx.Err(); err != nil {
				return false
			}
			if !usable {
				matched = true
				reason = "data_commit_ts_unavailable"
			} else if matched {
				reason = "data_commit_ts_hit"
			}
			return !matched
		}, obj.ObjectStats)
		if err != nil {
			return false, err
		}
		if matched {
			changed = true
			return false, nil
		}
		return true, nil
	})
	return changed, err
}
