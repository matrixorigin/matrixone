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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"

	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func calcNdvUsingZonemap(zm objectio.ZoneMap, t *types.Type) float64 {
	if !zm.IsInited() {
		return -1 /*for new added column, its zonemap will be empty and not initialized*/
	}
	switch t.Oid {
	case types.T_bool:
		return 2
	case types.T_int8:
		return float64(types.DecodeFixed[int8](zm.GetMaxBuf())) - float64(types.DecodeFixed[int8](zm.GetMinBuf())) + 1
	case types.T_int16:
		return float64(types.DecodeFixed[int16](zm.GetMaxBuf())) - float64(types.DecodeFixed[int16](zm.GetMinBuf())) + 1
	case types.T_int32:
		return float64(types.DecodeFixed[int32](zm.GetMaxBuf())) - float64(types.DecodeFixed[int32](zm.GetMinBuf())) + 1
	case types.T_int64:
		return float64(types.DecodeFixed[int64](zm.GetMaxBuf())) - float64(types.DecodeFixed[int64](zm.GetMinBuf())) + 1
	case types.T_uint8:
		return float64(types.DecodeFixed[uint8](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint8](zm.GetMinBuf())) + 1
	case types.T_uint16:
		return float64(types.DecodeFixed[uint16](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint16](zm.GetMinBuf())) + 1
	case types.T_uint32:
		return float64(types.DecodeFixed[uint32](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint32](zm.GetMinBuf())) + 1
	case types.T_uint64:
		return float64(types.DecodeFixed[uint64](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint64](zm.GetMinBuf())) + 1
	case types.T_decimal64:
		return types.Decimal64ToFloat64(types.DecodeFixed[types.Decimal64](zm.GetMaxBuf()), t.Scale) -
			types.Decimal64ToFloat64(types.DecodeFixed[types.Decimal64](zm.GetMinBuf()), t.Scale) + 1
	case types.T_decimal128:
		return types.Decimal128ToFloat64(types.DecodeFixed[types.Decimal128](zm.GetMaxBuf()), t.Scale) -
			types.Decimal128ToFloat64(types.DecodeFixed[types.Decimal128](zm.GetMinBuf()), t.Scale) + 1
	case types.T_float32:
		return float64(types.DecodeFixed[float32](zm.GetMaxBuf())) - float64(types.DecodeFixed[float32](zm.GetMinBuf())) + 1
	case types.T_float64:
		return types.DecodeFixed[float64](zm.GetMaxBuf()) - types.DecodeFixed[float64](zm.GetMinBuf()) + 1
	case types.T_timestamp:
		return float64(types.DecodeFixed[types.Timestamp](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Timestamp](zm.GetMinBuf())) + 1
	case types.T_date:
		return float64(types.DecodeFixed[types.Date](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Date](zm.GetMinBuf())) + 1
	case types.T_time:
		return float64(types.DecodeFixed[types.Time](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Time](zm.GetMinBuf())) + 1
	case types.T_datetime:
		return float64(types.DecodeFixed[types.Datetime](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Datetime](zm.GetMinBuf())) + 1
	case types.T_uuid, types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text,
		types.T_array_float32, types.T_array_float64:
		//NDV Function
		// An aggregate function that returns an approximate value similar to the result of COUNT(DISTINCT col),
		// the "number of distinct values".
		return -1
	case types.T_enum:
		return float64(types.DecodeFixed[types.Enum](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Enum](zm.GetMinBuf())) + 1
	default:
		return -1
	}
}

func getMinMaxValueByFloat64(typ types.Type, buf []byte) (float64, bool) {
	switch typ.Oid {
	case types.T_int8:
		return float64(types.DecodeInt8(buf)), true
	case types.T_int16:
		return float64(types.DecodeInt16(buf)), true
	case types.T_int32:
		return float64(types.DecodeInt32(buf)), true
	case types.T_int64:
		return float64(types.DecodeInt64(buf)), true
	case types.T_uint8:
		return float64(types.DecodeUint8(buf)), true
	case types.T_uint16:
		return float64(types.DecodeUint16(buf)), true
	case types.T_uint32:
		return float64(types.DecodeUint32(buf)), true
	case types.T_uint64:
		return float64(types.DecodeUint64(buf)), true
	case types.T_date:
		return float64(types.DecodeDate(buf)), true
	case types.T_char, types.T_varchar, types.T_text:
		return float64(plan2.ByteSliceToUint64(buf)), true
	default:
		return 0, false
	}
}

// get ndv, minval , maxval, datatype from zonemap. Retrieve all columns except for rowid, return accurate number of objects
func updateInfoFromZoneMap(info *plan2.InfoFromZoneMap, ctx context.Context, tbl *txnTable) error {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateInfoFromZonemapHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(tbl.tableDef.Cols) - 1 /* row-id */
	proc := tbl.db.txn.proc
	tableDef := tbl.GetTableDef(ctx)
	var (
		init    bool
		err     error
		part    *logtailreplay.PartitionState
		meta    objectio.ObjectDataMeta
		objMeta objectio.ObjectMeta
	)
	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return err
	}

	onObjFn := func(obj logtailreplay.ObjectEntry) error {
		location := obj.Location()
		if objMeta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
			return err
		}
		meta = objMeta.MustDataMeta()
		info.AccurateObjectNumber++
		info.BlockNumber += int(obj.BlkCnt())
		info.TableCnt += float64(meta.BlockHeader().Rows())
		if !init {
			init = true
			for idx, col := range tableDef.Cols[:lenCols] {
				objColMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] = int64(objColMeta.NullCnt())
				info.ColumnZMs[idx] = objColMeta.ZoneMap().Clone()
				info.DataTypes[idx] = types.T(col.Typ.Id).ToType()
				info.ColumnNDVs[idx] = float64(objColMeta.Ndv())
				if info.ColumnNDVs[idx] > 100 {
					info.ShuffleRanges[idx] = plan2.NewShuffleRange()
					minvalue, succ := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMinBuf())
					if !succ {
						info.ShuffleRanges[idx] = nil
					} else {
						maxvalue, _ := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMaxBuf())
						info.ShuffleRanges[idx].Update(minvalue, maxvalue, meta.BlockHeader().Rows(), objColMeta.NullCnt())
					}
				}
			}
		} else {
			for idx, col := range tableDef.Cols[:lenCols] {
				objColMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] += int64(objColMeta.NullCnt())
				zm := objColMeta.ZoneMap().Clone()
				if !zm.IsInited() {
					continue
				}
				index.UpdateZM(info.ColumnZMs[idx], zm.GetMaxBuf())
				index.UpdateZM(info.ColumnZMs[idx], zm.GetMinBuf())
				info.ColumnNDVs[idx] += float64(objColMeta.Ndv())
				if info.ShuffleRanges[idx] != nil {
					minvalue, succ := getMinMaxValueByFloat64(info.DataTypes[idx], zm.GetMinBuf())
					if !succ {
						info.ShuffleRanges[idx] = nil
					} else {
						maxvalue, _ := getMinMaxValueByFloat64(info.DataTypes[idx], zm.GetMaxBuf())
						info.ShuffleRanges[idx].Update(minvalue, maxvalue, meta.BlockHeader().Rows(), objColMeta.NullCnt())
					}
				}
			}
		}
		return nil
	}
	if err = tbl.ForeachVisibleDataObject(part, onObjFn); err != nil {
		return err
	}

	return nil
}

func adjustNDV(info *plan2.InfoFromZoneMap, tbl *txnTable) {
	tableDef := tbl.GetTableDef(context.TODO())
	lenCols := len(tbl.tableDef.Cols) - 1 /* row-id */

	if info.AccurateObjectNumber > 1 {
		for idx := range tableDef.Cols[:lenCols] {
			rate := info.ColumnNDVs[idx] / info.TableCnt
			if rate > 1 {
				rate = 1
			}
			if rate < 0.1 {
				info.ColumnNDVs[idx] /= math.Pow(float64(info.AccurateObjectNumber), (1 - rate))
			}
			ndvUsingZonemap := calcNdvUsingZonemap(info.ColumnZMs[idx], &info.DataTypes[idx])
			if ndvUsingZonemap != -1 && info.ColumnNDVs[idx] > ndvUsingZonemap {
				info.ColumnNDVs[idx] = ndvUsingZonemap
			}

			if info.ColumnNDVs[idx] > info.TableCnt {
				info.ColumnNDVs[idx] = info.TableCnt
			}
		}
	}
}

// calculate and update the stats for scan node.
func UpdateStats(ctx context.Context, tbl *txnTable, s *plan2.StatsInfoMap, approxNumObjects int) bool {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateStatsDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(tbl.tableDef.Cols) - 1 /* row-id */
	info := plan2.NewInfoFromZoneMap(lenCols)
	info.ApproxObjectNumber = approxNumObjects
	err := updateInfoFromZoneMap(info, ctx, tbl)
	if err != nil || info.ApproxObjectNumber == 0 {
		return false
	}
	adjustNDV(info, tbl)
	plan2.UpdateStatsInfoMap(info, tbl.GetTableDef(ctx), s)
	return true
}

// calculate and update the stats for scan node.
func UpdateStatsForPartitionTable(ctx context.Context, baseTable *txnTable, partitionTables []any, s *plan2.StatsInfoMap, approxNumObjects int) bool {
	if len(partitionTables) == 0 {
		return false
	}
	lenCols := len(baseTable.tableDef.Cols) - 1 /* row-id */
	info := plan2.NewInfoFromZoneMap(lenCols)
	info.ApproxObjectNumber = approxNumObjects
	for _, partitionTable := range partitionTables {
		ptable := partitionTable.(*txnTable)
		err := updateInfoFromZoneMap(info, ctx, ptable)
		if err != nil {
			return false
		}
	}
	if info.ApproxObjectNumber == 0 {
		return false
	}
	adjustNDV(info, baseTable)
	plan2.UpdateStatsInfoMap(info, baseTable.GetTableDef(ctx), s)
	return true
}
