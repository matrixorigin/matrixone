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

	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func calcNdvUsingZonemap(zm objectio.ZoneMap, t *types.Type) float64 {
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
	case types.T_uuid, types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return -1
	default:
		return -1
	}
}

// get ndv, minval , maxval, datatype from zonemap. Retrieve all columns except for rowid
func getInfoFromZoneMap(ctx context.Context, blocks []catalog.BlockInfo, tableDef *plan.TableDef, proc *process.Process) (*plan2.InfoFromZoneMap, error) {
	var tableCnt float64
	lenCols := len(tableDef.Cols) - 1 /* row-id */
	info := plan2.NewInfoFromZoneMap(lenCols)

	var objectMeta objectio.ObjectMeta
	lenobjs := 0

	var init bool
	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	for _, blk := range blocks {
		location := blk.MetaLocation()
		fs, err := fileservice.Get[fileservice.FileService](fs, defines.SharedFileServiceName)
		if err != nil {
			return nil, err
		}

		if !objectio.IsSameObjectLocVsMeta(location, objectMeta) {
			if objectMeta, err = objectio.FastLoadObjectMeta(ctx, &location, fs); err != nil {
				return nil, err
			}
			lenobjs++
			tableCnt += float64(objectMeta.BlockHeader().Rows())
			if !init {
				init = true
				for idx, col := range tableDef.Cols[:lenCols] {
					objColMeta := objectMeta.MustGetColumn(uint16(col.Seqnum))
					info.ColumnZMs[idx] = objColMeta.ZoneMap().Clone()
					info.DataTypes[idx] = types.T(col.Typ.Id).ToType()
					info.ColumnNDVs[idx] = float64(objColMeta.Ndv())
				}
			} else {
				for idx, col := range tableDef.Cols[:lenCols] {
					objColMeta := objectMeta.MustGetColumn(uint16(col.Seqnum))
					zm := objColMeta.ZoneMap().Clone()
					if !zm.IsInited() {
						continue
					}
					index.UpdateZM(info.ColumnZMs[idx], zm.GetMaxBuf())
					index.UpdateZM(info.ColumnZMs[idx], zm.GetMinBuf())
					info.ColumnNDVs[idx] += float64(objColMeta.Ndv())
				}
			}
		}
	}

	//adjust ndv
	if lenobjs > 1 {
		for idx := range tableDef.Cols[:lenCols] {
			rate := info.ColumnNDVs[idx] / tableCnt
			if rate > 1 {
				rate = 1
			}
			if rate < 0.1 {
				info.ColumnNDVs[idx] /= math.Pow(float64(lenobjs), (1 - rate))
			}
			ndvUsingZonemap := calcNdvUsingZonemap(info.ColumnZMs[idx], &info.DataTypes[idx])
			if ndvUsingZonemap != -1 && info.ColumnNDVs[idx] > ndvUsingZonemap {
				info.ColumnNDVs[idx] = ndvUsingZonemap
			}

			if info.ColumnNDVs[idx] > tableCnt {
				info.ColumnNDVs[idx] = tableCnt
			}
		}
	}

	info.TableCnt = tableCnt
	return info, nil
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(
	ctx context.Context,
	blocks []catalog.BlockInfo,
	tableDef *plan.TableDef,
	proc *process.Process,
	s *plan2.StatsInfoMap,
) bool {
	blockNumTotal := len(blocks)
	if blockNumTotal == 0 {
		return false
	}

	if s.NeedUpdate(blockNumTotal) {
		info, err := getInfoFromZoneMap(ctx, blocks, tableDef, proc)
		if err != nil {
			return false
		}
		plan2.UpdateStatsInfoMap(info, blockNumTotal, tableDef, s)
	}
	return true
}
