// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// updateColumnSummaries 合并一个已提交对象的精确列摘要；累加器不别名到缓存。
func updateColumnSummaries(ctx context.Context, meta objectio.ObjectDataMeta, location objectio.Location,
	cols []*plan.ColDef, zms []objectio.ZoneMap, tableTypes []uint8, fs fileservice.FileService, mp *mpool.MPool,
) error {
	for idx := range zms {
		zm, err := columnSummaryZoneMap(ctx, meta, location, uint16(cols[idx].Seqnum), fs, mp)
		if err != nil {
			return err
		}
		if zms[idx] == nil {
			zms[idx] = zm
			tableTypes[idx] = uint8(cols[idx].Typ.Id)
		} else if zm.IsInited() {
			index.UpdateZM(zms[idx], zm.GetMaxBuf())
			index.UpdateZM(zms[idx], zm.GetMinBuf())
		}
	}
	return nil
}

// columnSummaryZoneMap 返回可精确合并的私有摘要，不能将裁剪用的保守 BOOL
// 上界作为列最大值。历史 false/false 与真正全 false 无法从元数据区分。
func columnSummaryZoneMap(
	ctx context.Context,
	meta objectio.ObjectDataMeta,
	location objectio.Location,
	seqnum uint16,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (objectio.ZoneMap, error) {
	zm := meta.MustGetColumn(seqnum).ZoneMap()
	if !zm.IsInited() || zm.GetType() != types.T_bool || types.DecodeBool(zm.GetMinBuf()) {
		return zm.Clone(), nil
	}
	exact := index.NewZM(types.T_bool, 0)
	cache := containers.NewVectors(1)
	for i := uint32(0); i < meta.BlockCount(); i++ {
		block := meta.GetBlockMeta(i)
		blockLocation := objectio.BuildLocation(location.Name(), location.Extent(), block.GetRows(), block.GetID())
		if err := func() error {
			release, _, err := ioutil.LoadColumns(ctx, []uint16{seqnum}, []types.Type{types.T_bool.ToType()}, fs, blockLocation, cache, mp, fileservice.Policy(0))
			if err != nil {
				return err
			}
			defer release()
			return index.BatchUpdateZM(exact, &cache[0])
		}(); err != nil {
			return nil, err
		}
		// 两个可能的非 NULL 值都已观察到，后续块不能扩大 BOOL 的精确范围。
		if exact.IsInited() && !types.DecodeBool(exact.GetMinBuf()) && types.DecodeBool(exact.GetMaxBuf()) {
			break
		}
	}
	return exact, nil
}
