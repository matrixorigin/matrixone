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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// maxAndMinColumnZoneMap 返回可用于精确值且可由调用者合并的私有摘要。
// BOOL 元数据的兼容上界不精确，原始上界也可能来自旧 writer 的错误摘要；
// 因此仅在此精确值消费者中读取物理列，不改变过滤消费者的保守视图。
func maxAndMinColumnZoneMap(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	meta objectio.ObjectDataMeta,
	seqnum uint16,
	mp *mpool.MPool,
) (objectio.ZoneMap, error) {
	zm := meta.MustGetColumn(seqnum).ZoneMap()
	if zm.GetType() != types.T_bool {
		return zm.Clone(), nil
	}
	reader, err := ioutil.NewObjectReader(fs, location)
	if err != nil {
		return nil, err
	}
	exact := index.NewZM(types.T_bool, 0)
	for block := uint32(0); block < meta.BlockCount(); block++ {
		if err := func() error {
			bat, release, err := reader.LoadColumns(ctx, []uint16{seqnum}, []types.Type{types.T_bool.ToType()}, uint16(block), mp)
			if err != nil {
				return err
			}
			defer release()
			return index.BatchUpdateZM(exact, bat.Vecs[0])
		}(); err != nil {
			return nil, err
		}
	}
	return exact, nil
}
