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

package tempengine

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func (reader *TempReader) Close() error {
	return nil
}
func (reader *TempReader) Read(attrs []string, _ *plan.Expr, m *mheap.Mheap) (*batch.Batch, error) {
	if reader.ownRelation.blockNums == 0 || reader.waterMark >= uint64(len(reader.blockIdxs)) {
		return nil, nil
	}
	bat := batch.New(true, attrs)
	blockId := reader.blockIdxs[reader.waterMark]
	reader.waterMark++
	tblName := reader.ownRelation.tblSchema.tblName
	for i, attrName := range attrs {
		attr := reader.ownRelation.tblSchema.NameToAttr[attrName]
		bat.Vecs[i] = vector.New(attr.Type)
		copyData := copyData(reader.ownRelation.bytesData[tblName+"-"+fmt.Sprintf("%d", blockId)+"-"+attrName], reader.decodeBufs[i])
		if attr.Alg == compress.None {
			if err := bat.Vecs[i].Read(copyData); err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
		} else {
			n := int(types.DecodeInt32(copyData[len(copyData)-4:]))
			data := copyData[:len(copyData)-4]
			buf := reader.helperBufs[i]
			if buf.Cap() < n {
				buf.Grow(n)
			}
			_, err := compress.Decompress(data, buf.Bytes()[:n], int(attr.Alg))
			if err != nil {
				return nil, err
			}
			data = buf.Bytes()[:n]
			err = bat.Vecs[i].Read(data)
			if err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
		}
	}
	n := vector.Length(bat.Vecs[0])
	sels := m.GetSels()
	if n > cap(sels) {
		m.PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for i := 0; i < n; i++ {
		bat.Zs[i] = 1
	}
	return bat, nil
}
