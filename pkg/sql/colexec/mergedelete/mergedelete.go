// Copyright 2022 Matrix Origin
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
package mergedelete

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" MergeS3DeleteInfo ")
}

func Prepare(proc *process.Process, arg any) error {
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var err error
	var name string
	ap := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		// ToDo:
		// start to do compaction for cn blocks
		// there are three strageties:
		// 1.do compaction at deletion operator
		// 2.do compaction here
		// 3.do compaction when read
		// choose which one depends on next pr
		ap.DelSource.Delete(proc.Ctx, nil, catalog.BlockMeta_Delete_ID)
		return true, nil
	}

	if len(bat.Zs) == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	// 		blkId          		deltaLoc                        type
	// |-----------|-----------------------------------|----------------|
	// |  blk_id   |   batch.Marshal(metaLoc)          |  FlushMetaLoc  | DN Block
	// |  blk_id   |   batch.Marshal(int64 offset)     |  CNBlockOffset | CN Block
	// |  blk_id   |   batch.Marshal(rowId)            |  RawRowIdBatch | DN Blcok
	// |  blk_id   |   batch.Marshal(int64 offset)     | RawBatchOffset | RawBatch (in txn workspace)
	blkIds := vector.MustStrCol(bat.GetVector(0))
	metaLocBats := vector.MustBytesCol(bat.GetVector(1))
	typs := vector.MustFixedCol[int8](bat.GetVector(2))
	for i := 0; i < bat.Length(); i++ {
		name = fmt.Sprintf("%s|%d", blkIds[i], typs[i])
		bat := &batch.Batch{}
		if err := bat.UnmarshalBinary([]byte(metaLocBats[i])); err != nil {
			return false, err
		}
		err = ap.DelSource.Delete(proc.Ctx, bat, name)
		if err != nil {
			return false, err
		}
	}
	// and there are another attr used to record how many rows are deleted
	ap.AffectedRows += uint64(vector.GetFixedAt[uint32](bat.GetVector(3), 0))
	return false, nil
}
