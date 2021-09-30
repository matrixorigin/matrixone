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

package metadata

import (
	"bytes"
	"fmt"
	"matrixone/pkg/encoding"
)

func MockLogBatchId(id uint64) LogBatchId {
	return LogBatchId{
		Id:     id,
		Offset: uint32(0),
		Size:   uint32(1),
	}
}

func (id *LogBatchId) String() string {
	return fmt.Sprintf("(%d,%d,%d)", id.Id, id.Offset, id.Size)
}

func (id *LogBatchId) IsEnd() bool {
	return id.Offset == id.Size-1
}

func (idx *LogIndex) IsSameBatch(o *LogIndex) bool {
	return idx.ID.Id == o.ID.Id
}

func (idx *LogIndex) String() string {
	return fmt.Sprintf("(%s,%d,%d,%d)", idx.ID.String(), idx.Start, idx.Count, idx.Capacity)
}

func (idx *LogIndex) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}

func (idx *LogIndex) IsBatchApplied() bool {
	return idx.Capacity == idx.Start+idx.Count && idx.ID.IsEnd()
}

func (idx *LogIndex) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(idx.ID.Id))
	buf.Write(encoding.EncodeUint32(uint32(idx.ID.Offset)))
	buf.Write(encoding.EncodeUint32(uint32(idx.ID.Size)))
	buf.Write(encoding.EncodeUint64(idx.Count))
	buf.Write(encoding.EncodeUint64(idx.Start))
	buf.Write(encoding.EncodeUint64(idx.Capacity))
	return buf.Bytes(), nil
}

func (idx *LogIndex) UnMarshall(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	idx.ID.Id = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.ID.Offset = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	idx.ID.Size = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	idx.Count = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Start = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Capacity = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	return nil
}
