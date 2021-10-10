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

package metadata

import (
	"bytes"
	"fmt"
	"matrixone/pkg/encoding"
)

type LogIndex = ExternalIndex

type LogBatchId struct {
	Id     uint64
	Offset uint32
	Size   uint32
}

type ExternalIndex struct {
	Id       LogBatchId
	Start    uint64
	Count    uint64
	Capacity uint64
}

func SimpleBatchId(id uint64) LogBatchId {
	return LogBatchId{
		Id:   id,
		Size: 1,
	}
}

func CreateBatchId(id uint64, offset, size uint32) LogBatchId {
	if offset >= size {
		panic(fmt.Sprintf("bad parameters: offset %d, size %d", offset, size))
	}
	return LogBatchId{
		Id:     id,
		Offset: offset,
		Size:   size,
	}
}

func (id *LogBatchId) String() string {
	return fmt.Sprintf("(%d,%d,%d)", id.Id, id.Offset, id.Size)
}

func (id *LogBatchId) Valid() bool {
	return id.Size > id.Offset
}

func (id *LogBatchId) IsEnd() bool {
	return id.Offset == id.Size-1
}

func (idx *ExternalIndex) IsSameBatch(o *ExternalIndex) bool {
	return idx.Id.Id == o.Id.Id
}

func (idx *ExternalIndex) String() string {
	if idx == nil {
		return "null"
	}
	return fmt.Sprintf("(%s,%d,%d,%d)", idx.Id.String(), idx.Start, idx.Count, idx.Capacity)
}

func (idx *ExternalIndex) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}

func (idx *ExternalIndex) IsBatchApplied() bool {
	return idx.Capacity == idx.Start+idx.Count && idx.Id.IsEnd()
}

func (idx *ExternalIndex) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(idx.Id.Id))
	buf.Write(encoding.EncodeUint32(uint32(idx.Id.Offset)))
	buf.Write(encoding.EncodeUint32(uint32(idx.Id.Size)))
	buf.Write(encoding.EncodeUint64(idx.Count))
	buf.Write(encoding.EncodeUint64(idx.Start))
	buf.Write(encoding.EncodeUint64(idx.Capacity))
	return buf.Bytes(), nil
}

func (idx *ExternalIndex) UnMarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	idx.Id.Id = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Id.Offset = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	idx.Id.Size = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	idx.Count = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Start = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Capacity = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	return nil
}
