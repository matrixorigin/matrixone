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

package shard

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/encoding"
)

var (
	ParseReprErr = errors.New("parse repr error")
)

type IndexId struct {
	Id     uint64 `json:"id"`
	Offset uint32 `json:"offset"`
	Size   uint32 `json:"size"`
}

type Index struct {
	ShardId  uint64  `json:"sid"`
	Id       IndexId `json:"id"`
	Start    uint64  `json:"start"`
	Count    uint64  `json:"count"`
	Capacity uint64  `json:"cap"`
}

func SimpleIndexId(id uint64) IndexId {
	return IndexId{
		Id:   id,
		Size: 1,
	}
}

func CreateIndexId(id uint64, offset, size uint32) IndexId {
	if offset >= size {
		panic(fmt.Sprintf("bad parameters: offset %d, size %d", offset, size))
	}
	return IndexId{
		Id:     id,
		Offset: offset,
		Size:   size,
	}
}

func (id *IndexId) Compare(o *IndexId) int {
	if id.Id > o.Id {
		return 1
	} else if id.Id < o.Id {
		return -1
	}
	if id.Offset > o.Offset {
		return 1
	} else if id.Offset < o.Offset {
		return -1
	}
	return 0
}

func (id *IndexId) String() string {
	if id.Size == uint32(1) {
		return fmt.Sprintf("%d", id.Id)
	}
	return fmt.Sprintf("%d[%d/%d]", id.Id, id.Offset, id.Size)
}

func (id *IndexId) Valid() bool {
	return id.Size > id.Offset
}

func (id *IndexId) IsEnd() bool {
	return id.Offset == id.Size-1
}

func (id *IndexId) IsSingle() bool {
	return id.Size == 1
}

func (idx *Index) Repr() string {
	if idx == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%d:%d:%d:%d", idx.ShardId, idx.Id.Id, idx.Id.Offset, idx.Id.Size, idx.Start, idx.Count, idx.Capacity)
}

func (idx *Index) ParseRepr(repr string) (err error) {
	if repr == "" {
		err = ParseReprErr
		return
	}
	strs := strings.Split(repr, ":")
	if len(strs) != 7 {
		err = ParseReprErr
		return
	}
	if idx.ShardId, err = strconv.ParseUint(strs[0], 10, 64); err != nil {
		return
	}
	if idx.Id.Id, err = strconv.ParseUint(strs[1], 10, 64); err != nil {
		return
	}
	var tmp uint64
	if tmp, err = strconv.ParseUint(strs[2], 10, 32); err != nil {
		return
	} else {
		idx.Id.Offset = uint32(tmp)
	}
	if tmp, err = strconv.ParseUint(strs[3], 10, 32); err != nil {
		return
	} else {
		idx.Id.Size = uint32(tmp)
	}
	if idx.Start, err = strconv.ParseUint(strs[4], 10, 64); err != nil {
		return
	}
	if idx.Count, err = strconv.ParseUint(strs[5], 10, 64); err != nil {
		return
	}
	if idx.Capacity, err = strconv.ParseUint(strs[6], 10, 64); err != nil {
		return
	}
	return
}

func (idx *Index) ToBatchIndex() *BatchIndex {
	return NewBatchIndex(idx)
}

func (idx *Index) CompareID(o *Index) int {
	if idx.ShardId != o.ShardId {
		panic("cannot compare index with diff shard id")
	}
	return idx.Id.Compare(&o.Id)
}

func (idx *Index) Compare(o *Index) int {
	if idx.ShardId != o.ShardId {
		panic("cannot compare index with diff shard id")
	}

	ret := idx.Id.Compare(&o.Id)
	if ret != 0 {
		return ret
	}

	if idx.Start == o.Start {
		ret = 0
	} else if idx.Start > o.Start {
		ret = 1
	} else {
		ret = -1
	}
	return ret
}

func (idx *Index) IsSameBatch(o *Index) bool {
	return idx.Id.Id == o.Id.Id
}

func (idx *Index) String() string {
	if idx == nil {
		return "null"
	}
	if idx.Capacity == 0 {
		return fmt.Sprintf("S-%d:<%s>", idx.ShardId, idx.Id.String())
	}
	return fmt.Sprintf("S-%d:%s:<%d+%d/%d>", idx.ShardId, idx.Id.String(), idx.Start, idx.Count, idx.Capacity)
}

func (idx *Index) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}

func (idx *Index) IsBatchApplied() bool {
	return idx.Capacity == idx.Start+idx.Count && idx.Id.IsEnd()
}

func (idx *Index) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(idx.ShardId))
	buf.Write(encoding.EncodeUint64(idx.Id.Id))
	buf.Write(encoding.EncodeUint32(uint32(idx.Id.Offset)))
	buf.Write(encoding.EncodeUint32(uint32(idx.Id.Size)))
	buf.Write(encoding.EncodeUint64(idx.Count))
	buf.Write(encoding.EncodeUint64(idx.Start))
	buf.Write(encoding.EncodeUint64(idx.Capacity))
	return buf.Bytes(), nil
}

func (idx *Index) UnMarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	idx.ShardId = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
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
