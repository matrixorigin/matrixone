package shard

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/encoding"
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
	} else {
		return -1
	}
	return 0
}

func (id *IndexId) String() string {
	return fmt.Sprintf("(%d,%d,%d)", id.Id, id.Offset, id.Size)
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
	return fmt.Sprintf("S%d(%s,%d,%d,%d)", idx.ShardId, idx.Id.String(), idx.Start, idx.Count, idx.Capacity)
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
