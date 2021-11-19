package shard

import (
	"fmt"
)

type BatchInfo struct {
	Offset uint32
	Size   uint32
}

func (info *BatchInfo) Repr() string {
	return fmt.Sprintf("[%d/%d]", info.Offset, info.Size)
}

type BatchIndex struct {
	*Index
	Info *BatchInfo
}

func NewBatchIndex(index *Index) *BatchIndex {
	return &BatchIndex{
		Index: index,
	}
}

func (idx *BatchIndex) Clone() *BatchIndex {
	index := *idx.Index
	clone := &BatchIndex{
		Index: &index,
	}
	if idx.Info != nil {
		info := *idx.Info
		clone.Info = &info
	}
	return clone
}

func (idx *BatchIndex) Valid() bool {
	if idx.Info == nil {
		return idx.Index.Id.Valid()
	}
	v := idx.Index.Id.Valid()
	if !v {
		return false
	}
	return idx.Info.Offset < idx.Info.Size
}

func (idx *BatchIndex) IsSlice() bool {
	if idx.Info == nil {
		return false
	}
	return idx.Info.Size > 1
}

func (idx *BatchIndex) SliceSize() uint32 {
	if idx.Info == nil {
		return 1
	}
	return idx.Info.Size
}

func (idx *BatchIndex) String() string {
	if idx.Info == nil {
		return idx.Index.String()
	}
	return fmt.Sprintf("%s-%s", idx.Index.String(), idx.Info.Repr())
}

type BatchIndice struct {
	shardId uint64
	indice  []*BatchIndex
}

func NewBatchIndice(shardId uint64) *BatchIndice {
	return &BatchIndice{
		shardId: shardId,
		indice:  make([]*BatchIndex, 0, 10),
	}
}

func NewSimpleBatchIndice(index *Index) *BatchIndice {
	return &BatchIndice{
		shardId: index.ShardId,
		indice:  []*BatchIndex{&BatchIndex{Index: index}},
	}
}

func (bi *BatchIndice) GetShardId() uint64 {
	return bi.shardId
}

func (bi *BatchIndice) AppendIndex(index *Index) {
	bi.indice = append(bi.indice, &BatchIndex{Index: index})
}

func (bi *BatchIndice) Append(index *BatchIndex) {
	bi.indice = append(bi.indice, index)
}

func (bi *BatchIndice) String() string {
	if bi == nil {
		return "nil"
	}
	str := fmt.Sprintf("<BatchIndice>(ShardId-%d,Cnt-%d){", bi.shardId, len(bi.indice))
	for _, index := range bi.indice {
		str = fmt.Sprintf("%s\n%s", str, index.String())
	}
	if len(bi.indice) > 0 {
		str = fmt.Sprintf("%s\n}", str)
	} else {
		str = fmt.Sprintf("%s}", str)
	}
	return str
}
