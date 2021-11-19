package shard

import (
	"fmt"
)

type SliceInfo struct {
	Offset uint32
	Size   uint32
}

func (info *SliceInfo) Repr() string {
	return fmt.Sprintf("[%d/%d]", info.Offset, info.Size)
}

type SliceIndex struct {
	*Index
	Info *SliceInfo
}

func NewSliceIndex(index *Index) *SliceIndex {
	return &SliceIndex{
		Index: index,
	}
}

func (idx *SliceIndex) Clone() *SliceIndex {
	index := *idx.Index
	clone := &SliceIndex{
		Index: &index,
	}
	if idx.Info != nil {
		info := *idx.Info
		clone.Info = &info
	}
	return clone
}

func (idx *SliceIndex) Valid() bool {
	if idx.Info == nil {
		return idx.Index.Id.Valid()
	}
	v := idx.Index.Id.Valid()
	if !v {
		return false
	}
	return idx.Info.Offset < idx.Info.Size
}

func (idx *SliceIndex) IsSlice() bool {
	if idx.Info == nil {
		return false
	}
	return idx.Info.Size > 1
}

func (idx *SliceIndex) SliceSize() uint32 {
	if idx.Info == nil {
		return 1
	}
	return idx.Info.Size
}

func (idx *SliceIndex) String() string {
	if idx.Info == nil {
		return idx.Index.String()
	}
	return fmt.Sprintf("%s-%s", idx.Index.String(), idx.Info.Repr())
}

type SliceIndice struct {
	shardId uint64
	indice  []*SliceIndex
}

func NewBatchIndice(shardId uint64) *SliceIndice {
	return &SliceIndice{
		shardId: shardId,
		indice:  make([]*SliceIndex, 0, 10),
	}
}

func NewSimpleBatchIndice(index *Index) *SliceIndice {
	return &SliceIndice{
		shardId: index.ShardId,
		indice:  []*SliceIndex{&SliceIndex{Index: index}},
	}
}

func (bi *SliceIndice) GetShardId() uint64 {
	return bi.shardId
}

func (bi *SliceIndice) AppendIndex(index *Index) {
	bi.indice = append(bi.indice, &SliceIndex{Index: index})
}

func (bi *SliceIndice) Append(index *SliceIndex) {
	bi.indice = append(bi.indice, index)
}

func (bi *SliceIndice) String() string {
	if bi == nil {
		return "nil"
	}
	str := fmt.Sprintf("<SliceIndice>(ShardId-%d,Cnt-%d){", bi.shardId, len(bi.indice))
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
