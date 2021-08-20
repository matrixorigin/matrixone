package db

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

type Block struct {
	Host  *Segment
	Id    uint64
	StrId string
}

func (blk *Block) Rows() int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.GetRowCount())
}

func (blk *Block) Size(attr string) int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.Size(attr))
}

func (blk *Block) ID() string {
	return blk.StrId
}

func (blk *Block) Prefetch(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	if data == nil {
		return nil, errors.New(fmt.Sprintf("Specified blk %d not found", blk.Id))
	}
	defer data.Unref()
	bat.Is = make([]batch.Info, len(attrs))
	for i, attr := range attrs {
		if err := data.Prefetch(attr); err != nil {
			return nil, err
		}
		bat.Is[i].R = blk
		bat.Is[i].Ref = cs[i]
		bat.Is[i].Len = blk.Size(attr)
	}
	return bat, nil
}

func (blk *Block) Read(size int64, ref uint64, attr string, proc *process.Process) (*vector.Vector, error) {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	if data != nil {
		defer data.Unref()
		vec, err := data.GetVectorCopy(attr, ref, proc)
		return vec, err
	}
	return nil, errors.New(fmt.Sprintf("Specified blk %d not found", blk.Id))
}
