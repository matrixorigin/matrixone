package engine

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
	return 0
}

func (blk *Block) Size(attr string) int64 {
	return 0
}

func (blk *Block) ID() string {
	return blk.StrId
}

func (blk *Block) Prefetch(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	bat.Is = make([]batch.Info, len(attrs))
	for i, _ := range attrs {
		bat.Is[i].R = blk
		bat.Is[i].Ref = cs[i]
	}
	return bat, nil
}

func (blk *Block) Read(size int64, ref uint64, attr string, proc *process.Process) (*vector.Vector, error) {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	if data != nil {
		defer data.Unref()
		if vec := data.GetVectorCopy(attr); vec != nil {
			return vec, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Specified blk %d not found", blk.Id))
}
