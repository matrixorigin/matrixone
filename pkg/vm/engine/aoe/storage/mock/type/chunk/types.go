package chunk

import (
	vec "matrixone/pkg/vm/engine/aoe/storage/mock/type/vector"
	// log "github.com/sirupsen/logrus"
)

type IChunk interface {
	Append(IChunk, offset uint64) (n uint64, err error)
	GetVector(int) vec.Vector
	GetCount() uint64
}

type Chunk struct {
	Vectors []vec.Vector
}

func (c *Chunk) Append(o *Chunk, offset uint64) (n uint64, err error) {
	for i, vector := range c.Vectors {
		n, err = vector.Append(o.Vectors[i], offset)
		if err != nil {
			return n, err
		}
	}
	return n, err
}

func (c *Chunk) GetCount() uint64 {
	return c.Vectors[0].GetCount()
}
