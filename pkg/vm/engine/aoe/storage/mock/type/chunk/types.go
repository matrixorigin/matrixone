package chunk

import (
	"matrixone/pkg/container/types"
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

func MockChunk(types []types.Type, rows uint64) *Chunk {
	var vectors []vec.Vector
	buf := make([]byte, int32(rows)*types[0].Size)
	for _, colType := range types {
		vector := vec.NewStdVector(colType, buf)
		vector.(*vec.StdVector).Offset = cap(buf)
		vectors = append(vectors, vector)
	}

	return &Chunk{
		Vectors: vectors,
	}
}
