package chunk

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	// log "github.com/sirupsen/logrus"
)

type IChunk interface {
	Append(IChunk, offset uint64) (n uint64, err error)
	GetVector(int) vector.IVector
	GetCount() uint64
}

type Chunk struct {
	Vectors []vector.IVector
}

func (c *Chunk) Append(bat *batch.Batch, offset uint64) (n uint64, err error) {
	for i, vec := range c.Vectors {
		// log.Infof("i %d, offset %d, bat size %d, vec cap %d", i, offset, bat.Vecs[0].Length(), vec.Capacity())
		nr, err := vec.AppendVector(bat.Vecs[i], int(offset))
		if err != nil {
			return n, err
		}
		n = uint64(nr)
	}
	return n, err
}

func (c *Chunk) GetCount() uint64 {
	return uint64(c.Vectors[0].Length())
}

func MockChunk(types []types.Type, rows uint64) *Chunk {
	var vectors []vector.IVector
	for _, colType := range types {
		vectors = append(vectors, vector.MockVector(colType, rows))
	}

	return &Chunk{
		Vectors: vectors,
	}
}

func MockBatch(types []types.Type, rows uint64) *batch.Batch {
	var attrs []string
	for _, t := range types {
		attrs = append(attrs, t.Oid.String())
	}

	bat := batch.New(true, attrs)
	for i, colType := range types {
		vec := vector.MockVector(colType, rows)
		bat.Vecs[i] = vec.CopyToVector()
	}

	return bat
}
