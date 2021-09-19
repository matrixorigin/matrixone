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
