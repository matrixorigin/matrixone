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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Writer struct {
	writer objectio.Writer
	fs     *ObjectFS
}

func NewWriter(fs *ObjectFS) *Writer {
	return &Writer{
		fs: fs,
	}
}

func VectorsToMO(vec containers.Vector) *vector.Vector {
	mov := vector.NewOriginal(vec.GetType())
	data := vec.Data()
	typ := vec.GetType()
	mov.Typ = typ
	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
		//mov.Nsp.Np = vec.NullMask()
	}

	if vec.GetType().IsVarlen() {
		bs := vec.Bytes()
		nbs := len(bs.Offset)
		bsv := make([][]byte, nbs)
		for i := 0; i < nbs; i++ {
			bsv[i] = bs.Data[bs.Offset[i] : bs.Offset[i]+bs.Length[i]]
		}
		vector.AppendBytes(mov, bsv, nil)
	} else if vec.GetType().IsTuple() {
		cnt := types.DecodeInt32(data)
		if cnt != 0 {
			if err := types.Decode(data, &mov.Col); err != nil {
				panic(any(err))
			}
		}
	} else {
		vector.AppendFixedRaw(mov, data)
	}

	return mov
}
func CopyToMoVector(vec containers.Vector) *vector.Vector {
	return VectorsToMO(vec)
}
func CopyToMoVectors(vecs []containers.Vector) []*vector.Vector {
	movecs := make([]*vector.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}

func (w *Writer) WriteBlock(
	id *common.ID,
	columns *containers.Batch) (block objectio.BlockObject, err error) {
	name := EncodeBlkName(id)
	writer, err := objectio.NewObjectWriter(name, w.fs.service)
	if err != nil {
		return
	}
	w.writer = writer
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = CopyToMoVectors(columns.Vecs)
	block, err = w.writer.Write(bat)
	if err != nil {
		return
	}
	_, err = w.writer.WriteEnd()
	return
}

func (w *Writer) WriteIndex(
	block objectio.BlockObject,
	index objectio.IndexData) (err error) {
	w.writer.WriteIndex(block, index)
	return
}
