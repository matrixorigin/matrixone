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

package txnbase

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

const (
	IDSize = 8 + 8 + 8 + 4 + 2 + 1
)

func MarshalID(id *common.ID) []byte {
	var w bytes.Buffer
	binary.Write(&w, binary.BigEndian, id.TableID)
	binary.Write(&w, binary.BigEndian, id.SegmentID)
	binary.Write(&w, binary.BigEndian, id.BlockID)
	binary.Write(&w, binary.BigEndian, id.PartID)
	binary.Write(&w, binary.BigEndian, id.Idx)
	binary.Write(&w, binary.BigEndian, id.Iter)
	return w.Bytes()
}

func UnmarshalID(buf []byte) *common.ID {
	r := bytes.NewBuffer(buf)
	id := common.ID{}
	binary.Read(r, binary.BigEndian, &id.TableID)
	binary.Read(r, binary.BigEndian, &id.SegmentID)
	binary.Read(r, binary.BigEndian, &id.BlockID)
	binary.Read(r, binary.BigEndian, &id.PartID)
	binary.Read(r, binary.BigEndian, &id.Idx)
	binary.Read(r, binary.BigEndian, &id.Iter)
	return &id
}

func MarshalBatch(types []types.Type, data batch.IBatch) ([]byte, error) {
	var buf []byte
	if data == nil {
		return buf, nil
	}
	var bbuf bytes.Buffer
	vecs := make([]vector.IVectorNode, 0)
	for _, attr := range data.GetAttrs() {
		vec, err := data.GetVectorByAttr(attr)
		if err != nil {
			return buf, err
		}
		v := vec.(vector.IVectorNode)
		vecs = append(vecs, v)
	}
	binary.Write(&bbuf, binary.BigEndian, uint32(0))
	binary.Write(&bbuf, binary.BigEndian, uint16(len(vecs)))
	binary.Write(&bbuf, binary.BigEndian, uint32(data.Length()))
	bufs := make([][]byte, len(vecs))
	for i, vec := range vecs {
		vecBuf, _ := vec.Marshal()
		bufs[i] = vecBuf
		typeBuf := encoding.EncodeType(types[i])
		_, err := bbuf.Write(typeBuf)
		if err != nil {
			return nil, err
		}
		binary.Write(&bbuf, binary.BigEndian, uint32(len(vecBuf)))
	}
	for _, colBuf := range bufs {
		bbuf.Write(colBuf)
	}
	buf = bbuf.Bytes()
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf, nil
}

func UnmarshalBatch(buf []byte) (vecTypes []types.Type, bat batch.IBatch, n int64, err error) {
	r := bytes.NewBuffer(buf)
	return UnmarshalBatchFrom(r)
}

func UnmarshalBatchFrom(r io.Reader) (vecTypes []types.Type, bat batch.IBatch, n int64, err error) {
	var size uint32
	var vecs uint16
	pos := 0
	if binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}
	buf := make([]byte, size-4)
	if _, err = r.Read(buf); err != nil {
		return
	}
	bbuf := bytes.NewBuffer(buf)
	if binary.Read(bbuf, binary.BigEndian, &vecs); err != nil {
		return
	}
	var rows uint32
	if binary.Read(bbuf, binary.BigEndian, &rows); err != nil {
		return
	}
	pos += 2 + 4
	lens := make([]uint32, vecs)
	vecTypes = make([]types.Type, vecs)
	for i := uint16(0); i < vecs; i++ {
		colType := encoding.DecodeType(buf[pos : pos+encoding.TypeSize])
		vecTypes[i] = colType
		pos += encoding.TypeSize
		lens[i] = binary.BigEndian.Uint32(buf[pos:])
		pos += 4
	}

	attrs := make([]int, vecs)
	cols := make([]vector.IVector, vecs)
	for i := 0; i < int(vecs); i++ {
		col := vector.NewVector(vecTypes[i], uint64(rows))
		cols[i] = col
		attrs[i] = i
		if col.(vector.IVectorNode).Unmarshal(buf[pos : pos+int(lens[i])]); err != nil {
			return
		}
		pos += int(lens[i])
	}

	bat, err = batch.NewBatch(attrs, cols)
	n = int64(size)
	return
}
