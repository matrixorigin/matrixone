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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

const (
	IDSize = 8 + 8 + 8 + 4 + 2 + 1
)

func MarshalID(id *common.ID) []byte {
	var err error
	var w bytes.Buffer
	if err = binary.Write(&w, binary.BigEndian, id.TableID); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.SegmentID); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.BlockID); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.PartID); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.Idx); err != nil {
		panic(err)
	}
	if err = binary.Write(&w, binary.BigEndian, id.Iter); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func UnmarshalID(buf []byte) *common.ID {
	var err error
	r := bytes.NewBuffer(buf)
	id := common.ID{}
	if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.SegmentID); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.BlockID); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.PartID); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.Idx); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &id.Iter); err != nil {
		panic(err)
	}
	return &id
}

func MarshalBatch(colTypes []types.Type, data batch.IBatch) (buf []byte, err error) {
	if data == nil {
		return
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
	if err = binary.Write(&bbuf, binary.BigEndian, uint32(0)); err != nil {
		return
	}
	if err = binary.Write(&bbuf, binary.BigEndian, uint16(len(vecs))); err != nil {
		return
	}
	if err = binary.Write(&bbuf, binary.BigEndian, uint32(data.Length())); err != nil {
		return
	}
	bufs := make([][]byte, len(vecs))
	for i, vec := range vecs {
		vecBuf, _ := vec.Marshal()
		bufs[i] = vecBuf
		typeBuf := types.EncodeType(colTypes[i])
		_, err := bbuf.Write(typeBuf)
		if err != nil {
			return nil, err
		}
		if err = binary.Write(&bbuf, binary.BigEndian, uint32(len(vecBuf))); err != nil {
			return nil, err
		}
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
	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}
	buf := make([]byte, size-4)
	if _, err = r.Read(buf); err != nil {
		return
	}
	bbuf := bytes.NewBuffer(buf)
	if err = binary.Read(bbuf, binary.BigEndian, &vecs); err != nil {
		return
	}
	var rows uint32
	if err = binary.Read(bbuf, binary.BigEndian, &rows); err != nil {
		return
	}
	pos += 2 + 4
	lens := make([]uint32, vecs)
	vecTypes = make([]types.Type, vecs)
	for i := uint16(0); i < vecs; i++ {
		colType := types.DecodeType(buf[pos : pos+types.TypeSize])
		vecTypes[i] = colType
		pos += types.TypeSize
		lens[i] = binary.BigEndian.Uint32(buf[pos:])
		pos += 4
	}

	attrs := make([]int, vecs)
	cols := make([]vector.IVector, vecs)
	for i := 0; i < int(vecs); i++ {
		col := vector.NewVector(vecTypes[i], uint64(rows))
		cols[i] = col
		attrs[i] = i
		if err = col.(vector.IVectorNode).Unmarshal(buf[pos : pos+int(lens[i])]); err != nil {
			return
		}
		pos += int(lens[i])
	}

	bat, err = batch.NewBatch(attrs, cols)
	n = int64(size)
	return
}
