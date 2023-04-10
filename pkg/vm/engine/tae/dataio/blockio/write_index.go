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

package blockio

import (
	"fmt"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ZMWriter struct {
	cType       common.CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	zonemap     index.ZM
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter(t types.T) *ZMWriter {
	return &ZMWriter{
		zonemap: *index.NewZM(t),
	}
}

func (writer *ZMWriter) String() string {
	return fmt.Sprintf("ZmWriter[Cid-%d,%s]", writer.colIdx, writer.zonemap.String())
}

func (writer *ZMWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *ZMWriter) Finalize() error {
	appender := writer.writer

	//var startOffset uint32
	iBuf, err := writer.zonemap.Marshal()
	if err != nil {
		return err
	}
	zonemap, err := objectio.NewZoneMap(writer.colIdx, iBuf)
	if err != nil {
		return err
	}
	err = appender.WriteIndex(writer.block, zonemap)
	if err != nil {
		return err
	}
	//meta.SetStartOffset(startOffset)
	return nil
}

func (writer *ZMWriter) AddValues(values containers.Vector) (err error) {
	typ := values.GetType()
	if writer.zonemap.GetType() != typ.Oid {
		err = moerr.NewInternalErrorNoCtx("wrong type")
		return
	}
	return index.BatchUpdateZM(&writer.zonemap, values)
}

type BFWriter struct {
	cType       common.CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	impl        index.StaticFilter
	data        containers.Vector
	colIdx      uint16
	internalIdx uint16
}

func NewBFWriter() *BFWriter {
	return &BFWriter{}
}

func (writer *BFWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *BFWriter) Finalize() error {
	if writer.impl != nil {
		panic(any("formerly finalized filter not cleared yet"))
	}
	sf, err := index.NewBinaryFuseFilter(writer.data)
	if err != nil {
		return err
	}
	writer.impl = sf
	writer.data = nil

	appender := writer.writer

	//var startOffset uint32
	iBuf, err := writer.impl.Marshal()
	if err != nil {
		return err
	}
	bf := objectio.NewBloomFilter(writer.colIdx, uint8(writer.cType), iBuf)

	err = appender.WriteIndex(writer.block, bf)
	if err != nil {
		return err
	}
	//meta.SetStartOffset(startOffset)
	writer.impl = nil
	return nil
}

func (writer *BFWriter) AddValues(values containers.Vector) error {
	if writer.data == nil {
		writer.data = values
		return nil
	}
	if writer.data.GetType() != values.GetType() {
		return moerr.NewInternalErrorNoCtx("wrong type")
	}
	writer.data.Extend(values)
	return nil
}

type ObjectColumnMetasBuilder struct {
	totalRow uint32
	metas    []objectio.ObjectColumnMeta
	sks      []*hll.Sketch
	zms      []*index.ZM
}

func NewObjectColumnMetasBuilder(colIdx int) *ObjectColumnMetasBuilder {
	return &ObjectColumnMetasBuilder{
		metas: make([]objectio.ObjectColumnMeta, colIdx),
		sks:   make([]*hll.Sketch, colIdx),
		zms:   make([]*index.ZM, colIdx),
	}
}

func (b *ObjectColumnMetasBuilder) AddRowCnt(rows int) {
	b.totalRow += uint32(rows)
}

func (b *ObjectColumnMetasBuilder) InspectVector(idx int, vec containers.Vector) {
	if vec.HasNull() {
		b.metas[idx].NullCnt += uint32(vec.NullMask().GetCardinality())
	}

	if b.zms[idx] == nil {
		b.zms[idx] = index.NewZM(vec.GetType().Oid)
	}
	if b.sks[idx] == nil {
		b.sks[idx] = hll.New()
	}
	containers.ForeachWindowBytes(vec, 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
		if isNull {
			return
		}
		b.sks[idx].Insert(v)
		return
	})
}

func (b *ObjectColumnMetasBuilder) UpdateZm(idx int, zm *index.ZM) {
	// When UpdateZm is called, it is all in memroy, GetMin and GetMax has no loss
	// min and max can be nil if the input vector is null vector
	if !zm.IsInited() {
		return
	}
	index.UpdateZM(b.zms[idx], zm.GetMinBuf())
	index.UpdateZM(b.zms[idx], zm.GetMaxBuf())
}

func (b *ObjectColumnMetasBuilder) Build() (uint32, []objectio.ObjectColumnMeta) {
	for i := range b.metas {
		if b.sks[i] != nil { // rowid or types.TS
			b.metas[i].Ndv = uint32(b.sks[i].Estimate())
		}
		if b.zms[i] != nil {
			zmbuf, _ := b.zms[i].Marshal()
			objzm, _ := objectio.NewZoneMap(uint16(i), zmbuf)
			b.metas[i].Zonemap = *objzm.(*objectio.ZoneMap)
		}
	}
	ret := b.metas
	b.metas = nil
	b.sks = nil
	b.zms = nil
	return b.totalRow, ret
}
