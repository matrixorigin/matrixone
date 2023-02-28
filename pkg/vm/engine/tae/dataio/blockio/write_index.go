package blockio

import (
	"fmt"
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
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter() *ZMWriter {
	return &ZMWriter{}
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
	if writer.zonemap == nil {
		panic(any("unexpected error"))
	}
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
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = moerr.NewInternalErrorNoCtx("wrong type")
			return
		}
	}
	ctx := new(index.KeysCtx)
	ctx.Keys = values
	ctx.Count = values.Length()
	err = writer.zonemap.BatchUpdate(ctx)
	return
}

func (writer *ZMWriter) SetMinMax(min, max any, typ types.Type) (err error) {
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = moerr.NewInternalErrorNoCtx("wrong type")
			return
		}
	}
	writer.zonemap.SetMin(min)
	writer.zonemap.SetMax(max)
	return
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

// Query is only used for testing or debugging
func (writer *BFWriter) Query(key any) (bool, error) {
	return writer.impl.MayContainsKey(key)
}
