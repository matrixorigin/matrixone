package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	gCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type blockZoneMapIndexNode struct {
	*buffer.Node
	mgr  base.INodeManager
	host gCommon.IVFile
	inner *basic.ZoneMap
}

func newBlockZoneMapIndexNode(mgr base.INodeManager, host gCommon.IVFile, id *gCommon.ID) *blockZoneMapIndexNode {
	impl := new(blockZoneMapIndexNode)
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(host.Stat().Size()))
	impl.LoadFunc = impl.OnLoad
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.host = host
	//impl.meta = meta
	impl.mgr = mgr
	mgr.RegisterNode(impl)
	return impl
}

func (n *blockZoneMapIndexNode) OnLoad() {
	if n.inner != nil {
		// no-op
		return
	}
	var err error
	stat := n.host.Stat()
	size := stat.Size()
	compressTyp := stat.CompressAlgo()
	data := make([]byte, size)
	if _, err := n.host.Read(data); err != nil {
		panic(err)
	}
	rawSize := stat.OriginSize()
	buf := make([]byte, rawSize)
	if err = common.Decompress(data, buf, common.CompressType(compressTyp)); err != nil {
		panic(err)
	}
	n.inner, err = basic.NewZoneMapFromSource(buf)
	if err != nil {
		panic(err)
	}
	return
}

func (n *blockZoneMapIndexNode) OnUnload() {
	if n.inner == nil {
		// no-op
		return
	}
	n.inner = nil
}

func (n *blockZoneMapIndexNode) OnDestroy() {
	// no-op
}

func (n *blockZoneMapIndexNode) Close() error {
	// no-op
	return nil
}

type BlockZoneMapIndexReader struct {
	inode *blockZoneMapIndexNode
}

func NewBlockZoneMapIndexReader() *BlockZoneMapIndexReader {
	return &BlockZoneMapIndexReader{}
}

func (reader *BlockZoneMapIndexReader) Init(mgr base.INodeManager, host gCommon.IVFile, id *gCommon.ID) error {
	reader.inode = newBlockZoneMapIndexNode(mgr, host, id)
	return nil
}

func (reader *BlockZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, *roaring.Bitmap, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*blockZoneMapIndexNode).inner.MayContainsAnyKeys(keys)
}

func (reader *BlockZoneMapIndexReader) MayContainsKey(key interface{}) (bool, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*blockZoneMapIndexNode).inner.MayContainsKey(key)
}

type BlockZoneMapIndexWriter struct {
	cType       common.CompressType
	host        gCommon.IRWFile
	inner       *basic.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewBlockZoneMapIndexWriter() *BlockZoneMapIndexWriter {
	return &BlockZoneMapIndexWriter{}
}

func (writer *BlockZoneMapIndexWriter) Init(host gCommon.IRWFile, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *BlockZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.inner == nil {
		panic("unexpected error")
	}
	appender := writer.host
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.BlockZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.inner.Marshal()
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := common.Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	_, err = appender.Write(compressed)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	return meta, nil
}

func (writer *BlockZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	typ := values.Typ
	if writer.inner == nil {
		writer.inner = basic.NewZoneMap(typ, nil)
	} else {
		if writer.inner.GetType() != typ {
			return errors.ErrTypeMismatch
		}
	}
	if err := writer.inner.BatchUpdate(values, 0, -1); err != nil {
		return err
	}
	return nil
}

func (writer *BlockZoneMapIndexWriter) SetMinMax(min, max interface{}, typ types.Type) error {
	if writer.inner == nil {
		writer.inner = basic.NewZoneMap(typ, nil)
	} else {
		if writer.inner.GetType() != typ {
			return errors.ErrTypeMismatch
		}
	}
	writer.inner.SetMin(min)
	writer.inner.SetMax(max)
	return nil
}
