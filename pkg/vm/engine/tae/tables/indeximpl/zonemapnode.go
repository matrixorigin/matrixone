package indeximpl

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type blockZoneMapIndexNode struct {
	*buffer.Node
	mgr     base.INodeManager
	host    common.IVFile
	zonemap *index.ZoneMap
}

func newBlockZoneMapIndexNode(mgr base.INodeManager, host common.IVFile, id *common.ID) *blockZoneMapIndexNode {
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
	if n.zonemap != nil {
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
	if err = Decompress(data, buf, CompressType(compressTyp)); err != nil {
		panic(err)
	}
	n.zonemap, err = index.LoadZoneMapFrom(buf)
	if err != nil {
		panic(err)
	}
}

func (n *blockZoneMapIndexNode) OnUnload() {
	if n.zonemap == nil {
		// no-op
		return
	}
	n.zonemap = nil
}

func (n *blockZoneMapIndexNode) OnDestroy() {
	n.host.Unref()
}

func (n *blockZoneMapIndexNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.zonemap = nil
	return nil
}

type BlockZoneMapIndexReader struct {
	node *blockZoneMapIndexNode
}

func NewBlockZoneMapIndexReader() *BlockZoneMapIndexReader {
	return &BlockZoneMapIndexReader{}
}

func (reader *BlockZoneMapIndexReader) Init(mgr base.INodeManager, host common.IVFile, id *common.ID) error {
	reader.node = newBlockZoneMapIndexNode(mgr, host, id)
	return nil
}

func (reader *BlockZoneMapIndexReader) Destroy() (err error) {
	if err = reader.node.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *BlockZoneMapIndexReader) ContainsAny(keys *vector.Vector) (visibility *roaring.Bitmap, ok bool) {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.ContainsAny(keys)
}

func (reader *BlockZoneMapIndexReader) Contains(key any) bool {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.Contains(key)
}

type BlockZoneMapIndexWriter struct {
	cType       CompressType
	host        common.IRWFile
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewBlockZoneMapIndexWriter() *BlockZoneMapIndexWriter {
	return &BlockZoneMapIndexWriter{}
}

func (writer *BlockZoneMapIndexWriter) Init(host common.IRWFile, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *BlockZoneMapIndexWriter) Finalize() (*IndexMeta, error) {
	if writer.zonemap == nil {
		panic("unexpected error")
	}
	appender := writer.host
	meta := NewEmptyIndexMeta()
	meta.SetIndexType(BlockZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.zonemap.Marshal()
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	_, err = appender.Write(compressed)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	return meta, nil
}

func (writer *BlockZoneMapIndexWriter) AddValues(values *vector.Vector) (err error) {
	typ := values.Typ
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = data.ErrWrongType
			return
		}
	}
	err = writer.zonemap.BatchUpdate(values, 0, -1)
	return
}

func (writer *BlockZoneMapIndexWriter) SetMinMax(min, max any, typ types.Type) (err error) {
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = data.ErrWrongType
			return
		}
	}
	writer.zonemap.SetMin(min)
	writer.zonemap.SetMax(max)
	return
}
