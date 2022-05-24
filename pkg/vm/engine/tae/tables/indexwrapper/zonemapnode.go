package indexwrapper

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

type zonemapNode struct {
	*buffer.Node
	mgr     base.INodeManager
	host    common.IVFile
	zonemap *index.ZoneMap
}

func newZonemapNode(mgr base.INodeManager, host common.IVFile, id *common.ID) *zonemapNode {
	impl := new(zonemapNode)
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

func (n *zonemapNode) OnLoad() {
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

func (n *zonemapNode) OnUnload() {
	if n.zonemap == nil {
		// no-op
		return
	}
	n.zonemap = nil
}

func (n *zonemapNode) OnDestroy() {
	n.host.Unref()
}

func (n *zonemapNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.zonemap = nil
	return nil
}

type ZMReader struct {
	node *zonemapNode
}

func NewZMReader() *ZMReader {
	return &ZMReader{}
}

func (reader *ZMReader) Init(mgr base.INodeManager, host common.IVFile, id *common.ID) error {
	reader.node = newZonemapNode(mgr, host, id)
	return nil
}

func (reader *ZMReader) Destroy() (err error) {
	if err = reader.node.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *ZMReader) ContainsAny(keys *vector.Vector) (visibility *roaring.Bitmap, ok bool) {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.ContainsAny(keys)
}

func (reader *ZMReader) Contains(key any) bool {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.Contains(key)
}

type ZMWriter struct {
	cType       CompressType
	host        common.IRWFile
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter() *ZMWriter {
	return &ZMWriter{}
}

func (writer *ZMWriter) Init(host common.IRWFile, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *ZMWriter) Finalize() (*IndexMeta, error) {
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

func (writer *ZMWriter) AddValues(values *vector.Vector) (err error) {
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

func (writer *ZMWriter) SetMinMax(min, max any, typ types.Type) (err error) {
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
