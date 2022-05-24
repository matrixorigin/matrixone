package index

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
)

type staticFilterIndexNode struct {
	*buffer.Node
	mgr   base.INodeManager
	host  common.IVFile
	inner basic.StaticFilter
}

func newStaticFilterIndexNode(mgr base.INodeManager, host common.IVFile, id *common.ID) *staticFilterIndexNode {
	impl := new(staticFilterIndexNode)
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

func (n *staticFilterIndexNode) OnLoad() {
	if n.inner != nil {
		// no-op
		return
	}
	var err error
	//startOffset := n.meta.StartOffset
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
	}
	n.inner, err = basic.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
}

func (n *staticFilterIndexNode) OnUnload() {
	if n.inner == nil {
		// no-op
		return
	}
	n.inner = nil
}

func (n *staticFilterIndexNode) OnDestroy() {
	n.host.Unref()
}

func (n *staticFilterIndexNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.inner = nil
	return nil
}

type StaticFilterIndexReader struct {
	inode *staticFilterIndexNode
}

func NewStaticFilterIndexReader() *StaticFilterIndexReader {
	return &StaticFilterIndexReader{}
}

func (reader *StaticFilterIndexReader) Init(mgr base.INodeManager, host common.IVFile, id *common.ID) error {
	reader.inode = newStaticFilterIndexNode(mgr, host, id)
	return nil
}

func (reader *StaticFilterIndexReader) Destroy() (err error) {
	if err = reader.inode.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *StaticFilterIndexReader) MayContainsKey(key any) (bool, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsKey(key)
}

func (reader *StaticFilterIndexReader) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsAnyKeys(keys, visibility)
}

type StaticFilterIndexWriter struct {
	cType       CompressType
	host        common.IRWFile
	inner       basic.StaticFilter
	data        *vector.Vector
	colIdx      uint16
	internalIdx uint16
}

func NewStaticFilterIndexWriter() *StaticFilterIndexWriter {
	return &StaticFilterIndexWriter{}
}

func (writer *StaticFilterIndexWriter) Init(host common.IRWFile, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *StaticFilterIndexWriter) Finalize() (*IndexMeta, error) {
	if writer.inner != nil {
		panic("formerly finalized filter not cleared yet")
	}
	sf, err := basic.NewBinaryFuseFilter(writer.data)
	if err != nil {
		return nil, err
	}
	writer.inner = sf
	writer.data = nil

	appender := writer.host
	meta := NewEmptyIndexMeta()
	meta.SetIndexType(StaticFilterIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.inner.Marshal()
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
	writer.inner = nil
	return meta, nil
}

func (writer *StaticFilterIndexWriter) AddValues(values *vector.Vector) error {
	if writer.data == nil {
		writer.data = values
		return nil
	}
	if writer.data.Typ != values.Typ {
		return data.ErrWrongType
	}
	if err := vector.Append(writer.data, values.Col); err != nil {
		return err
	}
	return nil
}

// Query is only used for testing or debugging
func (writer *StaticFilterIndexWriter) Query(key any) (bool, error) {
	return writer.inner.MayContainsKey(key)
}
