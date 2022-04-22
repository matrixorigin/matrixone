package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type staticFilterIndexNode struct {
	*buffer.Node
	mgr   base.INodeManager
	host  dataio.IndexFile
	meta  *common.IndexMeta
	inner basic.StaticFilter
}

func newStaticFilterIndexNode(mgr base.INodeManager, host dataio.IndexFile, meta *common.IndexMeta) *staticFilterIndexNode {
	impl := new(staticFilterIndexNode)
	impl.Node = buffer.NewNode(impl, mgr, host.AllocIndexNodeId(), uint64(meta.Size))
	impl.LoadFunc = impl.OnLoad
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.host = host
	impl.meta = meta
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
	startOffset := n.meta.StartOffset
	size := n.meta.Size
	compressTyp := n.meta.CompType
	data := n.host.Read(startOffset, size)
	rawSize := n.meta.RawSize
	buf := make([]byte, rawSize)
	if err = common.Decompress(data, buf, compressTyp); err != nil {
		panic(err)
	}
	n.inner, err = basic.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
	return
}

func (n *staticFilterIndexNode) OnUnload() {
	if n.inner == nil {
		// no-op
		return
	}
	n.inner = nil
}

func (n *staticFilterIndexNode) OnDestroy() {
	// no-op
}

func (n *staticFilterIndexNode) Close() error {
	// no-op
	return nil
}

type StaticFilterIndexReader struct {
	inode *staticFilterIndexNode
}

func NewStaticFilterIndexReader() *StaticFilterIndexReader {
	return &StaticFilterIndexReader{}
}

func (reader *StaticFilterIndexReader) Init(mgr base.INodeManager, host dataio.IndexFile, meta *common.IndexMeta) error {
	reader.inode = newStaticFilterIndexNode(mgr, host, meta)
	return nil
}

func (reader *StaticFilterIndexReader) MayContainsKey(key interface{}) (bool, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsKey(key)
}

func (reader *StaticFilterIndexReader) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (*roaring.Bitmap, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsAnyKeys(keys, visibility)
}

type StaticFilterIndexWriter struct {
	cType  common.CompressType
	host   dataio.IndexFile
	inner  basic.StaticFilter
	data   *vector.Vector
	colIdx uint16
}

func NewStaticFilterIndexWriter() *StaticFilterIndexWriter {
	return &StaticFilterIndexWriter{}
}

func (writer *StaticFilterIndexWriter) Init(host dataio.IndexFile, cType common.CompressType, colIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	return nil
}

func (writer *StaticFilterIndexWriter) Finalize() (*common.IndexMeta, error) {
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
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.StaticFilterIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)

	var startOffset uint32
	iBuf, err := writer.inner.Marshal()
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := common.Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	startOffset, err = appender.Append(compressed)
	if err != nil {
		return nil, err
	}
	meta.SetStartOffset(startOffset)
	writer.inner = nil
	return meta, nil
}

func (writer *StaticFilterIndexWriter) AddValues(values *vector.Vector) error {
	if writer.data == nil {
		writer.data = values
		return nil
	}
	if writer.data.Typ != values.Typ {
		return errors.ErrTypeMismatch
	}
	if err := vector.Append(writer.data, values.Col); err != nil {
		return err
	}
	return nil
}

// Query is only used for testing or debugging
func (writer *StaticFilterIndexWriter) Query(key interface{}) (bool, error) {
	return writer.inner.MayContainsKey(key)
}
