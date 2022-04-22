package io

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	gCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type segmentZoneMapIndexNode struct {
	*buffer.Node
	mgr            base.INodeManager
	host           dataio.IndexFile
	meta           *common.IndexMeta
	segmentZoneMap *basic.ZoneMap
	blockZoneMaps  []*basic.ZoneMap
}

func newSegmentZoneMapIndexNode(mgr base.INodeManager, host dataio.IndexFile, meta *common.IndexMeta) *segmentZoneMapIndexNode {
	impl := new(segmentZoneMapIndexNode)
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

func (n *segmentZoneMapIndexNode) OnLoad() {
	if n.segmentZoneMap != nil {
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

	var blockZoneMap *basic.ZoneMap
	n.blockZoneMaps = make([]*basic.ZoneMap, 0)
	blockCount := encoding.DecodeUint32(data[:4])
	data = data[4:]
	for i := uint32(0); i < blockCount; i++ {
		bufLen := encoding.DecodeUint16(data[:2])
		data = data[2:]
		blockBuffer := data[:bufLen]
		data = data[bufLen:]
		blockZoneMap, err = basic.NewZoneMapFromSource(blockBuffer)
		if err != nil {
			panic(err)
		}
		n.blockZoneMaps = append(n.blockZoneMaps, blockZoneMap)
	}
	bufLen := encoding.DecodeUint16(data[:2])
	data = data[2:]
	segmentBuffer := data[:bufLen]
	data = data[bufLen:]
	n.segmentZoneMap, err = basic.NewZoneMapFromSource(segmentBuffer)
	if err != nil {
		panic(err)
	}
	return
}

func (n *segmentZoneMapIndexNode) OnUnload() {
	if n.segmentZoneMap == nil {
		// no-op
		return
	}
	n.segmentZoneMap = nil
	n.blockZoneMaps = nil
}

func (n *segmentZoneMapIndexNode) OnDestroy() {
	// no-op
}

func (n *segmentZoneMapIndexNode) Close() error {
	// no-op
	return nil
}

type SegmentZoneMapIndexReader struct {
	inode *segmentZoneMapIndexNode
}

func NewSegmentZoneMapIndexReader() *SegmentZoneMapIndexReader {
	return &SegmentZoneMapIndexReader{}
}

func (reader *SegmentZoneMapIndexReader) Init(mgr base.INodeManager, host dataio.IndexFile, meta *common.IndexMeta) error {
	reader.inode = newSegmentZoneMapIndexNode(mgr, host, meta)
	return nil
}

func (reader *SegmentZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, []*roaring.Bitmap, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	node := handle.GetNode().(*segmentZoneMapIndexNode)

	var ans []*roaring.Bitmap
	var res bool
	var err error
	for i := 0; i < len(node.blockZoneMaps); i++ {
		ans = append(ans, nil)
	}
	row := uint32(0)
	process := func(key interface{}) error {
		if res, err = node.segmentZoneMap.MayContainsKey(key); err != nil {
			return err
		}
		if res {
			deeper, blockOffset, err := reader.MayContainsKey(key)
			if err != nil {
				return err
			}
			if deeper {
				if ans[blockOffset] == nil {
					ans[blockOffset] = roaring.NewBitmap()
				}
				ans[blockOffset].Add(row)
			}
		}
		row++
		return nil
	}
	err = gCommon.ProcessVector(keys, 0, -1, process, nil)
	if err != nil {
		return false, nil, err
	}
	for _, v := range ans {
		if v != nil {
			return true, ans, nil
		}
	}
	return false, nil, nil
}

func (reader *SegmentZoneMapIndexReader) MayContainsKey(key interface{}) (bool, uint32, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	node := handle.GetNode().(*segmentZoneMapIndexNode)

	var res bool
	var err error
	if res, err = node.segmentZoneMap.MayContainsKey(key); err != nil {
		return false, 0, err
	}
	if !res {
		return false, 0, nil
	}
	var ans int
	start, end := uint32(0), uint32(len(node.blockZoneMaps)-1)
	for start <= end {
		mid := start + (end-start)/2
		blockZoneMap := node.blockZoneMaps[mid]
		if ans, err = blockZoneMap.Query(key); err != nil {
			return false, 0, err
		}
		if ans == 0 {
			return true, mid, nil
		}
		if ans > 0 {
			start = mid + 1
			continue
		}
		if ans < 0 {
			end = mid - 1
			continue
		}
	}
	return false, 0, nil
}

type SegmentZoneMapIndexWriter struct {
	cType              common.CompressType
	host               dataio.IndexFile
	segmentZoneMap     *basic.ZoneMap
	blockZoneMap       *basic.ZoneMap
	blockZoneMapBuffer [][]byte
	colIdx             uint16
}

func NewSegmentZoneMapIndexWriter() *SegmentZoneMapIndexWriter {
	return &SegmentZoneMapIndexWriter{}
}

func (writer *SegmentZoneMapIndexWriter) Init(host dataio.IndexFile, cType common.CompressType, colIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	return nil
}

func (writer *SegmentZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.segmentZoneMap == nil {
		panic("unexpected error")
	}
	if writer.blockZoneMap.Initialized() {
		if err := writer.FinishBlock(); err != nil {
			return nil, err
		}
	}
	appender := writer.host
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.SegmentZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)

	var startOffset uint32
	segBuffer, err := writer.segmentZoneMap.Marshal()
	if err != nil {
		return nil, err
	}
	blockCount := uint32(len(writer.blockZoneMapBuffer))
	var iBuf bytes.Buffer
	iBuf.Write(encoding.EncodeUint32(blockCount))
	for _, blockBuf := range writer.blockZoneMapBuffer {
		iBuf.Write(encoding.EncodeUint16(uint16(len(blockBuf))))
		iBuf.Write(blockBuf)
	}
	iBuf.Write(encoding.EncodeUint16(uint16(len(segBuffer))))
	iBuf.Write(segBuffer)
	finalBuf := iBuf.Bytes()
	rawSize := uint32(len(finalBuf))
	cBuf := common.Compress(finalBuf, writer.cType)
	exactSize := uint32(len(cBuf))
	meta.SetSize(rawSize, exactSize)
	if startOffset, err = appender.Append(cBuf); err != nil {
		return nil, err
	}
	meta.SetStartOffset(startOffset)
	return meta, nil
}

func (writer *SegmentZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	typ := values.Typ
	if writer.blockZoneMap == nil {
		writer.blockZoneMap = basic.NewZoneMap(typ, nil)
		writer.segmentZoneMap = basic.NewZoneMap(typ, nil)
	} else {
		if writer.blockZoneMap.GetType() != typ {
			return errors.ErrTypeMismatch
		}
	}
	if err := writer.blockZoneMap.BatchUpdate(values, 0, -1); err != nil {
		return err
	}
	return nil
}

func (writer *SegmentZoneMapIndexWriter) SetMinMax(min, max interface{}, typ types.Type) error {
	if writer.blockZoneMap == nil {
		writer.blockZoneMap = basic.NewZoneMap(typ, nil)
		writer.segmentZoneMap = basic.NewZoneMap(typ, nil)
	} else {
		if writer.blockZoneMap.GetType() != typ {
			return errors.ErrTypeMismatch
		}
	}
	writer.blockZoneMap.SetMin(min)
	writer.blockZoneMap.SetMax(max)
	return nil
}

func (writer *SegmentZoneMapIndexWriter) FinishBlock() error {
	if writer.blockZoneMap == nil {
		panic("unexpected error")
	}
	blockBuf, err := writer.blockZoneMap.Marshal()
	if err != nil {
		return err
	}
	writer.blockZoneMapBuffer = append(writer.blockZoneMapBuffer, blockBuf)
	if err = writer.segmentZoneMap.Update(writer.blockZoneMap.GetMax()); err != nil {
		return err
	}
	if err = writer.segmentZoneMap.Update(writer.blockZoneMap.GetMin()); err != nil {
		return err
	}
	writer.blockZoneMap = basic.NewZoneMap(writer.segmentZoneMap.GetType(), nil)
	return nil
}
