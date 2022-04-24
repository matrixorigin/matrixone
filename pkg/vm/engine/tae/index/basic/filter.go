package basic

import (
	"bytes"
	"github.com/FastFilter/xorfilter"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"strconv"
)

type StaticFilter interface {
	MayContainsKey(key interface{}) (bool, error)
	MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error)
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	GetMemoryUsage() uint32
	Print() string
}

type binaryFuseFilter struct {
	typ   types.Type
	inner *xorfilter.BinaryFuse8
}

func NewBinaryFuseFilter(data *vector.Vector) (StaticFilter, error) {
	sf := &binaryFuseFilter{typ: data.Typ}
	hashes := make([]uint64, 0)
	collector := func(v interface{}) error {
		hash, err := common.Hash(v, sf.typ)
		if err != nil {
			return err
		}
		hashes = append(hashes, hash)
		return nil
	}
	var err error
	if err = common.ProcessVector(data, 0, -1, collector, nil); err != nil {
		return nil, err
	}
	if sf.inner, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
		return nil, err
	}
	return sf, nil
}

func NewBinaryFuseFilterFromSource(data []byte) (StaticFilter, error) {
	sf := binaryFuseFilter{}
	if err := sf.Unmarshal(data); err != nil {
		return nil, err
	}
	return &sf, nil
}

func (filter *binaryFuseFilter) MayContainsKey(key interface{}) (bool, error) {
	hash, err := common.Hash(key, filter.typ)
	if err != nil {
		return false, err
	}
	if filter.inner.Contains(hash) {
		return true, nil
	}
	return false, nil
}

func (filter *binaryFuseFilter) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error) {
	positive := roaring.NewBitmap()
	row := uint32(0)
	exist := false

	collector := func(v interface{}) error {
		hash, err := common.Hash(v, filter.typ)
		if err != nil {
			return err
		}
		if filter.inner.Contains(hash) {
			positive.Add(row)
		}
		row++
		return nil
	}

	if err := common.ProcessVector(keys, 0, -1, collector, visibility); err != nil {
		return false, nil, err
	}
	if positive.GetCardinality() != 0 {
		exist = true
	}
	return exist, positive, nil
}

func (filter *binaryFuseFilter) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeType(filter.typ))
	buf.Write(encoding.EncodeUint64(filter.inner.Seed))
	buf.Write(encoding.EncodeUint32(filter.inner.SegmentLength))
	buf.Write(encoding.EncodeUint32(filter.inner.SegmentLengthMask))
	buf.Write(encoding.EncodeUint32(filter.inner.SegmentCount))
	buf.Write(encoding.EncodeUint32(filter.inner.SegmentCountLength))
	buf.Write(encoding.EncodeUint8Slice(filter.inner.Fingerprints))
	return buf.Bytes(), nil
}

func (filter *binaryFuseFilter) Unmarshal(buf []byte) error {
	filter.typ = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	filter.inner = &xorfilter.BinaryFuse8{}
	filter.inner.Seed = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	filter.inner.SegmentLength = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	filter.inner.SegmentLengthMask = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	filter.inner.SegmentCount = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	filter.inner.SegmentCountLength = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	filter.inner.Fingerprints = encoding.DecodeUint8Slice(buf)
	return nil
}

func (filter *binaryFuseFilter) Print() string {
	s := "<SF>\n"
	s += filter.typ.String()
	s += "\n"
	s += strconv.Itoa(int(filter.inner.SegmentCount))
	s += "\n"
	s += strconv.Itoa(int(filter.inner.SegmentCountLength))
	s += "\n"
	s += strconv.Itoa(int(filter.inner.SegmentLength))
	s += "\n"
	s += strconv.Itoa(int(filter.inner.SegmentLengthMask))
	s += "\n"
	s += strconv.Itoa(len(filter.inner.Fingerprints))
	s += "\n"
	s += "</SF>"
	return s
}

func (filter *binaryFuseFilter) GetMemoryUsage() uint32 {
	size := uint32(0)
	size += 8
	size += 4 * 4
	size += uint32(len(filter.inner.Fingerprints))
	return size
}
