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

package index

import (
	"bytes"
	"github.com/samber/lo"
	"strconv"

	"github.com/FastFilter/xorfilter"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const FuseFilterError = "too many iterations, you probably have duplicate keys"

type StaticFilter interface {
	MayContainsKey(key any) (bool, error)
	MayContainsAnyKeys(keys containers.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error)
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	GetMemoryUsage() uint32
	Print() string
}

type binaryFuseFilter struct {
	typ   types.Type
	inner *xorfilter.BinaryFuse8
}

func NewBinaryFuseFilter(data containers.Vector) (StaticFilter, error) {
	sf := &binaryFuseFilter{typ: data.GetType()}
	hashes := make([]uint64, 0)
	op := func(v any, _ int) error {
		hash, err := types.Hash(v, sf.typ)
		if err != nil {
			return err
		}
		hashes = append(hashes, hash)
		return nil
	}
	var err error
	if err = data.Foreach(op, nil); err != nil {
		return nil, err
	}
	if sf.inner, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
		if err.Error() == FuseFilterError {
			// 230+ duplicate keys in hashes
			// block was deleted 115+ rows
			hashes = lo.Uniq[uint64](hashes)
			if sf.inner, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
				return nil, err
			}
		}
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

func (filter *binaryFuseFilter) MayContainsKey(key any) (bool, error) {
	hash, err := types.Hash(key, filter.typ)
	if err != nil {
		return false, err
	}
	if filter.inner.Contains(hash) {
		return true, nil
	}
	return false, nil
}

func (filter *binaryFuseFilter) MayContainsAnyKeys(keys containers.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error) {
	positive := roaring.NewBitmap()
	row := uint32(0)
	exist := false

	op := func(v any, _ int) error {
		hash, err := types.Hash(v, filter.typ)
		if err != nil {
			return err
		}
		if filter.inner.Contains(hash) {
			positive.Add(row)
		}
		row++
		return nil
	}

	if err := keys.Foreach(op, visibility); err != nil {
		return false, nil, err
	}
	if positive.GetCardinality() != 0 {
		exist = true
	}
	return exist, positive, nil
}

func (filter *binaryFuseFilter) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if _, err = w.Write(types.EncodeType(&filter.typ)); err != nil {
		return
	}
	if _, err = types.WriteValues(
		&w,
		filter.inner.Seed,
		filter.inner.SegmentLength,
		filter.inner.SegmentLengthMask,
		filter.inner.SegmentCount,
		filter.inner.SegmentCountLength); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeSlice(filter.inner.Fingerprints)); err != nil {
		return
	}
	buf = w.Bytes()
	return
}

func (filter *binaryFuseFilter) Unmarshal(buf []byte) error {
	filter.typ = types.DecodeType(buf[:types.TSize])
	buf = buf[types.TSize:]
	filter.inner = &xorfilter.BinaryFuse8{}
	filter.inner.Seed = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	filter.inner.SegmentLength = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.inner.SegmentLengthMask = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.inner.SegmentCount = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.inner.SegmentCountLength = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.inner.Fingerprints = types.DecodeSlice[uint8](buf)
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
