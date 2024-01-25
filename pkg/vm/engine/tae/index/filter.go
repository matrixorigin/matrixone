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
	"strconv"

	"github.com/FastFilter/xorfilter"
	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/samber/lo"
)

const FuseFilterError = "too many iterations, you probably have duplicate keys"

func DecodeBloomFilter(sf StaticFilter, data []byte) error {
	if err := sf.Unmarshal(data); err != nil {
		return err
	}
	return nil
}

func NewEmptyBinaryFuseFilter() StaticFilter {
	return &binaryFuseFilter{}
}

type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	MayContainsAnyKeys(keys containers.Vector) (bool, *nulls.Bitmap, error)
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	String() string
}

func hashV1(v []byte) uint64 {
	return xxhash.Sum64(v)
}

type binaryFuseFilter struct {
	xorfilter.BinaryFuse8
}

func NewBinaryFuseFilter(data containers.Vector) (StaticFilter, error) {
	return NewBinaryFuseFilterByVectors([]containers.Vector{data})
}

func buildFuseFilter(hashes []uint64) (StaticFilter, error) {
	var inner *xorfilter.BinaryFuse8
	var err error
	if inner, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
		if err.Error() == FuseFilterError {
			// 230+ duplicate keys in hashes
			// block was deleted 115+ rows
			hashes = lo.Uniq(hashes)
			if inner, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
				return nil, err
			}
		}
	}
	sf := &binaryFuseFilter{}
	sf.BinaryFuse8 = *inner
	return sf, nil
}

func NewBinaryFuseFilterByVectors(datas []containers.Vector) (StaticFilter, error) {
	hashes := make([]uint64, 0)
	op := func(v []byte, _ bool, _ int) error {
		hash := hashV1(v)
		hashes = append(hashes, hash)
		return nil
	}
	var err error
	for _, data := range datas {
		if err = containers.ForeachWindowBytes(data.GetDownstreamVector(), 0, data.Length(), op, nil); err != nil {
			return nil, err
		}
	}
	return buildFuseFilter(hashes)
}

func (filter *binaryFuseFilter) MayContainsKey(key []byte) (bool, error) {
	hash := hashV1(key)
	return filter.Contains(hash), nil
}

func (filter *binaryFuseFilter) MayContainsAnyKeys(keys containers.Vector) (bool, *nulls.Bitmap, error) {
	var positive *nulls.Bitmap

	row := uint32(0)
	op := func(v []byte, _ bool, _ int) error {
		hash := hashV1(v)
		if filter.Contains(hash) {
			if positive == nil {
				positive = nulls.NewWithSize(int(row) + 1)
			}
			positive.Add(uint64(row))
		}
		row++
		return nil
	}

	if err := containers.ForeachWindowBytes(keys.GetDownstreamVector(), 0, keys.Length(), op, nil); err != nil {
		return false, nil, err
	}
	return !positive.IsEmpty(), positive, nil
}

func (filter *binaryFuseFilter) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if _, err = types.WriteValues(
		&w,
		filter.Seed,
		filter.SegmentLength,
		filter.SegmentLengthMask,
		filter.SegmentCount,
		filter.SegmentCountLength); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeSlice(filter.Fingerprints)); err != nil {
		return
	}
	buf = w.Bytes()
	return
}

func (filter *binaryFuseFilter) Unmarshal(buf []byte) error {
	filter.Seed = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	filter.SegmentLength = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.SegmentLengthMask = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.SegmentCount = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.SegmentCountLength = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	filter.Fingerprints = types.DecodeSlice[uint8](buf)
	return nil
}

func (filter *binaryFuseFilter) String() string {
	s := "<SF>\n"
	s += strconv.Itoa(int(filter.SegmentCount))
	s += "\n"
	s += strconv.Itoa(int(filter.SegmentCountLength))
	s += "\n"
	s += strconv.Itoa(int(filter.SegmentLength))
	s += "\n"
	s += strconv.Itoa(int(filter.SegmentLengthMask))
	s += "\n"
	s += strconv.Itoa(len(filter.Fingerprints))
	s += "\n"
	s += "</SF>"
	return s
}
