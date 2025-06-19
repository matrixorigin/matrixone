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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"strconv"
	"strings"

	"github.com/FastFilter/xorfilter"
	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/samber/lo"
)

const FuseFilterErrorMsg = "too many iterations"

func DecodeBloomFilter(sf StaticFilter, buf []byte) error {
	if err := sf.Unmarshal(buf); err != nil {
		return err
	}
	return nil
}

func NewEmptyBloomFilterWithType(t uint8) StaticFilter {
	if t == BF {
		return &bloomFilter{}
	} else if t == PBF {
		return &prefixBloomFilter{}
	} else if t == HBF {
		return &hybridFilter{}
	} else {
		return nil
	}
}

type BloomFilter = bloomFilter

func NewEmptyBloomFilter() StaticFilter {
	return &bloomFilter{}
}

func hashV1(v []byte) uint64 {
	return xxhash.Sum64(v)
}

type bloomFilter struct {
	xorfilter.BinaryFuse8
}

func NewBloomFilter(vec containers.Vector) (StaticFilter, error) {
	return NewBloomFilter2([]containers.Vector{vec})
}

func buildFuseFilter(hashes []uint64) (*bloomFilter, error) {
	var inners *xorfilter.BinaryFuse8
	var err error
	if inners, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
		logutil.Error("BuildFuseFilter",
			zap.String("error", err.Error()))
		if strings.Contains(err.Error(), FuseFilterErrorMsg) {
			// 230+ duplicate keys in hashes
			// block was deleted 115+ rows
			oldHashes := hashes
			hashes = lo.Uniq(hashes)
			logutil.Info("BuildFuseFilter",
				zap.String("error", err.Error()),
				zap.Uint64s("old hashes", oldHashes),
				zap.Uint64s("hashes", hashes))
			if inners, err = xorfilter.PopulateBinaryFuse8(hashes); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	sf := &bloomFilter{}
	sf.BinaryFuse8 = *inners
	return sf, nil
}

func NewBloomFilter2(vectors []containers.Vector) (StaticFilter, error) {
	hashes := make([]uint64, 0)
	op := func(v []byte, _ bool, _ int) error {
		hash := hashV1(v)
		hashes = append(hashes, hash)
		return nil
	}
	var err error
	for _, data := range vectors {
		if err = containers.ForeachWindowBytes(data.GetDownstreamVector(), 0, data.Length(), op, nil); err != nil {
			return nil, err
		}
	}
	return buildFuseFilter(hashes)
}

func (filter *bloomFilter) MayContainsKey(key []byte) (bool, error) {
	hash := hashV1(key)
	return filter.Contains(hash), nil
}

func (filter *bloomFilter) MayContainsAny(keys *vector.Vector, lowerBound int, upperBound int) bool {
	found := false
	op := func(v []byte, _ bool, _ int) error {
		hash := hashV1(v)
		if filter.Contains(hash) {
			found = true
			return moerr.GetOkExpectedEOB()
		}
		return nil
	}
	_ = containers.ForeachWindowBytes(keys, lowerBound, upperBound-lowerBound, op, nil)
	return found
}

func (filter *bloomFilter) MayContainsAnyKeys(keys containers.Vector) (bool, *nulls.Bitmap, error) {
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

func (filter *bloomFilter) Marshal() (buf []byte, err error) {
	var str bytes.Buffer
	if err = filter.MarshalWithBuffer(&str); err != nil {
		return
	}
	buf = str.Bytes()
	return
}

func (filter *bloomFilter) MarshalWithBuffer(str *bytes.Buffer) (err error) {
	if _, err = types.WriteValues(
		str,
		filter.Seed,
		filter.SegmentLength,
		filter.SegmentLengthMask,
		filter.SegmentCount,
		filter.SegmentCountLength); err != nil {
		return
	}
	_, err = str.Write(types.EncodeSlice(filter.Fingerprints))
	return
}

func (filter *bloomFilter) Unmarshal(buffer []byte) error {
	filter.Seed = types.DecodeFixed[uint64](buffer[:8])
	buffer = buffer[8:]
	filter.SegmentLength = types.DecodeFixed[uint32](buffer[:4])
	buffer = buffer[4:]
	filter.SegmentLengthMask = types.DecodeFixed[uint32](buffer[:4])
	buffer = buffer[4:]
	filter.SegmentCount = types.DecodeFixed[uint32](buffer[:4])
	buffer = buffer[4:]
	filter.SegmentCountLength = types.DecodeFixed[uint32](buffer[:4])
	buffer = buffer[4:]
	filter.Fingerprints = types.DecodeSlice[uint8](buffer)
	return nil
}

func (filter *bloomFilter) String() string {
	s := "<BF>\n"
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
	s += "</BF>"
	return s
}

func (filter *bloomFilter) PrefixFnId(_ uint8) uint8 {
	panic("not supported")
}

func (filter *bloomFilter) GetType() uint8 {
	return BF
}

func (filter *bloomFilter) PrefixMayContainsKey(key []byte, prefixFnId uint8, level uint8) (bool, error) {
	panic("not supported")
}

func (filter *bloomFilter) PrefixMayContainsAnyKeys(keys containers.Vector, prefixFnId uint8, level uint8) (bool, *nulls.Bitmap, error) {
	panic("not supported")
}

func (filter *bloomFilter) PrefixMayContainsAny(keys *vector.Vector, lowerBound int, upperBound int, prefixFnId uint8, level uint8) bool {
	panic("not supported")
}

func (filter *bloomFilter) MaxLevel() uint8 {
	return 0
}
