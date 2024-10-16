// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type prefixBloomFilter struct {
	prefixFnId uint8
	bloomFilter
}

func NewPrefixBloomFilter(
	data containers.Vector,
	prefixFnId uint8,
	prefixFn func([]byte) []byte,
) (StaticFilter, error) {
	hashes := make([]uint64, 0)
	op := func(v []byte, _ bool, _ int) error {
		hash := hashV1(prefixFn(v))
		hashes = append(hashes, hash)
		return nil
	}
	if err := containers.ForeachWindowBytes(
		data.GetDownstreamVector(), 0, data.Length(), op, nil,
	); err != nil {
		return nil, err
	}
	bf, err := buildFuseFilter(hashes)
	if err != nil {
		return nil, err
	}
	return &prefixBloomFilter{
		prefixFnId:  prefixFnId,
		bloomFilter: *bf,
	}, nil
}

func (bf *prefixBloomFilter) PrefixFnId(level uint8) uint8 {
	if level != 1 {
		panic(fmt.Sprintf("unsupported level: %d", level))
	}
	return bf.prefixFnId
}

func (bf *prefixBloomFilter) Marshal() ([]byte, error) {
	var w bytes.Buffer
	if err := bf.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (bf *prefixBloomFilter) MarshalWithBuffer(w *bytes.Buffer) (err error) {
	if err = w.WriteByte(byte(bf.prefixFnId)); err != nil {
		return
	}
	err = bf.bloomFilter.MarshalWithBuffer(w)
	return
}

func (bf *prefixBloomFilter) Unmarshal(data []byte) error {
	bf.prefixFnId = uint8(data[0])
	data = data[1:]
	return bf.bloomFilter.Unmarshal(data)
}

func (bf *prefixBloomFilter) String() string {
	s := fmt.Sprintf("<PBF:%d>\n", bf.prefixFnId)
	s += strconv.Itoa(int(bf.SegmentCount))
	s += "\n"
	s += strconv.Itoa(int(bf.SegmentCountLength))
	s += "\n"
	s += strconv.Itoa(int(bf.SegmentLength))
	s += "\n"
	s += strconv.Itoa(int(bf.SegmentLengthMask))
	s += "\n"
	s += strconv.Itoa(len(bf.Fingerprints))
	s += "\n"
	s += "</PBF>"
	return s
}

func (bf *prefixBloomFilter) GetType() uint8 {
	return PBF
}

func (bf *prefixBloomFilter) MayContainsKey(key []byte) (bool, error) {
	panic("not supported")
}

func (bf *prefixBloomFilter) MayContainsAnyKeys(
	keys containers.Vector,
) (bool, *nulls.Bitmap, error) {
	panic("not supported")
}

func (bf *prefixBloomFilter) MayContainsAny(
	keys *vector.Vector,
	lowerBound int,
	upperBound int,
) bool {
	panic("not supported")
}

func (bf *prefixBloomFilter) PrefixMayContainsKey(
	key []byte,
	prefixFnId uint8,
	level uint8,
) (bool, error) {
	if level != 1 {
		panic(fmt.Sprintf("unsupported level: %d", level))
	}
	if bf.prefixFnId != prefixFnId {
		return false, moerr.NewInternalErrorNoCtxf("prefixFnId mismatch: %d != %d", bf.prefixFnId, prefixFnId)
	}
	return bf.bloomFilter.MayContainsKey(key)
}

func (bf *prefixBloomFilter) PrefixMayContainsAny(
	keys *vector.Vector,
	lowerBound int,
	upperBound int,
	prefixFnId uint8,
	level uint8,
) bool {
	if level != 1 {
		panic(fmt.Sprintf("unsupported level: %d", level))
	}
	if bf.prefixFnId != prefixFnId {
		return false
	}
	return bf.bloomFilter.MayContainsAny(keys, lowerBound, upperBound)
}

func (bf *prefixBloomFilter) MaxLevel() uint8 { return 1 }
