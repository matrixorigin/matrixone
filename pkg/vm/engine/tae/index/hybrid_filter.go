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
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type HybridBloomFilter = hybridFilter

type hybridFilter struct {
	prefixLevel1 prefixBloomFilter
	prefixLevel2 prefixBloomFilter
	bloomFilter  bloomFilter
}

func NewHybridBloomFilter(
	data containers.Vector,
	level1PrefixFnId uint8,
	level1Prefix func([]byte) []byte,
	level2PrefixFnId uint8,
	level2Prefix func([]byte) []byte,
) (StaticFilter, error) {
	hashes := make([]uint64, 0)
	level1Hashes := make([]uint64, 0)
	level2Hashes := make([]uint64, 0)
	op := func(v []byte, _ bool, _ int) error {
		level1Hash := hashV1(level1Prefix(v))
		level2Hash := hashV1(level2Prefix(v))
		hash := hashV1(v)
		hashes = append(hashes, hash)
		level1Hashes = append(level1Hashes, level1Hash)
		level2Hashes = append(level2Hashes, level2Hash)
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
	lvl1, err := buildFuseFilter(level1Hashes)
	if err != nil {
		return nil, err
	}
	lvl2, err := buildFuseFilter(level2Hashes)
	if err != nil {
		return nil, err
	}
	return &hybridFilter{
		prefixLevel1: prefixBloomFilter{
			prefixFnId:  level1PrefixFnId,
			bloomFilter: *lvl1,
		},
		prefixLevel2: prefixBloomFilter{
			prefixFnId:  level2PrefixFnId,
			bloomFilter: *lvl2,
		},
		bloomFilter: *bf,
	}, nil
}

func (bf *hybridFilter) Marshal() ([]byte, error) {
	var (
		err              error
		w                bytes.Buffer
		len1, len2, len3 uint32
	)
	if _, err = w.Write(types.EncodeUint32(&len1)); err != nil {
		return nil, err
	}
	if _, err = w.Write(types.EncodeUint32(&len2)); err != nil {
		return nil, err
	}
	if _, err = w.Write(types.EncodeUint32(&len3)); err != nil {
		return nil, err
	}
	if err = bf.prefixLevel1.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	len1 = uint32(w.Len()) - uint32(3*unsafe.Sizeof(len1))
	if err = bf.prefixLevel2.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	len2 = uint32(w.Len()) - len1 - uint32(3*unsafe.Sizeof(len1))
	if err = bf.bloomFilter.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	len3 = uint32(w.Len()) - len1 - len2 - uint32(3*unsafe.Sizeof(len1))
	buf := w.Bytes()
	copy(buf[0:], types.EncodeUint32(&len1))
	copy(buf[unsafe.Sizeof(len1):], types.EncodeUint32(&len2))
	copy(buf[2*unsafe.Sizeof(len1):], types.EncodeUint32(&len3))
	return buf, nil
}

func (bf *hybridFilter) Unmarshal(data []byte) error {
	len1 := types.DecodeUint32(data)
	len2 := types.DecodeUint32(data[unsafe.Sizeof(len1):])
	len3 := types.DecodeUint32(data[2*unsafe.Sizeof(len1):])
	start := int(3 * unsafe.Sizeof(len1))
	end := start + int(len1)
	if err := bf.prefixLevel1.Unmarshal(data[start:end]); err != nil {
		return err
	}
	start = end
	end = start + int(len2)
	if err := bf.prefixLevel2.Unmarshal(data[start:end]); err != nil {
		return err
	}
	start = end
	end = start + int(len3)
	return bf.bloomFilter.Unmarshal(data[start:end])
}

func (bf *hybridFilter) String() string {
	s := fmt.Sprintf("<HBF:%d:%d>", bf.prefixLevel1.prefixFnId, bf.prefixLevel2.prefixFnId)
	s += "</HBF>"
	return s
}

func (bf *hybridFilter) GetType() uint8 {
	return HBF
}

func (bf *hybridFilter) PrefixFnId(level uint8) uint8 {
	if level == 1 {
		return bf.prefixLevel1.PrefixFnId(1)
	} else if level == 2 {
		return bf.prefixLevel2.PrefixFnId(1)
	}
	panic(fmt.Sprintf("invalid level %d", level))
}

func (bf *hybridFilter) MayContainsKey(key []byte) (bool, error) {
	return bf.bloomFilter.MayContainsKey(key)
}

func (bf *hybridFilter) MayContainsAnyKeys(
	keys containers.Vector,
) (bool, *nulls.Bitmap, error) {
	return bf.bloomFilter.MayContainsAnyKeys(keys)
}

func (bf *hybridFilter) MayContainsAny(
	keys *vector.Vector,
	lowerBound int,
	upperBound int,
) bool {
	return bf.bloomFilter.MayContainsAny(keys, lowerBound, upperBound)
}

func (bf *hybridFilter) PrefixMayContainsKey(
	key []byte,
	prefixFnId uint8,
	level uint8,
) (bool, error) {
	if level == 1 {
		return bf.prefixLevel1.PrefixMayContainsKey(key, prefixFnId, 1)
	} else if level == 2 {
		return bf.prefixLevel2.PrefixMayContainsKey(key, prefixFnId, 1)
	}
	panic(fmt.Sprintf("invalid level %d", level))
}

func (bf *hybridFilter) PrefixMayContainsAny(
	keys *vector.Vector,
	lowerBound int,
	upperBound int,
	prefixFnId uint8,
	level uint8,
) bool {
	if level == 1 {
		return bf.prefixLevel1.PrefixMayContainsAny(keys, lowerBound, upperBound, prefixFnId, 1)
	} else if level == 2 {
		return bf.prefixLevel2.PrefixMayContainsAny(keys, lowerBound, upperBound, prefixFnId, 1)
	}
	panic(fmt.Sprintf("invalid level %d", level))
}

func (bf *hybridFilter) MaxLevel() uint8 {
	return 2
}
