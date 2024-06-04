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

type hybridFilter struct {
	prefixBloomFilter prefixBloomFilter
	bloomFilter       bloomFilter
}

func NewHybridBloomFilter(
	data containers.Vector,
	prefixFnId uint8,
	prefixFn func([]byte) []byte,
) (StaticFilter, error) {
	prefixHashes := make([]uint64, 0)
	hashes := make([]uint64, 0)
	op := func(v []byte, _ bool, _ int) error {
		prefixHash := hashV1(prefixFn(v))
		hash := hashV1(v)
		hashes = append(hashes, hash)
		prefixHashes = append(prefixHashes, prefixHash)
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
	pbf, err := buildFuseFilter(prefixHashes)
	if err != nil {
		return nil, err
	}
	return &hybridFilter{
		prefixBloomFilter: prefixBloomFilter{
			prefixFnId:  prefixFnId,
			bloomFilter: *pbf,
		},
		bloomFilter: *bf,
	}, nil
}

func (bf *hybridFilter) Marshal() ([]byte, error) {
	var (
		err        error
		w          bytes.Buffer
		len1, len2 uint32
	)
	if _, err = w.Write(types.EncodeUint32(&len1)); err != nil {
		return nil, err
	}
	if _, err = w.Write(types.EncodeUint32(&len2)); err != nil {
		return nil, err
	}
	if err = bf.prefixBloomFilter.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	len1 = uint32(w.Len()) - uint32(2*unsafe.Sizeof(len1))
	if err = bf.bloomFilter.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	len2 = uint32(w.Len()) - len1 - uint32(2*unsafe.Sizeof(len1))
	buf := w.Bytes()
	copy(buf[0:], types.EncodeUint32(&len1))
	copy(buf[unsafe.Sizeof(len1):], types.EncodeUint32(&len2))
	return buf, nil
}

func (bf *hybridFilter) Unmarshal(data []byte) error {
	len1 := types.DecodeUint32(data)
	len2 := types.DecodeUint32(data[unsafe.Sizeof(len1):])
	start := int(2 * unsafe.Sizeof(len1))
	end := start + int(len1)
	if err := bf.prefixBloomFilter.Unmarshal(data[start:end]); err != nil {
		return err
	}
	start = end
	end = start + int(len2)
	return bf.bloomFilter.Unmarshal(data[start:end])
}

func (bf *hybridFilter) String() string {
	s := fmt.Sprintf("<HBF:%d>", bf.prefixBloomFilter.PrefixFnId())
	s += "</HBF>"
	return s
}

func (bf *hybridFilter) GetType() uint8 {
	return HBF
}

func (bf *hybridFilter) PrefixFnId() uint8 {
	return bf.prefixBloomFilter.PrefixFnId()
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
) (bool, error) {
	return bf.prefixBloomFilter.PrefixMayContainsKey(key, prefixFnId)
}

func (bf *hybridFilter) PrefixMayContainsAnyKeys(
	keys containers.Vector,
	prefixFnId uint8,
) (bool, *nulls.Bitmap, error) {
	return bf.prefixBloomFilter.PrefixMayContainsAnyKeys(keys, prefixFnId)
}

func (bf *hybridFilter) PrefixMayContainsAny(
	keys *vector.Vector,
	lowerBound int,
	upperBound int,
	prefixFnId uint8,
) bool {
	return bf.prefixBloomFilter.PrefixMayContainsAny(
		keys, lowerBound, upperBound, prefixFnId,
	)
}
