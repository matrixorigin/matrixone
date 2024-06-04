package index

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type hybridFilter struct {
	prefixBloomFilter
	bloomFilter
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
		err error
		w   bytes.Buffer
	)
	if err = w.WriteByte(byte(bf.prefixFnId)); err != nil {
		return nil, err
	}
	if err = bf.prefixBloomFilter.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	if err = bf.bloomFilter.MarshalWithBuffer(&w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (bf *hybridFilter) Unmarshal(data []byte) error {
	bf.prefixFnId = uint8(data[0])
	data = data[1:]
	if err := bf.prefixBloomFilter.Unmarshal(data); err != nil {
		return err
	}
	return bf.bloomFilter.Unmarshal(data)
}

func (bf *hybridFilter) String() string {
	s := fmt.Sprintf("<HBF:%d>", bf.prefixFnId)
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
	s += "</HBF>"
	return s
}

func (bf *hybridFilter) GetType() uint8 {
	return HBF
}

func (bf *hybridFilter) PrefixFnId() uint8 {
	return bf.prefixFnId
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
