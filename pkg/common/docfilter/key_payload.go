// Copyright 2026 Matrix Origin
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

package docfilter

import (
	"context"
	"encoding/binary"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// BuildKeysWithMemoryAdmission consumes all chunks into one tagged probe
// structure. It preserves BuildWithMemoryAdmission's single-vector fast path.
func BuildKeysWithMemoryAdmission(ctx context.Context, keys *keycodec.KeyPayload, admission MemoryAdmission) ([]byte, error) {
	if keys == nil {
		return nil, moerr.NewInvalidInputNoCtx("docfilter: missing runtime keys")
	}
	if keys.ChunkCount() == 1 {
		var data []byte
		err := keys.Visit(ctx, func(v *vector.Vector) (err error) { data, err = BuildWithMemoryAdmission(v, admission); return err })
		return data, err
	}
	if !SupportsBitset(keys.Type()) {
		bytes, ok := bloomfilter.EstimateCBloomFilterMemoryBytes(int64(keys.Rows()), bloomFpProbability)
		if !ok || bytes > math.MaxInt64/2-1024 {
			return nil, moerr.NewInvalidInputNoCtx("docfilter: invalid bloom size")
		}
		release, err := acquireMemory(admission, bytes*2+1024)
		if err != nil {
			return nil, err
		}
		if release != nil {
			defer release()
		}
		bf := bloomfilter.NewCBloomFilterWithProbability(int64(keys.Rows()), bloomFpProbability)
		defer bf.Free()
		if err = keys.Visit(ctx, func(v *vector.Vector) error { bf.AddVector(v); return nil }); err != nil {
			return nil, err
		}
		return bf.MarshalWithPrefix(TagBloom)
	}
	rng, err := keyIntegerRange(ctx, keys)
	if err != nil {
		return nil, err
	}
	base := uint64(0)
	if CbitmapUseOffset {
		base = rng.min
	}
	bitCap := cbitmapBitCapForCount(rng.count)
	dense := rng.count != 0 && bitCap != 0 && rng.max-base < bitCap
	bytes := uint64(16) + rng.count*8
	span := uint64(0)
	if dense {
		span = rng.max - base + 1
		bytes = 17 + ((span+63)/64)*8
	}
	if bytes > math.MaxInt64 {
		return nil, moerr.NewInvalidInputNoCtx("docfilter: integer payload size overflow")
	}
	release, err := acquireMemory(admission, int64(bytes))
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	if dense {
		data := make([]byte, int(bytes))
		data[0] = TagCbitmap
		binary.LittleEndian.PutUint64(data[1:], base)
		binary.LittleEndian.PutUint64(data[9:], span)
		err = keys.Visit(ctx, func(v *vector.Vector) error {
			for row := 0; row < v.Length(); row++ {
				if v.IsNull(uint64(row)) {
					continue
				}
				value := rawIntToUint64(v.GetRawBytesAt(row)) - base
				data[17+int(value/8)] |= byte(1 << uint(value%8))
			}
			return nil
		})
		return data, err
	}
	// Allocate aligned words once; shift the final representation in place to
	// prepend the tag, just as buildSorted64TaggedBytes does for one vector.
	words := make([]uint64, int(rng.count)+2)
	n := 0
	err = keys.Visit(ctx, func(v *vector.Vector) error {
		for row := 0; row < v.Length(); row++ {
			if !v.IsNull(uint64(row)) {
				words[2+n] = rawIntToUint64(v.GetRawBytesAt(row))
				n++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	values := words[2 : 2+n]
	slices.Sort(values)
	unique := 0
	for _, value := range values {
		if unique == 0 || values[unique-1] != value {
			values[unique] = value
			unique++
		}
	}
	words[1] = uint64(unique)
	data := types.EncodeSlice(words)
	copy(data[1:], data[8:8+(unique+1)*8])
	data[0] = TagSorted64
	return data[:1+(unique+1)*8], nil
}

func keyIntegerRange(ctx context.Context, keys *keycodec.KeyPayload) (integerRange, error) {
	var rng integerRange
	err := keys.Visit(ctx, func(v *vector.Vector) error {
		r, ok := integerValueRange(v)
		if !ok {
			return moerr.NewInvalidInputNoCtx("docfilter: integer keys required")
		}
		if r.count == 0 {
			return nil
		}
		if rng.count == 0 {
			rng = r
			return nil
		}
		rng.count += r.count
		rng.min = min(rng.min, r.min)
		rng.max = max(rng.max, r.max)
		return nil
	})
	return rng, err
}
