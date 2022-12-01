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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/index/dict"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	MaxLowCardinality = math.MaxUint16 + 1
)

var (
	ErrNotSupported = moerr.NewNotSupportedNoCtx("the type is not supported for low cardinality index")
)

type LowCardinalityIndex struct {
	typ types.Type

	m    *mpool.MPool
	dict *dict.Dict
	// poses is the positions of original data in the dictionary.
	// Currently, the type of poses[i] is `T_uint16` which means
	// the max cardinality of LowCardinalityIndex is 65536.
	// The position of `null` value is 0.
	poses *vector.Vector

	ref int
}

func New(typ types.Type, m *mpool.MPool) (*LowCardinalityIndex, error) {
	if typ.Oid == types.T_decimal128 || typ.Oid == types.T_json {
		return nil, ErrNotSupported
	}

	d, err := dict.New(typ, m)
	if err != nil {
		return nil, err
	}
	return &LowCardinalityIndex{
		typ:   typ,
		m:     m,
		dict:  d,
		poses: vector.New(types.T_uint16.ToType()),
		ref:   1,
	}, nil
}

func (idx *LowCardinalityIndex) GetPoses() *vector.Vector {
	return idx.poses
}

func (idx *LowCardinalityIndex) GetDict() *dict.Dict {
	return idx.dict
}

func (idx *LowCardinalityIndex) Dup() *LowCardinalityIndex {
	idx.ref++
	return idx
}

func (idx *LowCardinalityIndex) DupEmpty() *LowCardinalityIndex {
	return &LowCardinalityIndex{
		typ:   idx.typ,
		m:     idx.m,
		dict:  idx.dict.Dup(),
		poses: vector.New(types.T_uint16.ToType()),
		ref:   1,
	}
}

func (idx *LowCardinalityIndex) InsertBatch(data *vector.Vector) error {
	originalLen := data.Length()
	var sels []int64
	if nulls.Any(data.Nsp) {
		sels = make([]int64, 0, originalLen)
		for i := 0; i < originalLen; i++ {
			if !nulls.Contains(data.Nsp, uint64(i)) {
				sels = append(sels, int64(i))
			}
		}
	}

	var ips []uint16
	var err error
	if sels != nil {
		if err = vector.Shuffle(data, sels, idx.m); err != nil {
			return err
		}

		values, err := idx.dict.InsertBatch(data)
		if err != nil {
			return err
		}

		i := 0
		ips = make([]uint16, originalLen)
		for j := 0; j < originalLen; j++ {
			if i < len(sels) && int64(j) == sels[i] {
				ips[j] = values[i]
				i++
			} else {
				ips[j] = 0
			}
		}
	} else {
		if ips, err = idx.dict.InsertBatch(data); err != nil {
			return err
		}
	}

	return vector.AppendFixed(idx.poses, ips, idx.m)
}

// Encode uses the dictionary of the current index to encode the original data.
func (idx *LowCardinalityIndex) Encode(dst, src *vector.Vector) error {
	poses := idx.dict.FindBatch(src)
	return vector.AppendFixed(dst, poses, idx.m)
}

func (idx *LowCardinalityIndex) Free() {
	if idx.ref == 0 {
		return
	}
	idx.ref--
	if idx.ref > 0 {
		return
	}

	idx.poses.Free(idx.m)
	idx.dict.Free()
}
