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
	"errors"
	"math"

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
	ErrNotSupported = errors.New("the type is not supported for low cardinality index")
)

//type Poses struct {
//	values *vector.Vector
//	sels   [][]int64 // sels[0] -> null values
//	rowid  int64
//}
//
//func newPoses() *Poses {
//	return &Poses{
//		values: vector.New(types.T_uint16.ToType()),
//		sels:   make([][]int64, MaxLowCardinality+1),
//		rowid:  0,
//	}
//}
//
//func (p *Poses) GetSels() [][]int64 {
//	return p.sels
//}
//
//func (p *Poses) Insert(data []uint16, m *mpool.MPool) error {
//	for i, v := range data {
//		if len(p.sels[v]) == 0 {
//			p.sels[v] = make([]int64, 0, 64)
//		}
//		p.sels[v] = append(p.sels[v], p.rowid+int64(i))
//	}
//	p.rowid += int64(len(data))
//
//	return vector.AppendFixed(p.values, data, m)
//}
//
//func (p *Poses) Free(m *mpool.MPool) {
//	p.values.Free(m)
//}

type LowCardinalityIndex struct {
	typ types.Type

	m    *mpool.MPool
	dict *dict.Dict
	// poses is the positions of original data in the dictionary.
	// Currently, the type of poses[i] is `T_uint16` which means
	// the max cardinality of LowCardinalityIndex is 65536.
	// The position of `null` value is 0.
	poses *vector.Vector

	sels  [][]int64 // sels[0] -> null values
	rowid int

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
		sels:  make([][]int64, MaxLowCardinality+1),
		rowid: 0,
		ref:   1,
	}, nil
}

func (idx *LowCardinalityIndex) GetSels() [][]int64 {
	return idx.sels
}

func (idx *LowCardinalityIndex) UpdateSels(data []uint16, flags []uint8) {
	cnt := 0
	for i, v := range data {
		if flags != nil && flags[i] == 0 {
			continue
		}
		if len(idx.sels[v]) == 0 {
			idx.sels[v] = make([]int64, 0, 64)
		}
		idx.sels[v] = append(idx.sels[v], int64(i+idx.rowid))
		cnt++
	}
	idx.rowid += cnt
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
		sels:  make([][]int64, MaxLowCardinality+1),
		rowid: 0,
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

	idx.UpdateSels(ips, nil)
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
