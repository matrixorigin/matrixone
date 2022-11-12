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

package dict

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
)

const (
	unitLimit = 256
)

var (
	zeroUint64 []uint64
)

func init() {
	zeroUint64 = make([]uint64, unitLimit)
}

type reverseIndex interface {
	insert(keys any) ([]uint64, error)
	find(keys any) []uint64
	free()
}

type fixedReverseIndex struct {
	m  *mpool.MPool
	ht *hashtable.Int64HashMap
}

func newFixedReverseIndex(m *mpool.MPool) (*fixedReverseIndex, error) {
	ht := &hashtable.Int64HashMap{}
	if err := ht.Init(m); err != nil {
		ht.Free(m)
		return nil, err
	}
	return &fixedReverseIndex{
		m:  m,
		ht: ht,
	}, nil
}

func (idx *fixedReverseIndex) insert(keys any) ([]uint64, error) {
	ks := keys.([]uint64)
	count := len(ks)
	hashes := make([]uint64, unitLimit)
	values := make([]uint64, count)
	for i := 0; i < count; i += unitLimit {
		n := count - i
		if n > unitLimit {
			n = unitLimit
		}
		copy(hashes[:n], zeroUint64[:n])
		if err := idx.ht.InsertBatch(n, hashes[:n], unsafe.Pointer(&ks[i]), values[i:i+n], idx.m); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (idx *fixedReverseIndex) find(keys any) []uint64 {
	ks := keys.([]uint64)
	count := len(ks)
	hashes := make([]uint64, unitLimit)
	values := make([]uint64, count)
	for i := 0; i < count; i += unitLimit {
		n := count - i
		if n > unitLimit {
			n = unitLimit
		}
		copy(hashes[:n], zeroUint64[:n])
		idx.ht.FindBatch(n, hashes[:n], unsafe.Pointer(&ks[i]), values[i:i+n])
	}
	return values
}

func (idx *fixedReverseIndex) free() {
	idx.ht.Free(idx.m)
}

type varReverseIndex struct {
	m          *mpool.MPool
	ht         *hashtable.StringHashMap
	hashStates [][3]uint64
}

func newVarReverseIndex(m *mpool.MPool) (*varReverseIndex, error) {
	ht := &hashtable.StringHashMap{}
	if err := ht.Init(m); err != nil {
		ht.Free(m)
		return nil, err
	}
	return &varReverseIndex{
		m:          m,
		ht:         ht,
		hashStates: make([][3]uint64, unitLimit),
	}, nil
}

func (idx *varReverseIndex) insert(keys any) ([]uint64, error) {
	ks := checkPadding(keys.([][]byte))
	count := len(ks)
	values := make([]uint64, count)
	for i := 0; i < count; i += unitLimit {
		n := count - i
		if n > unitLimit {
			n = unitLimit
		}
		if err := idx.ht.InsertStringBatch(idx.hashStates, ks[i:i+n], values[i:i+n], idx.m); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (idx *varReverseIndex) find(keys any) []uint64 {
	ks := checkPadding(keys.([][]byte))
	count := len(ks)
	values := make([]uint64, count)
	for i := 0; i < count; i += unitLimit {
		n := count - i
		if n > unitLimit {
			n = unitLimit
		}
		idx.ht.FindStringBatch(idx.hashStates, ks[i:i+n], values[i:i+n])
	}
	return values
}

func (idx *varReverseIndex) free() {
	idx.ht.Free(idx.m)
}

// checkPadding checks if the length of each key is less than 16.
func checkPadding(keys [][]byte) [][]byte {
	ks := make([][]byte, len(keys))
	for i := range ks {
		if len(keys[i]) < 16 {
			dst := make([]byte, 16)
			copy(dst, keys[i])
			ks[i] = dst
		} else {
			ks[i] = keys[i]
		}
	}
	return ks
}
