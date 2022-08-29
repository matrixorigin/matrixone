package dict

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"unsafe"
)

type reverseIndex interface {
	insert(keys any) ([]uint64, error)
	find(keys any) []uint64
}

type fixedReverseIndex struct {
	m  *mheap.Mheap
	ht *hashtable.Int64HashMap
}

func newFixedReverseIndex(m *mheap.Mheap) (*fixedReverseIndex, error) {
	ht := &hashtable.Int64HashMap{}
	if err := ht.Init(m); err != nil {
		return nil, err
	}
	return &fixedReverseIndex{
		m:  m,
		ht: ht,
	}, nil
}

func (idx *fixedReverseIndex) insert(keys any) ([]uint64, error) {
	ks := keys.([]uint64)
	n := len(ks)
	hashes := make([]uint64, n)
	values := make([]uint64, n)
	if err := idx.ht.InsertBatch(n, hashes, unsafe.Pointer(&ks[0]), values, idx.m); err != nil {
		return nil, err
	}
	return values, nil
}

func (idx *fixedReverseIndex) find(keys any) []uint64 {
	ks := keys.([]uint64)
	n := len(ks)
	hashes := make([]uint64, n)
	values := make([]uint64, n)
	idx.ht.FindBatch(n, hashes, unsafe.Pointer(&ks[0]), values)
	return values
}

type varReverseIndex struct {
	m          *mheap.Mheap
	ht         *hashtable.StringHashMap
	hashStates [][3]uint64
}

func newVarReverseIndex(m *mheap.Mheap) (*varReverseIndex, error) {
	ht := &hashtable.StringHashMap{}
	if err := ht.Init(m); err != nil {
		return nil, err
	}
	return &varReverseIndex{
		m:          m,
		ht:         ht,
		hashStates: make([][3]uint64, hashmap.UnitLimit),
	}, nil
}

func (idx *varReverseIndex) insert(keys any) ([]uint64, error) {
	ks := idx.needPadding(keys.([][]byte))
	values := make([]uint64, len(ks))
	if err := idx.ht.InsertStringBatch(idx.hashStates, ks, values, idx.m); err != nil {
		return nil, err
	}
	return values, nil
}

func (idx *varReverseIndex) find(keys any) []uint64 {
	ks := idx.needPadding(keys.([][]byte))
	values := make([]uint64, len(ks))
	idx.ht.FindStringBatch(idx.hashStates, ks, values)
	return values
}

// needPadding checks if the length of each key is less than 16.
func (idx *varReverseIndex) needPadding(keys [][]byte) [][]byte {
	ks := make([][]byte, len(keys))
	for i := range ks {
		if len(keys) < 16 {
			dst := make([]byte, 16)
			copy(dst, keys[i])
			ks[i] = dst
		} else {
			ks[i] = keys[i]
		}
	}
	return ks
}
