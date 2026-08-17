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

package hashmap

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	UnitLimit             = 256
	HashMapSizeThreshHold = UnitLimit * 128
	HashMapSizeEstimate   = UnitLimit * 32
)

var (
	OneUInt8s  []uint8
	OneInt64s  []int64
	zeroUint64 []uint64
	zeroUint32 []uint32
)

// HashMap is the encapsulated hash table interface exposed to the outside
type HashMap interface {
	// HasNull returns whether the hash map considers the null values.
	HasNull() bool
	// Free method frees the hash map.
	Free()
	// AddGroup adds 1 to the row count of hash map.
	AddGroup()
	// AddGroups adds N to the row count of hash map.
	AddGroups(uint64)
	// GroupCount returns the hash map's row count.
	GroupCount() uint64
	// Size returns the hash map's size
	Size() int64
	// PreAlloc admits capacity for at most n additional keys before mutation.
	PreAlloc(n uint64) error
	// MarshalBinary serializes the hash map into a byte slice.
	MarshalBinary() ([]byte, error)
	// UnmarshalBinary deserializes a byte slice into the hash map.
	UnmarshalBinary(data []byte, mp *mpool.MPool) error
	// WriteTo serializes the hash map to a writer.
	WriteTo(w io.Writer) (int64, error)
	// UnmarshalFrom deserializes a byte slice from a reader.
	UnmarshalFrom(r io.Reader, mp *mpool.MPool) (int64, error)
	// FillGroupHashes appends all group hash codes into dst and returns the result.
	FillGroupHashes(dst []uint64) []uint64
}

// Iterator allows users to do insert or find operations on hash tables in bulk.
type Iterator interface {
	// not safe for multi parallel!!!!
	// Insert vecs[start, start+count) into hashmap
	// vs  : the number of rows corresponding to each value in the hash table (start with 1)
	// zvs: 0 indicates a SQL NULL key and 1 indicates a non-NULL key.
	Insert(start, count int, vecs []*vector.Vector) (vs []uint64, zvs []int64, err error)

	// not safe for multi parallel!!!!
	// Insert a row from multiple columns into the hashmap, return true if it is new, otherwise false
	DetectDup(vecs []*vector.Vector, row int) (bool, error)

	//safe for multi parallel
	// Find vecs[start, start+count) in hashmap
	// vs  : the number of rows corresponding to each value in the hash table (start with 1, and 0 means not found.)
	// zvs: 0 indicates a SQL NULL key and 1 indicates a non-NULL key.
	Find(start, count int, vecs []*vector.Vector) (vs []uint64, zvs []int64, err error)
}

// TransactionalIterator is the opt-in hash publication protocol used by
// operators which must admit every fallible allocation before changing their
// resident state. Ordinary hashmap users keep the smaller Iterator contract
// and do not pay for an InsertPlan.
type TransactionalIterator interface {
	Iterator

	// Preflight reserves all fallible iterator scratch needed by the next
	// Insert/Find work unit without mutating the hash table.
	Preflight(start, count int, vecs []*vector.Vector) error
	// PreviewInsert computes the exact group mapping and first-insert selection
	// for one work unit without mutating the hash table.
	PreviewInsert(
		start, count int,
		vecs []*vector.Vector,
		groupCount uint64,
		plan *InsertPlan,
	) error
	// CommitPreview atomically publishes a valid plan. It may resize the table,
	// but does not encode or hash the input a second time when the preview still
	// matches the current table generation.
	CommitPreview(plan *InsertPlan) (vs []uint64, zvs []int64, err error)
}

// InsertPlan owns the fixed-size scratch and immutable outputs of one bounded
// transactional insert. Keeping it with the opting-in operator avoids adding
// scratch to every hashmap iterator in the system. Values stay in the owning
// iterator: the plan epoch prevents any other iterator operation between
// preview and commit, and a resize re-plan reconstructs the same mapping from
// the immutable hashes plus insertion flags.
type InsertPlan struct {
	count     int
	newGroups uint64
	base      uint64
	version   uint64
	epoch     uint64
	ready     bool
	complete  bool
	strOwner  *transactionalStrIterator
	intOwner  *transactionalIntIterator
	slots     [UnitLimit]uint64
	inserted  [UnitLimit]uint8
}

func (p *InsertPlan) Values() []uint64 {
	if p == nil || p.count < 0 || p.count > UnitLimit {
		return nil
	}
	if p.intOwner != nil {
		return p.intOwner.values[:p.count]
	}
	if p.strOwner != nil {
		return p.strOwner.values[:p.count]
	}
	return nil
}

func (p *InsertPlan) Inserted() []uint8 {
	if p == nil || p.count < 0 || p.count > UnitLimit {
		return nil
	}
	return p.inserted[:p.count]
}

func (p *InsertPlan) NewGroups() uint64 {
	if p == nil {
		return 0
	}
	return p.newGroups
}

func (p *InsertPlan) reset() {
	if p == nil {
		return
	}
	p.count = 0
	p.newGroups = 0
	p.base = 0
	p.version = 0
	p.epoch = 0
	p.ready = false
	p.complete = false
	p.strOwner = nil
	p.intOwner = nil
}

// IteratorAllocation selects exact physical provenance for data-scaled hash
// key encoding scratch. It is immutable and shared by iterators created from
// one map generation.
type IteratorAllocation struct {
	account *mpool.AllocationAccount
	owner   mpool.AllocationOwner
	site    mpool.AllocationSite
}

func NewIteratorAllocation(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	site mpool.AllocationSite,
) (*IteratorAllocation, error) {
	allocation := &IteratorAllocation{
		account: account,
		owner:   owner,
		site:    site,
	}
	if account == nil || account.Handle() == 0 ||
		owner < mpool.AllocationOwnerMin || owner > mpool.AllocationOwnerCatalogMax ||
		site < mpool.AllocationSiteMin {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return allocation, nil
}

// StrHashMap key is []byte, value is an uint64 value (starting from 1)
//
//	each time a new key is inserted, the hashtable returns a last-value+1 or, if the old key is inserted, the value corresponding to that key
type StrHashMap struct {
	hasNull            bool
	groupingAware      bool
	rejectNaN          bool
	rows               uint64
	hashMap            *hashtable.StringHashMap
	mp                 *mpool.MPool
	iteratorAllocation *IteratorAllocation
}

// IntHashMap key is int64, value is an uint64 (start from 1)
// before you use the IntHashMap, the user should make sure that
// sum of vectors' length equal to 8
type IntHashMap struct {
	hasNull   bool
	rejectNaN bool
	rows      uint64
	hashMap   *hashtable.Int64HashMap
}

type strHashmapIterator struct {
	mp                  *StrHashMap
	keys                [][]byte
	values              []uint64
	keyBuffer           []byte
	keyBufferMP         *mpool.MPool
	keyBufferAllocation *IteratorAllocation
	// zValues: 0 indicates a SQL NULL key and 1 indicates a non-NULL key.
	zValues       []int64
	nonMatching   []bool
	strHashStates [][3]uint64
}

type intHashMapIterator struct {
	mp          *IntHashMap
	keys        []uint64
	keyOffs     []uint32
	values      []uint64
	zValues     []int64
	nonMatching []bool
	hashes      []uint64
}

type transactionalStrIterator struct {
	*strHashmapIterator
	epoch uint64
}

type transactionalIntIterator struct {
	*intHashMapIterator
	epoch uint64
}
