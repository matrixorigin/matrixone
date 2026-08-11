// Copyright 2024 Matrix Origin
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

package aggexec

import (
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type distinctHash struct {
	mp   *mpool.MPool
	maps []*hashmap.StrHashMap
	itrs []hashmap.Iterator

	// optimized for bulk and batch insertions.
	bs  []bool
	bs1 []bool
}

func newDistinctHash(mp *mpool.MPool) distinctHash {
	return distinctHash{
		mp:   mp,
		maps: nil,
		itrs: nil,
	}
}

func (d *distinctHash) grows(more int) error {
	oldLen, newLen := len(d.maps), len(d.maps)+more
	d.maps = append(d.maps, make([]*hashmap.StrHashMap, more)...)
	d.itrs = append(d.itrs, make([]hashmap.Iterator, more)...)

	var err error
	for i := oldLen; i < newLen; i++ {
		if d.maps[i], err = hashmap.NewStrHashMap(true, d.mp); err != nil {
			return err
		}
		d.itrs[i] = d.maps[i].NewIterator()
	}
	return nil
}

// fill inserts the row into the hash map.
// return true if this is a new value.
func (d *distinctHash) fill(group int, vs []*vector.Vector, row int) (bool, error) {
	return d.itrs[group].DetectDup(vs, row)
}

// merge was the method to merge two groups of distinct agg.
// but distinct agg should be run in only one node and without any parallel.
// because the distinct agg need to store all the source data to make sure the result is correct if we use parallel.
// there is one simple example that:
//
//	select count(distinct a) from t;
//	and `a` is a column with 1, 2, 3, 3, 5
//	if we use parallel, and the data is split into two parts: [1, 2, 3] and [3, 5].
//	once we do the merge, we will get the result 5 from (3 + 2), but the correct result should be 4 from (3 + 1).
//	we need to loop the [3, 5] to do a new data fill to make sure the result is correct, but not do 3 + 2.
//
// this action to store all the source data is very expensive.
//
// I add this check to make sure the distinct agg is not used in parallel.
func (d *distinctHash) merge(next *distinctHash) error {
	if len(d.maps) > 0 || len(next.maps) > 0 {
		return moerr.NewInternalErrorNoCtx("distinct agg should be run in only one node and without any parallel")
	}
	return nil
}

func (d *distinctHash) free() {
	for _, m := range d.maps {
		if m != nil {
			m.Free()
		}
	}
	d.maps = nil
	d.itrs = nil
	d.bs = nil
	d.bs1 = nil
}

func (d *distinctHash) Size() int64 {
	var size int64
	for _, m := range d.maps {
		if m != nil {
			size += m.Size()
		}
	}
	// 8 is the size of a pointer.
	size += int64(cap(d.maps)) * 8
	// 16 is the size of an interface.
	size += int64(cap(d.itrs)) * 16
	size += int64(cap(d.bs))
	size += int64(cap(d.bs1))
	return size
}

func (d *distinctHash) marshalToBuffers(flags []uint8, writer io.Writer) error {
	if flags != nil && len(flags) != len(d.maps) {
		return moerr.NewInvalidInputNoCtxf(
			"distinct selection length %d does not match state count %d",
			len(flags), len(d.maps))
	}
	var cnt int64
	if flags == nil {
		cnt = int64(len(d.maps))
	} else {
		for _, f := range flags {
			if f != 0 {
				cnt += 1
			}
		}
	}

	if err := types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	for i := range d.maps {
		if flags == nil || flags[i] != 0 {
			if _, err := d.maps[i].WriteTo(writer); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *distinctHash) unmarshalFromReader(
	buf io.Reader,
	mp *mpool.MPool,
) (retErr error) {
	n, err := types.ReadUint64(buf)
	if err != nil {
		return err
	}

	maxInt := int(^uint(0) >> 1)
	if n > uint64(maxInt/24) {
		return mpool.ErrAllocationAllocatorLimit
	}
	// Every map starts with an eight-byte payload length. For complete-memory
	// readers, reject impossible counts before a corrupt record can turn a few
	// bytes into a data-scaled Go allocation.
	if sized, ok := buf.(interface{ Len() int }); ok &&
		n > uint64(sized.Len()/8) {
		return io.ErrUnexpectedEOF
	}
	d.free()
	defer func() {
		if retErr != nil {
			d.free()
		}
	}()
	d.maps = make([]*hashmap.StrHashMap, int(n))
	d.itrs = make([]hashmap.Iterator, int(n))
	for i := uint64(0); i < n; i++ {
		l, err := types.ReadUint64(buf)
		if err != nil {
			return err
		}
		if l > uint64(^uint64(0)>>1) {
			return mpool.ErrAllocationAllocatorLimit
		}
		limited := &io.LimitedReader{R: buf, N: int64(l)}
		d.maps[i] = &hashmap.StrHashMap{}
		_, err = d.maps[i].UnmarshalFrom(limited, mp)
		if err != nil {
			return err
		}
		if trailing, err := io.Copy(io.Discard, limited); err != nil || trailing != 0 {
			if err != nil {
				return err
			}
			return moerr.NewInternalErrorNoCtx("distinct hash payload has trailing bytes")
		}
		if limited.N != 0 {
			return io.ErrUnexpectedEOF
		}
		d.itrs[i] = d.maps[i].NewIterator()
	}
	return nil
}
