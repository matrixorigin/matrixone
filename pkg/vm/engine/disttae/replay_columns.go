// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type replayColumnKey struct {
	account         uint32
	database, table uint64
	name            []byte
}

// key is read before the timestamp is inserted (rowid is the only prefix).
func replayKey(b *batch.Batch, row, offset int) replayColumnKey {
	return replayColumnKey{
		vector.GetFixedAtNoTypeCheck[uint32](b.Vecs[offset+catalog.MO_COLUMNS_ACCOUNT_ID_IDX], row),
		vector.GetFixedAtNoTypeCheck[uint64](b.Vecs[offset+catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX], row),
		vector.GetFixedAtNoTypeCheck[uint64](b.Vecs[offset+catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX], row),
		b.Vecs[offset+catalog.MO_COLUMNS_ATT_RELNAME_IDX].GetBytesAt(row),
	}
}
func replayKeyCompare(a, b replayColumnKey) int {
	if a.account < b.account {
		return -1
	}
	if a.account > b.account {
		return 1
	}
	if a.database < b.database {
		return -1
	}
	if a.database > b.database {
		return 1
	}
	if a.table < b.table {
		return -1
	}
	if a.table > b.table {
		return 1
	}
	return 0
}

func replayKeyEqual(a, b replayColumnKey) bool {
	return replayKeyCompare(a, b) == 0 && bytes.Equal(a.name, b.name)
}

// consumeReplayColumns owns every batch in bs, including on error. Windows borrow
// from a source batch and never survive its cleanup; carry owns only one table.
func consumeReplayColumns(ctx context.Context, bs []*batch.Batch, mp *mpool.MPool, ts types.TS, insert func(*batch.Batch)) error {
	return consumeReplayColumnsWithFaults(ctx, bs, mp, ts, insert, nil, nil)
}

// copyWindow is scoped to one invocation and permits testing a partially copied carry.
func consumeReplayColumnsWithFaults(ctx context.Context, bs []*batch.Batch, mp *mpool.MPool, ts types.TS, insert func(*batch.Batch), copyWindow func(*batch.Batch, *batch.Batch, int, int, *mpool.MPool) error, appendTS func(*vector.Vector, types.TS, *mpool.MPool) error) error {
	copyCarry := func(dst, src *batch.Batch, offset, count int) error {
		if copyWindow != nil {
			return copyWindow(dst, src, offset, count, mp)
		}
		return dst.UnionWindow(src, offset, count, mp)
	}
	next := 0
	defer func() {
		for _, b := range bs[next:] {
			if b != nil {
				b.Clean(mp)
			}
		}
	}()
	var prev replayColumnKey
	have := false
	for _, b := range bs {
		if b == nil {
			return moerr.NewInternalError(ctx, "nil replay columns batch")
		}
		for i := 0; i < b.RowCount(); i++ {
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			k := replayKey(b, i, 1)
			if have {
				cmp := replayKeyCompare(prev, k)
				if cmp > 0 || (cmp == 0 && !bytes.Equal(prev.name, k.name)) {
					return moerr.NewInternalError(ctx, "invalid replay columns order or table name")
				}
			}
			prev, have = k, true
		}
	}
	var carry *batch.Batch
	defer func() {
		if carry != nil {
			carry.Clean(mp)
		}
	}()
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if carry != nil {
			insert(carry)
			carry.Clean(mp)
			carry = nil
		}
		return nil
	}
	for i, b := range bs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b.RowCount() == 0 {
			b.Clean(mp)
			next = i + 1
			continue
		}
		if err := fillTsVecForSysTableQueryBatchWithAppend(b, ts, mp, appendTS); err != nil {
			return err
		}
		rows := b.RowCount()
		start := 0
		if carry != nil {
			key := replayKey(b, 0, 2)
			end := 1
			for end < rows && replayKeyEqual(replayKey(b, end, 2), key) {
				end++
				if end%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
			}
			if err := copyCarry(carry, b, 0, end); err != nil {
				return err
			}
			start = end
			if end < rows {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		// If the last table continues in a later nonempty batch, retain only its tail.
		tail := rows
		var following *batch.Batch
		for j := i + 1; j < len(bs); j++ {
			if bs[j].RowCount() > 0 {
				following = bs[j]
				break
			}
		}
		if start == rows {
			if following == nil || !replayKeyEqual(replayKey(b, rows-1, 2), replayKey(following, 0, 1)) {
				if err := flush(); err != nil {
					return err
				}
			}
			b.Clean(mp)
			next = i + 1
			continue
		}
		if following != nil && replayKeyEqual(replayKey(b, rows-1, 2), replayKey(following, 0, 1)) {
			k := replayKey(b, rows-1, 2)
			tail = rows - 1
			for tail > start && replayKeyEqual(replayKey(b, tail-1, 2), k) {
				tail--
				if tail%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
			}
		}
		if start < tail {
			w, err := b.Window(start, tail)
			if err != nil {
				return err
			}
			insert(w)
			w.Clean(nil)
		}
		if tail < rows {
			if carry == nil {
				carry = batch.NewWithSize(len(b.Vecs))
				carry.Attrs = b.Attrs
				for j, v := range b.Vecs {
					carry.Vecs[j] = vector.NewVec(*v.GetType())
				}
			}
			if err := copyCarry(carry, b, tail, rows-tail); err != nil {
				return err
			}
		} else if carry != nil {
			if err := flush(); err != nil {
				return err
			}
		}
		b.Clean(mp)
		next = i + 1
	}
	return flush()
}
