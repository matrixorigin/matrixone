// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

const rebaseDeltaKeyBatch = 64
const rebaseDeltaKeyScratchBytes = 256 << 10

// Integer-only keys make the bounded formatter and local duplicate check
// exact. Other admitted layouts retain the original SQL path before writes.
func rebaseIntegerKeyType(typ types.T) bool {
	switch typ {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	default:
		return false
	}
}

type rebaseDeltaKeys struct {
	store     databranchutils.BranchHashmap
	types     []types.Type
	resources dataBranchVisibleStateRecoveryResources
	mp        *mpool.MPool
	reserved  bool
}

func newRebaseDeltaKeys(allocator *branchHashmapAllocator, mp *mpool.MPool, keyTypes []types.Type) (*rebaseDeltaKeys, error) {
	if len(keyTypes) == 0 || len(keyTypes) > 16 || allocator == nil || mp == nil {
		return nil, moerr.NewInternalErrorNoCtx("invalid Rebase delta key storage")
	}
	for _, typ := range keyTypes {
		if !rebaseIntegerKeyType(typ.Oid) {
			return nil, moerr.NewInternalErrorNoCtx("unsupported Rebase delta key encoding")
		}
	}
	keys := &rebaseDeltaKeys{types: append([]types.Type(nil), keyTypes...), mp: mp,
		resources: dataBranchVisibleStateRecoveryResources{allocator: allocator}}
	if err := keys.resources.ReserveBuffer(rebaseDeltaKeyScratchBytes); err != nil {
		return nil, err
	}
	keys.reserved = true
	store, err := databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(allocator),
		databranchutils.WithBranchHashmapStrictCapacity(),
	)
	if err != nil {
		return nil, errors.Join(err, keys.close())
	}
	keys.store = store
	return keys, nil
}

func (keys *rebaseDeltaKeys) close() error {
	var err error
	if keys.store != nil {
		err = keys.store.Close()
		keys.store = nil
	}
	if keys.reserved {
		keys.resources.ReleaseBuffer(rebaseDeltaKeyScratchBytes)
		keys.reserved = false
	}
	return err
}

func (keys *rebaseDeltaKeys) add(ctx context.Context, input []*vector.Vector) error {
	if keys.store == nil || len(input) != len(keys.types) {
		return moerr.NewInternalError(ctx, "invalid Rebase delta key input")
	}
	if input[0] == nil {
		return moerr.NewInternalError(ctx, "Rebase delta key schema differs")
	}
	rows := input[0].Length()
	for i, vec := range input {
		if vec == nil || vec.Length() != rows || !vec.GetType().Eq(keys.types[i]) {
			return moerr.NewInternalError(ctx, "Rebase delta key schema differs")
		}
	}
	for start := 0; start < rows; start += rebaseDeltaKeyBatch {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(rows, start+rebaseDeltaKeyBatch)
		if err := keys.addWindow(input, start, end); err != nil {
			return err
		}
	}
	return nil
}

func (keys *rebaseDeltaKeys) addWindow(input []*vector.Vector, start, end int) error {
	windows := make([]*vector.Vector, len(input))
	selected := make([]*vector.Vector, len(input))
	defer func() {
		for _, vecs := range [][]*vector.Vector{windows, selected} {
			for _, vec := range vecs {
				if vec != nil {
					vec.Free(keys.mp)
				}
			}
		}
	}()
	for i, vec := range input {
		var err error
		windows[i], err = vec.Window(start, end)
		if err != nil {
			return err
		}
		selected[i] = vector.NewVec(keys.types[i])
	}
	found, err := keys.store.GetByVectors(windows)
	if err != nil {
		return err
	}
	if len(found) != end-start {
		return moerr.NewInternalErrorNoCtx("Rebase delta key probe count differs")
	}
	// This map is bounded by one 64-row window; keys use the typed tuple codec,
	// never concatenated SQL literals or Go's numeric/string equality.
	seen := make(map[string]struct{}, end-start)
	packer := types.NewPackerWithFixedBuffer(make([]byte, 512))
	defer packer.Close()
	for row, result := range found {
		if result.Exists {
			continue
		}
		packer.Reset()
		for _, vec := range windows {
			if vec.IsNull(uint64(row)) {
				return moerr.NewInternalErrorNoCtx("Rebase delta has a NULL primary key")
			}
			switch v := vector.GetAny(vec, row, false).(type) {
			case int8:
				packer.EncodeInt64(int64(v))
			case int16:
				packer.EncodeInt64(int64(v))
			case int32:
				packer.EncodeInt64(int64(v))
			case int64:
				packer.EncodeInt64(v)
			case uint8:
				packer.EncodeUint64(uint64(v))
			case uint16:
				packer.EncodeUint64(uint64(v))
			case uint32:
				packer.EncodeUint64(uint64(v))
			case uint64:
				packer.EncodeUint64(v)
			default:
				return moerr.NewInternalErrorNoCtx("unsupported Rebase delta key value")
			}
		}
		encoded := string(packer.GetBuf())
		if _, exists := seen[encoded]; exists {
			continue
		}
		seen[encoded] = struct{}{}
		for i, vec := range windows {
			if err = selected[i].UnionOne(vec, int64(row), keys.mp); err != nil {
				return err
			}
		}
	}
	indexes := make([]int, len(selected))
	for i := range indexes {
		indexes[i] = i
	}
	return keys.store.PutByVectors(selected, indexes)
}

func (keys *rebaseDeltaKeys) drain(ctx context.Context, ses *Session, consume func([][]string) error) error {
	if keys.store == nil || consume == nil {
		return moerr.NewInternalError(ctx, "invalid Rebase delta key consumer")
	}
	rows := make([][]string, 0, rebaseDeltaKeyBatch)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if err := consume(rows); err != nil {
			return err
		}
		clear(rows)
		rows = rows[:0]
		return nil
	}
	err := keys.store.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key, _ []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			result, err := cursor.PopByEncodedKey(key, true)
			if err != nil {
				return err
			}
			if !result.Exists {
				return nil
			}
			if len(result.Rows) != 1 {
				return moerr.NewInternalError(ctx, "Rebase delta key is not unique")
			}
			tuple, _, err := keys.store.DecodeRow(result.Rows[0])
			if err != nil {
				return err
			}
			if len(tuple) != len(keys.types) {
				return moerr.NewInternalError(ctx, "Rebase delta key tuple width differs")
			}
			values := make([]string, len(tuple))
			for i, value := range tuple {
				var buffer bytes.Buffer
				if err = formatValIntoString(ses, value, keys.types[i], &buffer); err != nil {
					return err
				}
				values[i] = buffer.String()
			}
			rows = append(rows, values)
			if len(rows) == rebaseDeltaKeyBatch {
				return flush()
			}
			return nil
		})
	}, 1)
	if err != nil {
		return err
	}
	return flush()
}
