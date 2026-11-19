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

package batch

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// HasDictionary reports whether an explicit compatibility copy is necessary.
// Call it only at an edge whose producer can publish dictionary vectors.
func (bat *Batch) HasDictionary() bool {
	if bat != nil {
		for _, vec := range bat.Vecs {
			if vec.IsDictionary() {
				return true
			}
		}
	}
	return false
}

// FlatCopy makes an owned flat/constant logical copy for a consumer or codec
// that does not support dictionaries. It never changes the borrowed input.
// Passing nil explicitly chooses an unaccounted destination. The caller owns
// the result and must Clean it, including on early termination.
func (bat *Batch) FlatCopy(mp *mpool.MPool, allocation *vector.AllocationAccountSelection) (_ *Batch, retErr error) {
	if bat == nil || mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("flat batch copy requires a batch and pool")
	}
	result := NewOffHeapWithSize(len(bat.Vecs))
	defer func() {
		if retErr != nil {
			result.Clean(mp)
		}
	}()
	if err := result.SetAllocationAccount(allocation); err != nil {
		return nil, err
	}
	for i, src := range bat.Vecs {
		if src == nil {
			continue
		}
		dst, err := src.DupOffHeapWithAllocation(mp, allocation)
		if err != nil {
			return nil, err
		}
		src.CopyExpressionMetadataTo(dst)
		result.Vecs[i] = dst
	}
	if len(bat.ExtraBuf) > 0 {
		if allocation == nil {
			data, err := mp.Alloc(len(bat.ExtraBuf), true)
			if err != nil {
				return nil, err
			}
			result.ExtraBuf, result.extraBufMP = data, mp
			copy(data, bat.ExtraBuf)
		} else {
			buffer, err := allocation.NewAccountedBuffer(mp)
			if err != nil {
				return nil, err
			}
			defer buffer.Free()
			if _, err = buffer.Write(bat.ExtraBuf); err != nil {
				return nil, err
			}
			if err = result.SetAccountedExtraBuffer(buffer); err != nil {
				return nil, err
			}
		}
	}
	result.Attrs = append([]string(nil), bat.Attrs...)
	result.Recursive, result.ShuffleIDX, result.rowCount = bat.Recursive, bat.ShuffleIDX, bat.rowCount
	return result, nil
}
