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

package vector

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestRollbackAppendRejectsBeforeMutation(t *testing.T) {
	for _, name := range []string{"length", "area", "negative", "overflow", "prepared", "string", "binary"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_varchar.ToType())
			defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()); require.Zero(t, mp.OnHeapCurrNB()) }()
			require.NoError(t, AppendBytes(v, []byte(strings.Repeat("a", 64)), false, mp))
			v.SetSorted(true)
			cp := v.MakeAppendCheckpoint()
			attempted := 1
			switch name {
			case "length":
				cp.length++
			case "area":
				cp.areaLength++
			case "negative":
				attempted = -1
			case "overflow":
				attempted = math.MaxInt
			case "prepared":
				cp.hadPrepareParamKinds = true
			case "string":
				cp.hadStringSources = true
			case "binary":
				cp.hadBinaryStringRows = true
			}
			err := v.RollbackAppend(cp, attempted)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
			require.False(t, mpool.IsRetryableAllocationCapacity(errors.Join(mpool.ErrAllocationAccountCapacity, err)))
			require.Equal(t, 1, v.Length())
			require.Equal(t, 64, len(v.GetArea()))
			require.True(t, v.GetSorted())
			require.Equal(t, strings.Repeat("a", 64), v.GetStringAt(0))
		})
	}
}

func TestAppendAllocatorLimitReturnsErrorAndKeepsPrefix(t *testing.T) {
	old := mpool.CapLimit
	mpool.CapLimit = 128
	t.Cleanup(func() { mpool.CapLimit = old })
	mp := mpool.MustNewZero()
	v := NewVec(types.T_varchar.ToType())
	defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()); require.Zero(t, mp.OnHeapCurrNB()) }()
	first := strings.Repeat("a", 64)
	require.NoError(t, AppendBytes(v, []byte(first), false, mp))
	err := AppendBytes(v, []byte(strings.Repeat("b", 64)), false, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	require.NotErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Equal(t, 1, v.Length())
	require.Equal(t, first, v.GetStringAt(0))
}
