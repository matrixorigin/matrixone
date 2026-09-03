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

package analyze

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountMinMergeIsOrderIndependent(t *testing.T) {
	seed := sha256.Sum256([]byte("count min"))
	values := []ValueHash{HashValue([]byte("a")), HashValue([]byte("b")), HashValue([]byte("a"))}
	left, err := NewCountMin(seed, 3, 5, 64)
	require.NoError(t, err)
	right, err := NewCountMin(seed, 3, 5, 64)
	require.NoError(t, err)
	for _, value := range values[:2] {
		require.NoError(t, left.Add(value, 1))
	}
	require.NoError(t, right.Add(values[2], 1))

	forward, _ := NewCountMin(seed, 3, 5, 64)
	require.NoError(t, forward.Merge(left))
	require.NoError(t, forward.Merge(right))
	reverse, _ := NewCountMin(seed, 3, 5, 64)
	require.NoError(t, reverse.Merge(right))
	require.NoError(t, reverse.Merge(left))
	require.Equal(t, uint64(3), forward.Total())
	for _, value := range values {
		require.Equal(t, forward.Estimate(value), reverse.Estimate(value))
	}
	require.GreaterOrEqual(t, forward.Estimate(values[0]), uint64(2))
}

func TestOccurrenceReservoirKeepsGlobalKMinimumAcrossMergeOrder(t *testing.T) {
	seed := sha256.Sum256([]byte("reservoir"))
	first, err := NewOccurrenceReservoir(seed, 7, 8)
	require.NoError(t, err)
	second, err := NewOccurrenceReservoir(seed, 7, 8)
	require.NoError(t, err)
	all, err := NewOccurrenceReservoir(seed, 7, 8)
	require.NoError(t, err)

	for i := uint64(0); i < 40; i++ {
		identity := testRowIdentity(i)
		value := HashValue(identity[:])
		typed := identity[:]
		require.NoError(t, all.Add(identity, uint8(i%8), value, typed))
		if i%2 == 0 {
			require.NoError(t, first.Add(identity, uint8(i%8), value, typed))
		} else {
			require.NoError(t, second.Add(identity, uint8(i%8), value, typed))
		}
	}
	forward, _ := NewOccurrenceReservoir(seed, 7, 8)
	require.NoError(t, forward.Merge(first))
	require.NoError(t, forward.Merge(second))
	reverse, _ := NewOccurrenceReservoir(seed, 7, 8)
	require.NoError(t, reverse.Merge(second))
	require.NoError(t, reverse.Merge(first))
	require.Equal(t, all.Items(), forward.Items())
	require.Equal(t, forward.Items(), reverse.Items())

	identities := make([]RowIdentity, 0, len(forward.Items()))
	for _, item := range forward.Items() {
		identities = append(identities, item.RowIdentity)
	}
	require.Equal(t, len(identities), len(slices.Compact(identities)))
}

func TestOccurrenceReservoirDuplicateAndLargeValueContracts(t *testing.T) {
	seed := sha256.Sum256([]byte("reservoir duplicate"))
	r, err := NewOccurrenceReservoir(seed, 1, 2)
	require.NoError(t, err)
	identity := testRowIdentity(1)
	value := HashValue([]byte("value"))
	require.NoError(t, r.Add(identity, 0, value, make([]byte, MaxMaterializedValue+1)))
	require.NoError(t, r.Add(identity, 0, value, make([]byte, MaxMaterializedValue+1)))
	items := r.Items()
	require.Len(t, items, 1)
	require.True(t, items[0].ValueTooLarge)
	require.Empty(t, items[0].TypedValue)
	require.ErrorIs(t, r.Add(identity, 1, value, nil), ErrReservoirState)
}

func testRowIdentity(value uint64) RowIdentity {
	var identity RowIdentity
	binary.BigEndian.PutUint64(identity[len(identity)-8:], value)
	return identity
}
