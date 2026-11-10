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

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCMembershipOf(t *testing.T) {
	m, err := CMembershipOf(nil)
	require.NoError(t, err)
	require.Equal(t, CMembership{}, m)

	mp := mpool.MustNewZero()
	keys := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 9, 27}, nil)
	defer keys.Free(mp)

	// a C-built membership is accepted
	f, err := NewForC(keys, nil)
	require.NoError(t, err)
	m, err = CMembershipOf(f)
	require.NoError(t, err)
	require.Equal(t, f.(CFilter).CHandle(), m.Handle)
	f.Free()
	// a freed one is invalid: an error, not no membership
	_, err = CMembershipOf(f)
	require.ErrorContains(t, err, "invalid (non-nil) membership filter")

	// a Go-owned sorted64 is not a C membership
	payload, err := BuildSorted64Bytes(keys)
	require.NoError(t, err)
	gf, err := NewSorted64Filter(payload)
	require.NoError(t, err)
	defer gf.Free()
	_, err = CMembershipOf(gf)
	require.ErrorContains(t, err, "is not built in C memory")
}

// NewForC picks cbitmap first, then the smaller estimate of CRoaring and a C sorted64;
// the estimate is reserved before the build and released by the last Free.
func TestNewForCPicksByEstimate(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := func(n int, step int64) ([]int64, func()) {
		values := make([]int64, n)
		for i := range values {
			values[i] = 7 + int64(i)*step
		}
		return values, func() {}
	}
	for _, tc := range []struct {
		name string
		step int64
		kind byte
	}{
		{"dense", 1, TagCbitmap},
		{"one key every 100", 100, TagCRoaring},
		{"one key every 2^32", 1 << 32, TagSorted64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, _ := keys(10000, tc.step)
			v := buildIntVec(t, mp, types.T_int64.ToType(), values, nil)
			defer v.Free(mp)
			admission := &recordingMemoryAdmission{grant: true}
			f, err := NewForC(v, admission)
			require.NoError(t, err)
			cf, ok := f.(CFilter)
			require.True(t, ok)
			require.Equal(t, tc.kind, cf.CKind())
			require.NotNil(t, cf.CHandle())
			require.True(t, f.Exact())
			require.Len(t, admission.acquired, 1)
			require.Positive(t, admission.acquired[0])
			for i, value := range values {
				if i%997 == 0 {
					require.True(t, f.Test(rawInt64(value)), "key %d", value)
					if tc.step > 1 {
						require.False(t, f.Test(rawInt64(value+1)), "key %d", value+1)
					}
				}
			}
			require.False(t, f.Test(rawInt64(values[len(values)-1]+1)))
			require.False(t, f.Test(rawInt64(values[0]-1)))
			m, err := CMembershipOf(f)
			require.NoError(t, err)
			require.Equal(t, tc.kind, m.Kind)

			shared := f.Share()
			f.Free()
			require.Empty(t, admission.released, "a shared filter keeps its reservation")
			shared.Free()
			require.Equal(t, admission.acquired, admission.released)
		})
	}
}

// A denied reservation fails the build; nothing is allocated or kept.
func TestNewForCAdmissionDenied(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 1 << 40, 1 << 50}, nil)
	defer v.Free(mp)
	admission := &recordingMemoryAdmission{grant: false}
	_, err := NewForC(v, admission)
	var denied *MemoryAdmissionError
	require.ErrorAs(t, err, &denied)
	require.Empty(t, admission.released)
}

func rawInt64(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(uint64(v) >> (8 * i))
	}
	return b
}

// The cuVS mask takes only the three exact C memberships: a C bloom filter is an error.
func TestCExactMembershipOf(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 9, 1 << 40}, nil)
	defer keys.Free(mp)
	f, err := NewForC(keys, nil)
	require.NoError(t, err)
	defer f.Free()
	m, err := CExactMembershipOf(f)
	require.NoError(t, err)
	require.Equal(t, f.(CFilter).CKind(), m.Kind)

	words := vector.NewVec(types.T_varchar.ToType())
	defer words.Free(mp)
	require.NoError(t, vector.AppendBytes(words, []byte("k"), false, mp))
	payload, err := Build(words)
	require.NoError(t, err)
	bloom, err := New(payload)
	require.NoError(t, err)
	defer bloom.Free()
	_, err = CMembershipOf(bloom)
	require.NoError(t, err, "usearch accepts a C bloom filter")
	_, err = CExactMembershipOf(bloom)
	require.ErrorContains(t, err, "not a bloom filter")
}
