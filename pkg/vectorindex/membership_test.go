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

package vectorindex

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type recordingAdmission struct {
	grant              bool
	acquired, released []int64
}

func (a *recordingAdmission) Acquire(bytes int64) (int64, bool) {
	a.acquired = append(a.acquired, bytes)
	return 0, a.grant
}

func (a *recordingAdmission) Release(bytes int64) int64 {
	a.released = append(a.released, bytes)
	return 0
}

func membershipOf(t *testing.T, keys []int64) docfilter.MembershipFilter {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	for _, k := range keys {
		require.NoError(t, vector.AppendFixed(v, k, false, mp))
	}
	f, err := docfilter.NewForC(v, nil)
	require.NoError(t, err)
	return f
}

func TestAdmitFilterMask(t *testing.T) {
	const largeRows = uint64(16 << 20) // a 2 MiB mask and a 2 MiB delete copy
	cbitmap := membershipOf(t, []int64{1, 2, 3, 4})
	defer cbitmap.Free()
	sparse := membershipOf(t, []int64{1, 1 << 40, 1 << 50})
	defer sparse.Free()
	require.Equal(t, docfilter.TagCbitmap, cbitmap.(docfilter.CFilter).CKind())
	require.NotEqual(t, docfilter.TagCbitmap, sparse.(docfilter.CFilter).CKind())

	for _, tc := range []struct {
		name string
		rt   RuntimeConfig
		rows uint64
		want int64 // 0: nothing reserved
	}{
		{"no filter", RuntimeConfig{}, largeRows, 0},
		{"small mask", RuntimeConfig{FilterJSON: "[]"}, 1 << 20, 0},
		{"just below the threshold", RuntimeConfig{FilterJSON: "[]"}, 4<<20 - 64, 0},
		{"at the threshold", RuntimeConfig{FilterJSON: "[]"}, 4 << 20, 1 << 20},
		{"predicates only", RuntimeConfig{FilterJSON: "[]"}, largeRows, int64(largeRows / 4)},
		{"cbitmap membership: mask only", RuntimeConfig{Membership: cbitmap}, largeRows, int64(largeRows / 4)},
		{"sparse membership: mask only", RuntimeConfig{Membership: sparse}, largeRows, int64(largeRows / 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admission := &recordingAdmission{grant: true}
			release, err := AdmitFilterMask(admission, tc.rt, tc.rows)
			require.NoError(t, err)
			require.NotNil(t, release)
			if tc.want == 0 {
				require.Empty(t, admission.acquired)
			} else {
				require.Equal(t, []int64{tc.want}, admission.acquired)
			}
			release()
			require.Equal(t, admission.acquired, admission.released)
		})
	}

	_, err := AdmitFilterMask(&recordingAdmission{grant: false}, RuntimeConfig{Membership: sparse}, largeRows)
	var denied *docfilter.MemoryAdmissionError
	require.ErrorAs(t, err, &denied)
}
