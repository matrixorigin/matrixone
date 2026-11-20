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

package top

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestTopNullHeapRootNeverPublishes pins defect D2 of issue #32469: when the
// heap root's leading key is NULL, its raw slot bytes are not a comparable
// key and must never become a pruning bound. The hzb counterexample shape
// (an all-NULL first block crowding a populated second block out of an ASC
// heap) is simulated directly at the container boundary: a NULL root
// publishes nothing and the scan fails open, while a non-NULL root keeps
// the pre-existing publish behavior.
func TestTopNullHeapRootNeverPublishes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		varlen bool
	}{
		{name: "fixed width"},
		{name: "varlen", varlen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			typ := types.T_int64.ToType()
			if tc.varlen {
				typ = types.T_varbinary.ToType()
			}
			vec := vector.NewVec(typ)
			defer vec.Free(mp)
			if tc.varlen {
				require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
				require.NoError(t, vector.AppendBytes(vec, []byte("v"), false, mp))
			} else {
				require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
				require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp))
			}
			top := &Top{Fs: []*plan.OrderBySpec{{Expr: &plan.Expr{
				Typ: plan.Type{Id: int32(typ.Oid)},
			}}}}
			top.ctr = container{
				// getTopValue requires a full heap before it inspects the
				// root, so the root slot below is the heap's best row.
				limit:      1,
				poses:      []int32{0},
				sels:       []int64{0},
				cmps:       []compare.Compare{compare.NewOrder(typ, false, true)},
				topValueZM: objectio.NewZM(typ.Oid, 0),
			}
			top.ctr.cmps[0].Set(0, vec)
			require.True(t, vec.IsNull(0))
			require.False(t, top.updateTopValueZM(), "NULL heap root must publish nothing")
			require.False(t, top.ctr.topValueZM.IsInited(), "no bound may originate from a NULL root")

			top.ctr.sels = []int64{1}
			require.True(t, top.updateTopValueZM(), "a non-NULL heap root publishes as before")
			require.True(t, top.ctr.topValueZM.IsInited())
		})
	}
}
