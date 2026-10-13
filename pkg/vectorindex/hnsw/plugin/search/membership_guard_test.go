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

package search

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"github.com/stretchr/testify/require"
)

func membershipSpec() *plan.IndexSearchScan {
	spec := hnswSpec("idx")
	spec.SourceTableDef = &plan.TableDef{
		Name:          "t",
		Cols:          []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "v"}},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	return spec
}

func membershipPayload(t *testing.T, typ types.Type, appendKeys func(*vector.Vector)) []byte {
	proc := testutil.NewProc(t)
	v := vector.NewVec(typ)
	appendKeys(v)
	defer v.Free(proc.Mp())
	payload, err := v.MarshalBinary()
	require.NoError(t, err)
	return payload
}

func TestNewReaderMembership(t *testing.T) {
	proc := testutil.NewProc(t)
	req := f32Request([]float32{1, 2}, 10)

	// a required membership filter that is missing is an error
	r := req
	r.MembershipFilterRequired = true
	_, err := Hooks{}.NewReader(proc, membershipSpec(), r)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)

	// an exact empty key set returns no rows without searching
	r = req
	r.HasMembershipFilter, r.MembershipFilterRequired = true, true
	s, err := newSearcher(proc, membershipSpec(), r)
	require.NoError(t, err)
	require.True(t, s.emptyMembership)
	chunk, more, err := s.Next(context.Background())
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, chunk.Keys)
	require.NoError(t, s.Close())

	// int64 keys build an exact filter; Close frees it
	r.MembershipFilter = membershipPayload(t, types.T_int64.ToType(), func(v *vector.Vector) {
		for _, k := range []int64{3, 1, 2} {
			require.NoError(t, vector.AppendFixed(v, k, false, proc.Mp()))
		}
	})
	s, err = newSearcher(proc, membershipSpec(), r)
	require.NoError(t, err)
	require.NotNil(t, s.membership)
	require.Equal(t, uint64(3), s.membershipCount)
	require.NoError(t, s.Close())
	require.Nil(t, s.membership)

	// keys that do not match the primary key type are an error
	r.MembershipFilter = membershipPayload(t, types.T_varchar.ToType(), func(v *vector.Vector) {
		require.NoError(t, vector.AppendBytes(v, []byte("a"), false, proc.Mp()))
	})
	_, err = newSearcher(proc, membershipSpec(), r)
	require.Error(t, err)

	// a spec without a primary key is an error
	spec := membershipSpec()
	spec.SourceTableDef.Pkey = nil
	r.MembershipFilter = membershipPayload(t, types.T_int64.ToType(), func(v *vector.Vector) {
		require.NoError(t, vector.AppendFixed(v, int64(1), false, proc.Mp()))
	})
	_, err = newSearcher(proc, spec, r)
	require.Error(t, err)

	require.True(t, Hooks{}.SupportsMembership())

	// the iterative scan option makes the membership search widen
	r = f32Request([]float32{1, 2}, 10)
	s, err = newSearcher(proc, membershipSpec(), r)
	require.NoError(t, err)
	require.False(t, s.iterative)
	spec = membershipSpec()
	spec.AlgoOptions, err = hnswplan.EncodeScanOptions(hnswplan.ScanOptions{Iterative: true})
	require.NoError(t, err)
	s, err = newSearcher(proc, spec, r)
	require.NoError(t, err)
	require.True(t, s.iterative)
}
