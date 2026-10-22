// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func batchProofTable() *plan.TableDef {
	key := &plan.ColDef{Name: "__mo_cpkey_col", ColId: 3, Seqnum: 2, Hidden: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}}
	state := testBirthFillState()
	state.BaseWidth = 3
	state.PkSeqnum = 2
	for i, c := range state.Columns {
		c.Seqnum = uint32(i + 3)
	}
	return &plan.TableDef{TblId: 42, Version: 2, Pkey: &plan.PrimaryKeyDef{CompPkeyCol: key}, AppendMissingState: state,
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "b", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}}, key,
			{Name: "flag", ColId: 4, Seqnum: 3, Typ: plan.Type{Id: int32(types.T_bool)}, Default: &plan.Default{NullAbility: true}},
			{Name: "nullable", ColId: 5, Seqnum: 4, Typ: plan.Type{Id: int32(types.T_bool)}, Default: &plan.Default{NullAbility: true}},
			{Name: "__mo_rowid", ColId: 6, Seqnum: 5, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		}}
}

func TestM0BatchProofRequiresCapturedCompletePrefix(t *testing.T) {
	table := batchProofTable()
	attrs := []string{"__mo_rowid", "commit_ts", "a", "b", "__mo_cpkey_col"}
	vecs := []*vector.Vector{nil, nil, vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_varchar.ToType())}
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	proof, err := NewM0BatchProof(table, attrs, vecs, 0, 3, 2)
	require.NoError(t, err)
	require.True(t, proof.MatchesReadSchema(table))
	table.TblId++
	require.False(t, proof.MatchesReadSchema(table))
	table.TblId--
	out := vector.NewVec(types.T_bool.ToType())
	defer out.Free(mp)
	require.NoError(t, proof.AppendFill(out, 3, mp))
	require.False(t, out.IsNull(0))
	require.False(t, vector.GetFixedAtNoTypeCheck[bool](out, 0))
	require.NoError(t, proof.AppendFill(out, 4, mp))
	require.True(t, out.IsNull(1))
	require.Error(t, proof.AppendFill(out, 2, mp))
	for _, version := range []uint32{1, 2, 99} {
		_, err = NewM0BatchProof(table, attrs, vecs, version, 3, 2)
		require.Error(t, err, "partial new batch must not masquerade as an old batch")
	}
	attrs[3] = ""
	_, err = NewM0BatchProof(table, attrs, vecs, 0, 3, 2)
	require.Error(t, err)
	attrs[3] = "b"
	vecs[3] = nil
	_, err = NewM0BatchProof(table, attrs, vecs, 0, 3, 2)
	require.Error(t, err)
}
