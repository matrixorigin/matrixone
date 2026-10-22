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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func testBirthFillState() *plan.AppendMissingState {
	s := &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 1, PkSeqnum: 0}
	for i, kind := range []plan.MissingFillKind{plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, plan.MissingFillKind_MISSING_FILL_NULL_V1} {
		s.Columns = append(s.Columns, &plan.ColumnBirthFill{Seqnum: uint32(i + 1), BirthAppendOrdinal: uint32(i + 1), Type: plan.Type{Id: int32(types.T_bool)}, Kind: kind})
	}
	return s
}

func TestM0BirthProofScopeAndOwnership(t *testing.T) {
	state := testBirthFillState()
	proof, err := NewM0ObjectProof("object", 1, 3, 42, 2, state)
	require.NoError(t, err)
	table := &plan.TableDef{TblId: 42, Version: 2, AppendMissingState: plan.CloneAppendMissingState(state)}
	require.True(t, proof.MatchesReadSchema(table))
	table.TblId = 43
	require.False(t, proof.MatchesReadSchema(table), "same object in another clone needs its own proof")
	table.TblId = 42
	table.Version = 1
	require.False(t, proof.MatchesReadSchema(table))
	table.Version = 2
	table.AppendMissingState = nil
	require.False(t, proof.MatchesReadSchema(table))
	state.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
	state.LayoutId[0] = 'x'
	ctx := WithM0ProvenObject(context.Background(), proof)
	kind, ok := MissingColumnFill(ctx, "object", 1)
	require.True(t, ok)
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, kind)
	require.False(t, CanFillMissingColumn(ctx, "object", 0))
	require.False(t, CanFillMissingColumn(ctx, "different", 1))
	for _, tc := range []struct {
		width, size uint32
		tid         uint64
		version     uint32
	}{
		{0, 3, 42, 2}, {1, 4, 42, 2}, {1, 3, 0, 2}, {1, 3, 42, 1},
	} {
		_, err := NewM0ObjectProof("object", tc.width, tc.size, tc.tid, tc.version, testBirthFillState())
		require.Error(t, err)
	}
}

func TestM0BirthFillReadsFalseAndPreservesStoredNull(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], true, false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], false, true, mp))
	input.SetRowCount(2)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	_, err = writer.Write(input)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)
	reader, err := NewObjectReaderWithStr(name.String(), fs)
	require.NoError(t, err)
	meta, err := reader.ReadAllMeta(ctx, mp)
	require.NoError(t, err)
	data := meta.MustDataMeta()
	proof, err := NewM0ObjectProof(name.String(), 1, 3, 42, 2, testBirthFillState())
	require.NoError(t, err)
	proofCtx := WithM0ProvenObject(ctx, proof)
	ioVec, err := ReadOneBlock(proofCtx, &data, name.String(), 0, []uint16{0, 1, 2},
		[]types.Type{types.T_bool.ToType(), types.T_bool.ToType(), types.T_bool.ToType()}, mp, fs, fileservice.SkipAllCache)
	require.NoError(t, err)
	defer ReleaseIOVector(&ioVec)
	for i := range ioVec.Entries {
		decoded, err := DecodeCached(ioVec.Entries[i].CachedData)
		require.NoError(t, err)
		v := decoded.(*vector.Vector)
		require.Equal(t, 2, v.Length())
		switch i {
		case 0:
			require.True(t, vector.GetFixedAtNoTypeCheck[bool](v, 0))
			require.True(t, v.IsNull(1))
		case 1:
			require.False(t, v.IsConstNull())
			require.False(t, v.IsNull(0))
			require.False(t, vector.GetFixedAtNoTypeCheck[bool](v, 0))
		case 2:
			require.True(t, v.IsConstNull())
		}
	}
	// The first generated payload is freed before a later bad type aborts.
	before := mp.CurrNB()
	_, err = ReadOneBlock(proofCtx, &data, name.String(), 0, []uint16{1, 1},
		[]types.Type{types.T_bool.ToType(), types.T_int32.ToType()}, mp, fs, fileservice.SkipAllCache)
	require.ErrorIs(t, err, ErrMissingColumnWithoutProof)
	require.Equal(t, before, mp.CurrNB())
}
