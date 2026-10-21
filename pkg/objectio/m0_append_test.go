//go:build mo_m0

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
	"github.com/stretchr/testify/require"
)

func m0TestLayout() M0AppendLayout {
	cols := make([]M0AppendColumn, 8)
	for i := 0; i < 6; i++ {
		cols[i] = M0AppendColumn{Seqnum: uint16(i), Type: types.T_int32}
	}
	cols[6] = M0AppendColumn{Seqnum: 6, Type: types.T_varchar, Nullable: false, Hidden: true, Primary: true, SortKey: true}
	cols[7] = M0AppendColumn{Seqnum: 7, Type: types.T_varchar, Nullable: true}
	return M0AppendLayout{
		Version: 1, NextColSeqnum: 8, PKSeqnum: 6, CompositePK: true, HiddenPK: true, SingleSortKey: true, Columns: cols,
	}
}

func m0WriteObject(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, version uint32, seqs []uint16) (string, Header, ObjectDataMeta) {
	t.Helper()
	bat := batch.NewWithSize(len(seqs))
	for i, seq := range seqs {
		ty := types.T_int32.ToType()
		if seq == 6 || seq == 7 {
			ty = types.T_varchar.ToType()
		}
		v := vector.NewVec(ty)
		bat.SetVector(int32(i), v)
		for row := 0; row < 2; row++ {
			if seq == 6 || seq == 7 {
				require.NoError(t, vector.AppendBytes(v, []byte{byte('a' + row)}, false, mp))
			} else {
				require.NoError(t, vector.AppendFixed(v, int32(row+1), false, mp))
			}
		}
	}
	bat.SetRowCount(2)
	defer bat.Clean(mp)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	w, err := NewObjectWriter(name, fs, version, seqs, nil)
	require.NoError(t, err)
	_, err = w.Write(bat)
	require.NoError(t, err)
	_, err = w.WriteEnd(context.Background())
	require.NoError(t, err)
	reader, err := NewObjectReaderWithStr(name.String(), fs)
	require.NoError(t, err)
	h, err := reader.ReadHeader(context.Background(), mp)
	require.NoError(t, err)
	meta, err := reader.ReadAllMeta(context.Background(), mp)
	require.NoError(t, err)
	return name.String(), h, meta.MustDataMeta()
}

func TestM0AppendObjectProofRejectsHolesAndReportsSuffixNull(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	layout := m0TestLayout()

	name, h, meta := m0WriteObject(t, fs, mp, 0, []uint16{0, 1, 2, 3, 4, 5, 6})
	width, err := ValidateM0Object(layout, h, meta)
	require.NoError(t, err)
	require.Equal(t, uint32(7), width)
	absent, err := ProveM0AbsentColumn(layout, h, meta, 7)
	require.NoError(t, err)
	require.True(t, absent)
	ioVec, err := ReadOneBlock(
		WithM0ProvenObject(context.Background(), M0ObjectProof{
			ObjectName:  name,
			WrittenSize: width,
			SchemaSize:  layout.NextColSeqnum,
		}), &meta, name, 0, []uint16{7},
		[]types.Type{types.T_varchar.ToType()}, mp, fs,
		fileservice.SkipAllCache,
	)
	require.NoError(t, err)
	defer ReleaseIOVector(&ioVec)
	vecAny, err := DecodeCached(ioVec.Entries[0].CachedData)
	require.NoError(t, err)
	vec := vecAny.(*vector.Vector)
	require.True(t, vec.IsConstNull())
	require.True(t, vec.HasNull())
	var materialized vector.Vector
	require.NoError(t, MustVectorToCachedWithMpool(&materialized, ioVec.Entries[0].CachedData, mp))
	defer materialized.Free(mp)
	require.True(t, materialized.IsConstNull())
	require.True(t, materialized.HasNull())

	_, h, meta = m0WriteObject(t, fs, mp, 0, []uint16{0, 1, 2, 4, 5, 6})
	_, err = ValidateM0Object(layout, h, meta)
	require.ErrorIs(t, err, ErrM0AppendLayout)
}

func TestM0AppendTransitionRejectsAnyIdentityOrTypeChange(t *testing.T) {
	before := m0TestLayout()
	after := before
	after.Columns = append(append([]M0AppendColumn(nil), before.Columns...),
		M0AppendColumn{Seqnum: 8, Type: types.T_int32, Nullable: true})
	after.NextColSeqnum = 9
	after.Version = 2
	require.NoError(t, ValidateM0AppendTransition(before, after))

	after.Columns[2].Type = types.T_int64
	require.Error(t, ValidateM0AppendTransition(before, after))
}
