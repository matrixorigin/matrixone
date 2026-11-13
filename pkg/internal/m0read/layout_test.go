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

package m0read

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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

func m0WriteObject(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, version uint32, seqs []uint16, blockCounts ...int) (string, objectio.Header, objectio.ObjectDataMeta) {
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
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	w, err := objectio.NewObjectWriter(name, fs, version, seqs, nil)
	require.NoError(t, err)
	blocks := 1
	if len(blockCounts) > 0 {
		blocks = blockCounts[0]
	}
	for range blocks {
		_, err = w.Write(bat)
		require.NoError(t, err)
	}
	_, err = w.WriteEnd(context.Background())
	require.NoError(t, err)
	reader, err := objectio.NewObjectReaderWithStr(name.String(), fs)
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
	_ = name // Engine materialization and physical NULL compatibility have separate consumer tests.

	_, h, meta = m0WriteObject(t, fs, mp, 0, []uint16{0, 1, 2, 4, 5, 6})
	_, err = ValidateM0Object(layout, h, meta)
	require.ErrorIs(t, err, ErrM0AppendLayout)
}

func TestM0AppendObjectProofRejectsConflictingSortKey(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	layout := m0TestLayout()
	_, h, meta := m0WriteObject(t, fs, mp, 1, []uint16{0, 1, 2, 3, 4, 5, 6, 7})
	meta.BlockHeader().SetSortKey(layout.PKSeqnum)
	_, err = ValidateM0Object(layout, h, meta)
	require.NoError(t, err)
	// Columns 6 and 7 share an OID; only the recorded key identifies the
	// contradiction. The shared LOAD SQL regression exercises this case.
	meta.BlockHeader().SetSortKey(7)
	_, err = ValidateM0Object(layout, h, meta)
	require.ErrorIs(t, err, ErrM0AppendLayout)
	// Old/unknown markers remain a separate compatibility-policy decision.
	meta.BlockHeader().SetSortKey(math.MaxUint16)
	_, err = ValidateM0Object(layout, h, meta)
	require.NoError(t, err)
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

func TestM0BlockScopedValidationDoesNotAuthorizeAnotherBlock(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(t.Context())
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	_, h, meta := m0WriteObject(t, fs, mp, 0, []uint16{0, 1, 2, 3, 4, 5, 6}, 2)
	require.Equal(t, uint32(2), meta.BlockCount())
	clear(meta.GetBlockMeta(1).MustGetColumn(0)) // synthetic corrupt unread block
	width, err := ValidateM0ReadBlock(m0TestLayout(), h, meta, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(7), width)
	_, err = ValidateM0ReadBlock(m0TestLayout(), h, meta, 1)
	require.ErrorIs(t, err, ErrM0AppendLayout)
	_, err = ValidateM0ReadBlock(m0TestLayout(), h, meta, 2)
	require.ErrorIs(t, err, ErrM0AppendLayout)
	_, err = ValidateM0Object(m0TestLayout(), h, meta)
	require.ErrorIs(t, err, ErrM0AppendLayout)
}
