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

func TestMissingNormalColumnRequiresProof(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(1)
	bat.SetVector(0, vector.NewVec(types.T_int32.ToType()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int32{1, 2, 3}, nil, mp))
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 1, []uint16{0}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)

	reader, err := NewObjectReaderWithStr(name.String(), fs)
	require.NoError(t, err)
	meta, err := reader.ReadAllMeta(ctx, mp)
	require.NoError(t, err)
	dataMeta := meta.MustDataMeta()
	typs := []types.Type{types.T_int32.ToType()}

	_, err = ReadOneBlock(
		ctx, &dataMeta, name.String(), 0, []uint16{1}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.ErrorIs(t, err, ErrMissingColumnWithoutProof)

	ioVec, err := ReadOneBlock(
		WithM0ProvenObject(ctx, M0ObjectProof{
			ObjectName:  name.String(),
			WrittenSize: 1,
			SchemaSize:  2,
		}), &dataMeta, name.String(), 0, []uint16{1}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.NoError(t, err)
	defer ReleaseIOVector(&ioVec)
	vecAny, err := DecodeCached(ioVec.Entries[0].CachedData)
	require.NoError(t, err)
	vec := vecAny.(*vector.Vector)
	require.True(t, vec.IsConstNull())

	_, err = ReadOneBlock(
		WithM0ProvenObject(ctx, M0ObjectProof{
			ObjectName:  "different-object",
			WrittenSize: 1,
			SchemaSize:  2,
		}), &dataMeta, name.String(), 0, []uint16{1}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.ErrorIs(t, err, ErrMissingColumnWithoutProof)

	_, err = ReadOneBlock(
		WithM0ProvenObject(ctx, M0ObjectProof{
			ObjectName:  name.String(),
			WrittenSize: 2,
			SchemaSize:  3,
		}), &dataMeta, name.String(), 0, []uint16{1}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.ErrorIs(t, err, ErrMissingColumnWithoutProof)

	_, err = ReadOneBlock(
		WithM0ProvenObject(ctx, M0ObjectProof{
			ObjectName:  name.String(),
			WrittenSize: 1,
			SchemaSize:  2,
		}), &dataMeta, name.String(), 0, []uint16{2}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.ErrorIs(t, err, ErrMissingColumnWithoutProof)

	legacy, err := ReadOneBlock(
		WithLegacyMissingColumnProof(ctx, name.String(), 1),
		&dataMeta, name.String(), 0, []uint16{1}, typs, mp, fs,
		fileservice.SkipAllCache,
	)
	require.NoError(t, err)
	defer ReleaseIOVector(&legacy)
}
