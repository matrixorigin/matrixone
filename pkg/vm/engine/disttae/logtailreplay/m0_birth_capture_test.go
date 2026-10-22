// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0BirthLogtailReplayKeepsCapturedVersion(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	b := batch.New([]string{"__mo_rowid", "commit_ts", "id"})
	defer b.Clean(mp)
	b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	b.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(b.Vecs[0], types.BuildTestRowid(1, 1), false, mp))
	require.NoError(t, vector.AppendFixed(b.Vecs[1], types.BuildTS(5, 0), false, mp))
	require.NoError(t, vector.AppendFixed(b.Vecs[2], int64(42), false, mp))
	b.SetRowCount(1)
	wire, err := batch.BatchToProtoBatch(b)
	require.NoError(t, err)
	wire.LogtailWriteSchema = &api.LogtailWriteSchema{Version: 0, NextColSeqnum: 1}
	p := NewPartitionState("", false, 42, false)
	packer := types.NewPacker()
	defer packer.Close()
	p.HandleRowsInsert(context.Background(), wire, 0, packer, mp)
	wire.LogtailWriteSchema.Version = 99
	iter := p.NewRowsIter(types.BuildTS(10, 0), nil, false)
	defer iter.Close()
	require.True(t, iter.Next())
	entry := iter.Entry()
	require.Equal(t, uint32(0), entry.WriteSchema.Version)
	require.Equal(t, uint32(1), entry.WriteSchema.NextColSeqnum)
	require.NotSame(t, wire.LogtailWriteSchema, entry.WriteSchema)
}
