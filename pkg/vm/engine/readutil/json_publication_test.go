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

package readutil

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

type jsonMemorySource struct{ readerOwnershipDataSource }

func (*jsonMemorySource) Next(ctx context.Context, _ []string, _ []types.Type, _ []uint16, _ int32, _ any, mp *mpool.MPool, output *batch.Batch) (*objectio.BlockInfo, engine.DataState, error) {
	if objectio.JSONReadSetFromContext(ctx) != nil {
		return nil, engine.InMem, io.ErrUnexpectedEOF
	}
	value, err := types.ParseStringToByteJson(`{"keep":1}`)
	if err != nil {
		return nil, engine.InMem, err
	}
	if err := vector.AppendByteJson(output.Vecs[0], value, false, mp); err != nil {
		return nil, engine.InMem, err
	}
	output.SetRowCount(1)
	return nil, engine.InMem, nil
}
func TestMemoryReaderPublishesJSONVersion(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	table := &plan.TableDef{Name: "t", Pkey: &plan.PrimaryKeyDef{PkeyColName: "doc", Names: []string{"doc"}}, Cols: []*plan.ColDef{{Name: "doc", Seqnum: 0, Typ: typ.PlanType()}}, Name2ColIndex: map[string]int32{"doc": 0}}
	reader, err := NewReader(context.Background(), mp, nil, nil, table, timestamp.Timestamp{}, nil, &jsonMemorySource{}, 0, engine.FilterHint{})
	require.NoError(t, err)
	defer reader.Close()
	output := batch.NewWithSize(1)
	output.Vecs[0] = vector.NewVec(typ)
	defer output.Clean(mp)
	publication := objectio.NewJSONReadSet([]int{0})
	defer publication.Close()
	ctx := objectio.WithJSONReadSet(context.Background(), publication)
	end, err := reader.Read(ctx, []string{"doc"}, nil, mp, output)
	require.NoError(t, err)
	require.False(t, end)
	version := publication.Readers()[0]
	require.NotNil(t, version)
	output.Clean(mp)
	path, err := types.ParseStringToPath("$.keep")
	require.NoError(t, err)
	result, err := version.Query(0, &path)
	require.NoError(t, err)
	defer result.Close()
	text, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
}
