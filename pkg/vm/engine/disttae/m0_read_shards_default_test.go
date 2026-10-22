//go:build !mo_m0

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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestM0ReadShardsDefaultKeepsContiguousSplit(t *testing.T) {
	data := readutil.NewBlockListRelationData(0)
	for i := 1; i <= 8; i++ {
		data.AppendBlockInfo(&objectio.BlockInfo{PartitionIdx: int32(i)})
	}
	got := activeM0ReadShards(context.Background(), nil, data, 2, false)
	want := data.Split(2)
	for i := range want {
		require.Equal(t, want[i].GetBlockInfoSlice(), got[i].GetBlockInfoSlice())
	}
}

func TestM0DefaultHooksDoNotGrantAppendAuthority(t *testing.T) {
	require.False(t, activeM0Enabled)
	defs, err := activeM0CNAddColumn(nil, context.Background(), nil)
	require.Nil(t, defs)
	require.ErrorContains(t, err, "not enabled")
	proven, proof, err := activeM0ReadObject(context.Background(), nil, objectio.ObjectEntry{}, nil)
	require.NoError(t, err)
	require.False(t, proven)
	require.Zero(t, proof)
}
