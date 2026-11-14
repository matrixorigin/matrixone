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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceHandoffOutputNames(t *testing.T) {
	proc := testutil.NewProc(t)
	out := makeS3OutputBatch()
	defer out.Clean(proc.Mp())
	var expected []string
	for _, attrs := range [][]string{{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}, {catalog.ObjectMeta_ObjectStats}} {
		inner := batch.NewOffHeapWithSize(len(attrs))
		inner.Attrs = attrs
		for i := range inner.Vecs {
			inner.Vecs[i] = vector.NewOffHeapVecWithType(types.T_binary.ToType())
		}
		name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
		stats := objectio.NewObjectStatsWithObjectID(name.ObjectId(), false, false, true)
		if len(attrs) == 2 {
			require.NoError(t, vector.AppendBytes(inner.Vecs[0], []byte("block"), false, proc.Mp()))
		}
		require.NoError(t, vector.AppendBytes(inner.Vecs[len(attrs)-1], stats[:], false, proc.Mp()))
		inner.SetRowCount(1)
		encoded, err := inner.MarshalBinary()
		require.NoError(t, err)
		inner.Clean(proc.Mp())
		require.NoError(t, vector.AppendBytes(out.Vecs[4], encoded, false, proc.Mp()))
		expected = append(expected, name.String())
	}
	out.SetRowCount(2)
	names, err := workspaceHandoffOutputNames(proc, out)
	require.NoError(t, err)
	require.Equal(t, expected, names)
}
