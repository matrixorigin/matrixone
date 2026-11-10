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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONInputPublicationReleasesPartialVersions(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := batch.NewWithSize(2)
	defer input.Clean(proc.Mp())
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.T_json.ToType())
		doc, err := types.ParseStringToByteJson(`{"keep":1}`)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[i], doc, false, proc.Mp()))
	}
	input.SetRowCount(1)
	input.Vecs[1].GetBytesAt(0)[0] = 0xfd
	before := proc.Mp().CurrNB()
	readers, err := PublishJSONInputSnapshots(proc, input, []int{0, 1})
	require.Error(t, err)
	require.Nil(t, readers)
	require.Equal(t, before, proc.Mp().CurrNB())
	readers, err = PublishJSONInputSnapshotsForEvaluation(proc, input, []int{0, 1, 1})
	require.NoError(t, err)
	require.NotNil(t, readers[0])
	declined, exists := readers[1]
	require.True(t, exists)
	require.Nil(t, declined, "malformed input must never publish a reader")
	CloseJSONInputVersions(readers)
	require.Equal(t, before, proc.Mp().CurrNB())
	require.NoError(t, vector.SetBytesAt(input.Vecs[1], 0, input.Vecs[0].GetBytesAt(0), proc.Mp()))
	readers, err = PublishJSONInputSnapshots(proc, input, []int{0, 0, 1})
	require.NoError(t, err)
	require.Len(t, readers, 2)
	CloseJSONInputVersions(readers)
	require.Equal(t, before, proc.Mp().CurrNB())
}
