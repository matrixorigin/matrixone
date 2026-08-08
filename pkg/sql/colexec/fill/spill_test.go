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

package fill

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFillSpillRoundTripPreservesPrepareParamKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	spill, err := newFillSpill(proc)
	require.NoError(t, err)
	defer spill.close(proc)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, proc.Mp()))
	bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamFloat)
	bat.SetRowCount(1)
	require.NoError(t, spill.writeRecord(spill.input, bat))
	bat.Clean(proc.Mp())

	pos := int64(-1)
	got, err := readRecordReverse(spill.input, &pos, proc.Mp(), nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, got.Vecs[0].GetPrepareParamKindAt(0))
	got.Clean(proc.Mp())
}

func TestFillSpillRoundTripPreservesHeterogeneousPrepareParamKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	spill, err := newFillSpill(proc)
	require.NoError(t, err)
	defer spill.close(proc)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, proc.Mp()))
	}
	bat.Vecs[0].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	bat.SetRowCount(2)
	require.NoError(t, spill.writeRecord(spill.input, bat))
	bat.Clean(proc.Mp())

	pos := int64(-1)
	got, err := readRecordReverse(spill.input, &pos, proc.Mp(), nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, got.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamNone, got.Vecs[0].GetPrepareParamKindAt(1))
	got.Clean(proc.Mp())
}
