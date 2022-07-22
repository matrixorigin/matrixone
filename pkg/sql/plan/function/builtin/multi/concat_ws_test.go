// Copyright 2021 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConcat_ws(t *testing.T) {
	vector0 := testutil.MakeScalarVarchar("---", 4)
	vector1 := testutil.MakeVarcharVector([]string{"hello", "", "guten", "guten"}, []uint64{1})
	vector2 := testutil.MakeVarcharVector([]string{"hello", "", "guten", "guten"}, []uint64{1})
	inputVectors := []*vector.Vector{vector0, vector1, vector2}
	proc := testutil.NewProc()
	outputVector, err := Concat_ws(inputVectors, proc)
	require.NoError(t, err)
	outputBytes := outputVector.Col.(*types.Bytes)
	require.Equal(t, []byte("hello---helloguten---gutenguten---guten"), outputBytes.Data)
	require.Equal(t, []uint32{13, 0, 13, 13}, outputBytes.Lengths)
	require.Equal(t, []uint32{0, 13, 13, 26}, outputBytes.Offsets)
}
