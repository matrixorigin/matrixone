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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestConcat(t *testing.T) {
	v1 := testutil.MakeVarcharVector([]string{"hello "}, []uint64{})
	v2 := testutil.MakeVarcharVector([]string{"world"}, []uint64{})

	inputVectors := []*vector.Vector{v1, v2}
	proc := testutil.NewProc()
	outputVector, err := Concat(inputVectors, proc)
	require.NoError(t, err)
	require.Equal(t, "hello world", outputVector.GetStringAt(0))
}
