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

package filter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// BenchmarkFilterBlock filters 8192-row batches with a > 10 (45% kept, one
// row in 13 NULL), compacting the output (flat) or publishing a selection.
func BenchmarkFilterBlockFlat(b *testing.B)     { benchFilterBlock(b, false) }
func BenchmarkFilterBlockSelected(b *testing.B) { benchFilterBlock(b, true) }

func benchFilterBlock(b *testing.B, selected bool) {
	const rows = 8192
	tc := makeTestCases(b)[0] // a > 10
	proc := tc.proc
	mp := proc.Mp()
	bat := batch.New([]string{"a", "b", "c"})
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < rows; i++ {
		require.NoError(b, vector.AppendFixed(bat.Vecs[0], int32(i%20), i%13 == 0, mp))
		require.NoError(b, vector.AppendFixed(bat.Vecs[1], int64(i), false, mp))
		require.NoError(b, vector.AppendBytes(bat.Vecs[2], []byte("value"), false, mp))
	}
	bat.SetRowCount(rows)
	inputs := make([]*batch.Batch, b.N)
	for i := range inputs {
		inputs[i] = bat
	}
	tc.arg.Children = nil
	tc.arg.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(b, tc.arg.Prepare(proc))
	tc.arg.SetSelectionOutput(selected)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := vm.Exec(tc.arg, proc)
		if err != nil {
			b.Fatal(err)
		}
		if res.Batch == nil || res.Batch.RowCount() == 0 {
			b.Fatal("no rows")
		}
	}
}
