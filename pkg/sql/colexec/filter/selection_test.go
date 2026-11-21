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

package filter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// rowsOf renders every column of bat's logical rows, reading through a
// selection when present.
func rowsOf(bat *batch.Batch) [][]string {
	physical := make([]int, bat.RowCount())
	for i := range physical {
		physical[i] = i
	}
	if bat.HasSelection() {
		for i, row := range bat.SelectionRows(nil) {
			physical[i] = int(row)
		}
	}
	out := make([][]string, len(physical))
	for i, row := range physical {
		for _, vec := range bat.Vecs {
			out[i] = append(out[i], vec.RowToString(row))
		}
	}
	return out
}

// runFilter drains the filter and returns every output batch's rows.
func runFilter(t *testing.T, arg *Filter, proc *process.Process, selected bool) ([][][]string, bool) {
	sawSelection := false
	var all [][][]string
	for {
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		if res.Batch.HasSelection() {
			sawSelection = true
			require.True(t, selected, "a compacting filter must not publish a selection")
			require.Equal(t, res.Batch.Selection().Count(), res.Batch.RowCount())
			require.Equal(t, res.Batch.Vecs[0].Length(), res.Batch.PhysicalRowCount())
		}
		all = append(all, rowsOf(res.Batch))
	}
	return all, sawSelection
}

func TestFilterSelectionOutputMatchesCompactedOutput(t *testing.T) {
	for i, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc)
		require.NoError(t, tc.arg.Prepare(tc.proc))
		tc.arg.SetSelectionOutput(false)
		want, _ := runFilter(t, tc.arg, tc.proc, false)
		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc)
		require.NoError(t, tc.arg.Prepare(tc.proc))
		tc.arg.SetSelectionOutput(true)
		got, sawSelection := runFilter(t, tc.arg, tc.proc, true)
		require.Equal(t, want, got, "case %d", i)
		require.True(t, sawSelection, "case %d keeps a strict subset of rows", i)

		for _, child := range tc.arg.Children {
			child.Reset(tc.proc, false, nil)
			child.Free(tc.proc, false, nil)
		}
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}
}

// A selected input is filtered on its selected rows only, and a compacting
// filter returns exactly those surviving rows as flat data.
func TestFilterSelectedInput(t *testing.T) {
	for _, output := range []bool{false, true} {
		tc := makeTestCases(t)[0] // a > 10
		proc := tc.proc
		source := MakeFilterMockBatchs(proc.Mp())
		physicalRows := source.RowCount()
		var selection bitmap.Bitmap
		selection.InitWithSize(int64(physicalRows))
		var selectedRows []int
		for row := 0; row < physicalRows; row += 3 {
			selection.Add(uint64(row))
			selectedRows = append(selectedRows, row)
		}
		var wantRows [][]string
		flat := rowsOf(source)
		values := vector.MustFixedColNoTypeCheck[int32](source.Vecs[0])
		for _, row := range selectedRows {
			if values[row] > 10 {
				wantRows = append(wantRows, flat[row])
			}
		}
		source.SetSelection(physicalRows, &selection)
		tc.arg.Children = nil
		tc.arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		require.NoError(t, tc.arg.Prepare(proc))
		tc.arg.SetSelectionOutput(output)

		got, sawSelection := runFilter(t, tc.arg, proc, output)
		require.Len(t, got, 1)
		require.Equal(t, wantRows, got[0])
		require.Equal(t, output, sawSelection)
		// The input's vectors and selection are not modified.
		require.Equal(t, physicalRows, source.Vecs[0].Length())

		for _, child := range tc.arg.Children {
			child.Reset(proc, false, nil)
			child.Free(proc, false, nil)
		}
		tc.arg.Reset(proc, false, nil)
		tc.arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}
}

// vm.Prepare configures the edge: a filter under a filter publishes a
// selection, the root filter compacts.
func TestFilterSelectionEdgeConfiguration(t *testing.T) {
	cases := makeTestCases(t)
	parent, child := cases[0].arg, cases[1].arg
	proc := cases[0].proc
	resetChildren(child, proc)
	parent.Children = nil
	parent.AppendChild(child)
	require.NoError(t, vm.Prepare(parent, proc))
	require.True(t, child.ctr.selection.output)
	require.False(t, parent.ctr.selection.output)
	vm.ConfigureSelectionEdges(parent)
	require.True(t, child.ctr.selection.output)
	require.False(t, parent.ctr.selection.output)
	for {
		res, err := vm.Exec(parent, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		require.False(t, res.Batch.HasSelection())
	}
	for _, c := range child.Children {
		c.Reset(proc, false, nil)
		c.Free(proc, false, nil)
	}
	child.Reset(proc, false, nil)
	child.Free(proc, false, nil)
	parent.Reset(proc, false, nil)
	parent.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
