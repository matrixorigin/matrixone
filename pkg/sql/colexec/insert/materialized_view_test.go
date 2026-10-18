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

package insert

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewWriterBudget(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.WithMaterializedViewBudget(proc.Ctx, 99, 1)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(10), false, proc.Mp()))
	bat.SetRowCount(1)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	op := &Insert{delegated: true, input: vm.CallResult{Batch: bat}, InsertCtx: &InsertCtx{TableDef: &plan.TableDef{TblId: 99}}}
	// An uninitialized writer would panic if execution reached native Write.
	_, err := op.insert_table(proc, process.NewTempAnalyzer())
	require.ErrorContains(t, err, "output budget exceeded")
}
