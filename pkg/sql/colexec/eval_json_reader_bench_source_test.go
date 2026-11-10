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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func jsonProjectionResidentBenchmarkEvaluator(b *testing.B, proc *process.Process, projection *Projection, input *batch.Batch) func() (*batch.Batch, error) {
	admission, err := function.JSONReaderMemoryAdmission(proc)
	require.NoError(b, err)
	reader, err := vector.NewJSONReaderSnapshotWithAllocation(input.Vecs[0], proc.Mp(), nil, admission)
	require.NoError(b, err)
	b.Cleanup(reader.Close)
	readers := map[int]vector.JSONReader{0: reader}
	return func() (*batch.Batch, error) { return projection.EvalProjectionWithJSONReaders(input, proc, readers) }
}
