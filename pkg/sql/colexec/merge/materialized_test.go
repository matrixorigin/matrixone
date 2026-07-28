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

package merge_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func TestMaterializedSinkScanConsumersAdvanceIndependently(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	inputs := make([]*batch.Batch, 4)
	for i := range inputs {
		inputs[i] = batch.NewWithSize(1)
		inputs[i].Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(inputs[i].Vecs[0], int64(i), false, proc.Mp()))
		inputs[i].SetRowCount(1)
	}
	inputMemory := proc.Mp().CurrNB()

	producer := dispatch.NewArgument()
	defer producer.Release()
	producer.FuncId = dispatch.SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, producer.Prepare(proc))
	for {
		result, err := producer.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, source.CurrentBytes(), producer.GetOperatorBase().OpAnalyzer.GetOpStats().MemorySize)
	producer.Reset(proc, false, nil)

	readers := []*merge.Merge{merge.NewArgument(), merge.NewArgument()}
	defer readers[0].Release()
	defer readers[1].Release()
	for i, reader := range readers {
		reader.SinkScan = true
		reader.MaterializedSource = source
		reader.MaterializedReaderID = i
		require.NoError(t, reader.Prepare(proc))
	}

	// The second consumer completes before the first consumer starts. Streaming
	// fan-out cannot make progress in this topology once its bounded spool fills.
	for _, reader := range []*merge.Merge{readers[1], readers[0]} {
		for i := range inputs {
			result, err := reader.Call(proc)
			require.NoError(t, err)
			require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
		}
		result, err := reader.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		reader.Reset(proc, false, nil)
	}

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	for _, bat := range inputs {
		bat.Clean(proc.Mp())
	}
}

func TestMaterializedSinkScanConcurrentProductionReaders(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	inputs := make([]*batch.Batch, 32)
	for i := range inputs {
		inputs[i] = batch.NewWithSize(1)
		inputs[i].Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(inputs[i].Vecs[0], int64(i), false, proc.Mp()))
		inputs[i].SetRowCount(1)
	}
	inputMemory := proc.Mp().CurrNB()

	producer := dispatch.NewArgument()
	defer producer.Release()
	producer.FuncId = dispatch.SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, producer.Prepare(proc))

	readers := []*merge.Merge{merge.NewArgument(), merge.NewArgument()}
	defer readers[0].Release()
	defer readers[1].Release()
	for i, reader := range readers {
		reader.SinkScan = true
		reader.MaterializedSource = source
		reader.MaterializedReaderID = i
		require.NoError(t, reader.Prepare(proc))
	}

	errCh := make(chan error, len(readers))
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range inputs {
				result, err := reader.Call(proc)
				if err != nil {
					errCh <- err
					return
				}
				if got := vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0); got != int64(i) {
					errCh <- moerr.NewInternalErrorNoCtxf("unexpected value %d at %d", got, i)
					return
				}
			}
			result, err := reader.Call(proc)
			if err != nil {
				errCh <- err
				return
			}
			if result.Status != vm.ExecStop {
				errCh <- moerr.NewInternalErrorNoCtx("materialized reader did not stop")
			}
		}()
	}

	for {
		result, err := producer.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	producer.Reset(proc, false, nil)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	for _, reader := range readers {
		reader.Reset(proc, false, nil)
	}

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	for _, bat := range inputs {
		bat.Clean(proc.Mp())
	}
}
