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

package spillutil

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newExactTestSpillEngine(
	t testing.TB,
	cfg SpillEngineConfig,
) *SpillEngine {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	if cfg.Budget == nil {
		budget := process.MustNewExecutionResourceBudget(1<<60, 1<<60)
		cfg.Budget, err = budget.OpenGeneration(1)
		require.NoError(t, err)
	}
	account, err := registry.OpenWithController(1<<60, cfg.Budget)
	require.NoError(t, err)
	engine, err := NewSpillEngine(
		cfg,
		account,
		mpool.AllocationOwnerHashBuild,
	)
	require.NoError(t, err)
	return engine
}

func initTestSpillFiles(engine *SpillEngine, fds []*os.File, rows ...int64) {
	if len(rows) != len(fds) {
		panic("spill test file/row metadata mismatch")
	}
	files := make([]*message.SpillFile, len(fds))
	for i, fd := range fds {
		if fd != nil {
			files[i] = newTestSpillFile(fd, rows[i])
		}
	}
	engine.InitFromSpilledFiles(files)
}

func newTestSpillFile(fd *os.File, rows int64) *message.SpillFile {
	info, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	return newTestSpillFileWithSize(fd, rows, uint64(info.Size()), nil)
}

func newTestSpillFileWithSize(fd *os.File, rows int64, size uint64, release func()) *message.SpillFile {
	name := fd.Name()
	if err := fd.Close(); err != nil {
		panic(err)
	}
	return message.NewReopenableSpillFile(
		func(ctx context.Context) (*os.File, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return os.Open(name)
		},
		func() error { return os.Remove(name) }, rows, size, release,
	)
}

type testSpillRecordBuffer struct {
	bytes.Buffer
}

func (b *testSpillRecordBuffer) EnsureCapacity(required int) error {
	if b.Cap() < required {
		b.Buffer = *bytes.NewBuffer(make([]byte, 0, required))
	}
	return nil
}

func marshalTestSpillRecord(bat *batch.Batch) []byte {
	var encoded testSpillRecordBuffer
	if err := marshalSpillRecordTo(bat, &encoded); err != nil {
		panic(err)
	}
	return bytes.Clone(encoded.Bytes())
}
