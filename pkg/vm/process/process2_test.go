// Copyright 2024 Matrix Origin
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

package process

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPipelineContext(t *testing.T) {
	// Create a parent context
	parentCtx := context.Background()

	// Create a new process
	proc := &Process{
		Base: &BaseProcess{
			sqlContext: QueryBaseContext{
				outerContext: parentCtx,
			},
		},
	}

	// Build pipeline context
	pipelineCtx := proc.BuildPipelineContext(parentCtx)

	// Check if the pipeline context is not nil
	assert.NotNil(t, pipelineCtx)

	// Check if the pipeline context is different from the parent context
	assert.NotEqual(t, parentCtx, pipelineCtx)

	// Check if the process context is set correctly
	assert.Equal(t, pipelineCtx, proc.Ctx)

	// Check if the cancel function is set
	assert.NotNil(t, proc.Cancel)

	// Cancel the context and check if it is canceled
	proc.Cancel(nil)
	assert.Error(t, proc.Ctx.Err())
}

func TestGetTaskService(t *testing.T) {
	// Create a parent context
	parentCtx := context.Background()

	// Create a new process
	proc := &Process{
		Base: &BaseProcess{
			sqlContext: QueryBaseContext{
				outerContext: parentCtx,
			},
		},
	}

	assert.Nil(t, proc.GetTaskService())
}

func TestAffectedRows(t *testing.T) {
	// nil pointer is safe: Get returns 0, Set is a no-op.
	procNil := &Process{Base: &BaseProcess{}}
	assert.Equal(t, int64(0), procNil.GetAffectedRows())
	procNil.SetAffectedRows(5)
	assert.Equal(t, int64(0), procNil.GetAffectedRows())

	// with backing storage: round-trips, including the -1 sentinel.
	proc := &Process{Base: &BaseProcess{AffectedRows: new(int64)}}
	assert.Equal(t, int64(0), proc.GetAffectedRows())

	proc.SetAffectedRows(42)
	assert.Equal(t, int64(42), proc.GetAffectedRows())

	proc.SetAffectedRows(-1)
	assert.Equal(t, int64(-1), proc.GetAffectedRows())

	proc.SetAffectedRows(0)
	assert.Equal(t, int64(0), proc.GetAffectedRows())
}

func TestGetSpillFileService(t *testing.T) {
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	proc := &Process{
		Base: &BaseProcess{
			FileService: localFS,
		},
	}
	_, err = proc.GetSpillFileService()
	assert.Nil(t, err)
}

func TestGetSpillFileServiceError(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(
		"foo",
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	proc := &Process{
		Base: &BaseProcess{
			FileService: fs,
		},
	}
	_, err = proc.GetSpillFileService()
	assert.NotNil(t, err)
}

func TestOwnedPrepareParamsLifecycle(t *testing.T) {
	proc := &Process{Base: &BaseProcess{mp: mpool.MustNewZero()}}
	newParams := func(value string) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
		return params
	}

	first := newParams("first")
	proc.SetOwnedPrepareParamsWithIsBin(first, []bool{true})
	proc.SetPrepareParamsWithIsBin(first, []bool{false})
	require.Equal(t, 1, first.Length(), "setting the same pointer must not release it")
	require.True(t, proc.Base.prepareParamsOwned, "setting the same pointer must preserve ownership")

	borrowed := newParams("borrowed")
	proc.SetPrepareParams(borrowed)
	require.Zero(t, first.Length())
	require.Nil(t, first.GetData())
	require.Equal(t, 1, borrowed.Length())
	require.False(t, proc.Base.prepareParamsOwned)
	require.Nil(t, proc.Base.prepareParamsIsBin)

	second := newParams("second")
	proc.SetOwnedPrepareParamsWithIsBin(second, []bool{true})
	proc.Free()
	require.Zero(t, second.Length())
	require.Nil(t, second.GetData())
	require.Nil(t, proc.GetPrepareParams())
	require.Nil(t, proc.Base.prepareParamsIsBin)
	require.False(t, proc.Base.prepareParamsOwned)
	require.Equal(t, 1, borrowed.Length(), "Process must not release borrowed params")
	borrowed.Free(proc.Mp())
}

func TestDetachAndRestorePrepareParams(t *testing.T) {
	proc := &Process{Base: &BaseProcess{mp: mpool.MustNewZero()}}
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("binary"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithIsBin(params, []bool{true})

	state := proc.DetachPrepareParams()
	require.Nil(t, proc.GetPrepareParams())
	require.False(t, proc.GetPrepareParamIsBin(0))
	require.Equal(t, 1, params.Length(), "detach must not release owned params")

	proc.Free()
	require.Equal(t, 1, params.Length(), "transient Process.Free must not release detached params")

	proc.RestorePrepareParams(state)
	require.Same(t, params, proc.GetPrepareParams())
	require.True(t, proc.GetPrepareParamIsBin(0))
	require.True(t, proc.Base.prepareParamsOwned)

	proc.Free()
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
}
