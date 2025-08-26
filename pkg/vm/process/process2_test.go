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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
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
