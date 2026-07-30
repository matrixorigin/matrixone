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

package write

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type AppendExecutor interface {
	CommitAppend(ctx context.Context, req api.AppendRequest) (*api.CommitResult, error)
}

type AppendExecutorFunc func(context.Context, api.AppendRequest) (*api.CommitResult, error)

func (f AppendExecutorFunc) CommitAppend(ctx context.Context, req api.AppendRequest) (*api.CommitResult, error) {
	return f(ctx, req)
}

type AppendEntrypointResult struct {
	Request api.AppendRequest
	Commit  api.CommitResult
}

func ExecuteExistingMappingCTAS(ctx context.Context, executor AppendExecutor, spec AppendRequestSpec) (*AppendEntrypointResult, error) {
	return executeAppendEntrypoint(ctx, executor, spec, BuildExistingMappingCTASAppendRequest)
}

func ExecuteSinkPublish(ctx context.Context, executor AppendExecutor, spec AppendRequestSpec) (*AppendEntrypointResult, error) {
	return executeAppendEntrypoint(ctx, executor, spec, BuildSinkAppendRequest)
}

func ExecuteMOIPublish(ctx context.Context, executor AppendExecutor, spec AppendRequestSpec) (*AppendEntrypointResult, error) {
	return executeAppendEntrypoint(ctx, executor, spec, BuildMOIPublishAppendRequest)
}

func executeAppendEntrypoint(
	ctx context.Context,
	executor AppendExecutor,
	spec AppendRequestSpec,
	build func(context.Context, AppendRequestSpec) (api.AppendRequest, error),
) (*AppendEntrypointResult, error) {
	if executor == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append entrypoint requires an executor", nil)
	}
	req, err := build(ctx, spec)
	if err != nil {
		return nil, err
	}
	result, err := executor.CommitAppend(ctx, req)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, api.NewError(api.ErrInternal, "Iceberg append executor returned nil result", nil)
	}
	return &AppendEntrypointResult{
		Request: req,
		Commit:  *result,
	}, nil
}
