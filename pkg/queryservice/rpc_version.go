// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func handleGetProtocolVersion(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.GetProtocolVersion == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	version, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		resp.WrapError(moerr.NewInternalError(ctx, "protocol version not found"))
		return nil
	}
	resp.GetProtocolVersion = &query.GetProtocolVersionResponse{
		Version: version.(int64),
	}
	return nil
}

func handleSetProtocolVersion(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.SetProtocolVersion == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.MOProtocolVersion, req.SetProtocolVersion.Version)
	resp.SetProtocolVersion = &query.SetProtocolVersionResponse{
		Version: req.SetProtocolVersion.Version,
	}
	return nil
}
