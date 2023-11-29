package queryservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func handleGetProtocolVersion(ctx context.Context, req *query.Request, resp *query.Response) error {
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

func handleSetProtocolVersion(ctx context.Context, req *query.Request, resp *query.Response) error {
	if req.SetProtocolVersion == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.MOProtocolVersion, req.SetProtocolVersion.Version)
	resp.SetProtocolVersion = &query.SetProtocolVersionResponse{
		Version: req.SetProtocolVersion.Version,
	}
	return nil
}
