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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// RequireArrowLoadEnabled checks the LOAD-only Arrow rollout gates. Planner
// callers use it before probing source objects; compile calls it again as a
// defense-in-depth check before constructing execution scopes.
func RequireArrowLoadEnabled(
	proc *process.Process,
	param *tree.ExternParam,
) (config.ArrowLoadParameters, error) {
	if param == nil || param.Format != tree.ARROW {
		return config.ArrowLoadParameters{}, nil
	}

	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	var parameterUnit *config.ParameterUnit
	if value := ctx.Value(config.ParameterUnitKey); value != nil {
		parameterUnit, _ = value.(*config.ParameterUnit)
	}
	if parameterUnit == nil && proc != nil {
		if runtime := moruntime.ServiceRuntime(proc.GetService()); runtime != nil {
			if value, ok := runtime.GetGlobalVariables("parameter-unit"); ok {
				parameterUnit, _ = value.(*config.ParameterUnit)
			}
		}
	}
	if parameterUnit == nil || parameterUnit.SV == nil {
		return config.ArrowLoadParameters{}, moerr.NewNotSupported(
			ctx, "Arrow LOAD is disabled because runtime configuration is unavailable",
		)
	}

	settings := parameterUnit.SV.ArrowLoad
	if !settings.Enabled {
		return settings, moerr.NewNotSupported(ctx, "Arrow LOAD is disabled by configuration")
	}
	if arrowLoadUsesS3(param) && !settings.S3Enabled {
		return settings, moerr.NewNotSupported(ctx, "Arrow LOAD from S3 or stage is disabled by configuration")
	}
	return settings, nil
}

func arrowLoadUsesS3(param *tree.ExternParam) bool {
	if param == nil {
		return false
	}
	if param.ScanType == tree.S3 {
		return true
	}
	if _, ok := param.FileService.(*fileservice.S3FS); ok {
		return true
	}
	parsed, err := fileservice.ParsePath(param.Filepath)
	if err != nil {
		return false
	}
	switch strings.ToLower(parsed.Service) {
	case "s3", "s3-no-key", "s3-opts", "opts", "options", "minio":
		return true
	}
	// A configured FileService can be named freely (for example, "archive")
	// while still being backed by S3. Resolve that service, including SubPath
	// wrappers, so the object-storage rollout gate cannot be bypassed by an
	// alias. An empty service is intentionally local in GetForETL.
	return parsed.Service != "" &&
		fileservice.IsS3BackedFileService(param.FileService, param.Filepath)
}
