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

package datalink

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	systemAccountName = "sys"
	moAdminRoleName   = "moadmin"
)

// AuthorizeSQLFileAccess admits the SQL file targets that are intentionally
// backed by a caller-supplied remote service.  A path without a service would
// make GetForETL create a local filesystem rooted at the caller's path.  A
// named service would select an already configured server-side filesystem.
// Both are server-owned resources and require the same system moadmin
// identity used by other server-control SQL functions.
//
// Supported argumented remote services are resolved by GetForETL (and retain
// its validation), so this helper does not duplicate remote credentials
// policy.  An argumented path that selects the process-local disk backend is
// still server-owned and follows the local-path rule. The check is
// deliberately repeated at the actual I/O boundary by
// Datalink.NewReadCloser/NewWriter because a Datalink can outlive planning.
func AuthorizeSQLFileAccess(proc *process.Process, moPath string) error {
	path, err := fileservice.ParsePath(moPath)
	if err != nil {
		return err
	}
	return authorizeSQLFileAccessPath(proc, path)
}

func authorizeSQLFileAccessPath(proc *process.Process, path fileservice.Path) error {
	return authorizeSQLFileAccessPathWithOrigin(proc, path, false)
}

// AuthorizeSQLFileAccessPath is the parsed-path form used by SQL readers that
// must keep the authorization parse and ETL resolution on one path. Callers
// must pass the result of fileservice.ParsePath without changing it.
func AuthorizeSQLFileAccessPath(proc *process.Process, path fileservice.Path) error {
	return authorizeSQLFileAccessPath(proc, path)
}

func authorizeSQLFileAccessPathWithOrigin(proc *process.Process, path fileservice.Path, fromStage bool) error {
	if path.File == "" {
		return moerr.NewFileNotFoundNoCtx(path.File)
	}

	// Keep this admission check aligned with GetForETL's provider selection so
	// an argumented disk backend cannot be classified as remote here while the
	// resolver creates a server-owned local filesystem at the I/O boundary.
	if path.Service != "" && len(path.ServiceArguments) > 0 &&
		!fileservice.PathUsesLocalETLBackend(path) {
		return nil
	}

	// A fulltext2 CDC task is trusted internal SQL, but it resolves a tenant-owned
	// stage:// value to the stage's concrete local path before this boundary. Keep
	// that asynchronous workflow working without treating background execution as
	// a blanket host-file grant: the origin must be a stage URL and the process
	// must be an internal, non-frontend execution.
	if fromStage && isTrustedInternalStageExecution(proc) {
		return nil
	}

	if proc != nil && proc.Base != nil {
		info := proc.GetSessionInfo()
		if info != nil &&
			strings.EqualFold(info.Account, systemAccountName) &&
			strings.EqualFold(info.Role, moAdminRoleName) {
			return nil
		}
	}

	return moerr.NewInternalErrorNoCtx("only the sys account moadmin role can access SQL file paths")
}

func isTrustedInternalStageExecution(proc *process.Process) bool {
	return proc != nil &&
		proc.Base != nil &&
		!proc.Base.IsFrontend &&
		proc.Ctx != nil &&
		defines.IsInternalExecutor(proc.Ctx)
}
