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

package process

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
// repeated at I/O admission because prepared plans and Datalinks can outlive
// the statement that created them. Stage resolution and background execution
// do not confer authority over server-owned files.
func AuthorizeSQLFileAccess(proc *Process, moPath string) error {
	path, err := fileservice.ParsePath(moPath)
	if err != nil {
		return err
	}
	return AuthorizeSQLFileAccessPath(proc, path)
}

// AuthorizeSQLFileAccessPath is the parsed-path form used by SQL readers that
// must keep the authorization parse and ETL resolution on one path. Generic
// ETL callers pass the unchanged result of fileservice.ParsePath. Owners that
// always open local storage may construct a service-less Path for admission.
func AuthorizeSQLFileAccessPath(proc *Process, path fileservice.Path) error {

	// Keep this admission check aligned with GetForETL's provider selection so
	// an argumented disk backend cannot be classified as remote here while the
	// resolver creates a server-owned local filesystem at the I/O boundary.
	if path.Service != "" && len(path.ServiceArguments) > 0 &&
		!fileservice.PathUsesLocalETLBackend(path) {
		return nil
	}

	if proc != nil && proc.Base != nil {
		info := proc.GetSessionInfo()
		if strings.ToLower(info.Account) == systemAccountName &&
			strings.ToLower(info.Role) == moAdminRoleName {
			return nil
		}
	}

	return moerr.NewInternalErrorNoCtx("only the sys account moadmin role can access SQL file paths")
}
