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
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// AuthorizeSQLFileAccess admits the SQL file targets that are intentionally
// backed by a caller-supplied remote service.  A path without a service would
// make GetForETL create a local filesystem rooted at the caller's path.  A
// named service would select an already configured server-side filesystem.
// Both are server-owned resources and require the same system moadmin
// identity used by other server-control SQL functions.
//
// Planning preflight shares fileservice resource classification. Actual acquisition
// independently enforces the statement policy at the ETL factory.
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
	var account, role string
	if proc != nil && proc.Base != nil {
		info := proc.GetSessionInfo()
		account, role = info.Account, info.Role
	}
	return fileservice.AuthorizeSQLFileAccessPath(account, role, path)
}

// SQLFileAccessContext snapshots the current statement identity on any request context.
func SQLFileAccessContext(ctx context.Context, proc *Process) context.Context {
	var account, role string
	if proc != nil && proc.Base != nil {
		info := proc.GetSessionInfo()
		account, role = info.Account, info.Role
	}
	return fileservice.WithSQLFileAccess(ctx, account, role)
}
