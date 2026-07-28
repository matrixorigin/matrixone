// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func canExecPublicationInternalCmd(tenant *TenantInfo) bool {
	return tenant != nil && tenant.IsSysTenant()
}

func ensurePublicationInternalCmdAccess(ctx context.Context, ses FeSession, cmd string) error {
	if !ses.GetFromRealUser() {
		return nil
	}
	tenant := ses.GetTenantInfo()
	if canExecPublicationInternalCmd(tenant) {
		return nil
	}
	if tenant == nil {
		return moerr.NewInternalErrorf(ctx, "publication internal command %s requires sys tenant", cmd)
	}
	return moerr.NewInternalErrorf(
		ctx,
		"tenant %s user %s is not allowed to execute publication internal command %s",
		tenant.GetTenant(),
		tenant.GetUser(),
		cmd,
	)
}
