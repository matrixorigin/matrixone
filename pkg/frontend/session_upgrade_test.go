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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type loginTenantUpgradeService struct {
	MockBaseService
	check func(context.Context, int64) error
}

func (s *loginTenantUpgradeService) GetFinalVersion() string { return "4.0.8" }

func (s *loginTenantUpgradeService) CheckTenantUpgrade(ctx context.Context, tenantID int64) error {
	return s.check(ctx, tenantID)
}

func TestSessionMaybeUpgradeTenant(t *testing.T) {
	for _, test := range []struct {
		name    string
		version string
		check   bool
		fail    bool
	}{
		{name: "older_account", version: "4.0.7", check: true},
		{name: "matching_hint_still_checks_catalog", version: "4.0.8", check: true},
		{name: "newer_account_on_old_cn", version: "4.0.9"},
		{name: "upgrade_failure_rejects_login", version: "4.0.7", check: true, fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			wantErr := moerr.NewInternalErrorNoCtx("tenant upgrade failed")
			base := &loginTenantUpgradeService{check: func(ctx context.Context, tenantID int64) error {
				calls++
				require.Equal(t, t.Context(), ctx)
				require.Equal(t, int64(11), tenantID)
				if test.fail {
					return wantErr
				}
				return nil
			}}
			ses := &Session{rm: &RoutineManager{baseService: base}}
			err := ses.MaybeUpgradeTenant(t.Context(), test.version, 11)
			if test.fail {
				require.ErrorIs(t, err, wantErr)
			} else {
				require.NoError(t, err)
			}
			if test.check {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}
