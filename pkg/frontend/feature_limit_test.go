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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func newFeatureLimitTestSession(t *testing.T) *Session {
	t.Helper()

	proc := testutil.NewProcess(t)
	service := "feature-limit-" + t.Name()
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{
		SV:          &config.FrontendParameters{},
		FileService: proc.Base.FileService,
	})

	return &Session{
		feSessionImpl: feSessionImpl{service: service},
		proc:          proc,
	}
}

func TestCheckBranchQuotaLocksFiniteQuota(t *testing.T) {
	const (
		accountName = "account-a"
		accountID   = uint32(42)
	)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: accountName, TenantID: accountID})

	bh := &backgroundExecTest{}
	bh.init()

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	lockedQuotaSQL := quotaSQL + " for update"
	countSQL := fmt.Sprintf(
		"select count(*) from %s.%s where creator = %d and table_deleted = false for update",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, accountID,
	)

	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{{int64(0)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL, lockedQuotaSQL, countSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaUsesLockedQuota(t *testing.T) {
	const accountID = uint32(42)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account-a", TenantID: accountID})

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	lockedQuotaSQL := quotaSQL + " for update"

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(-1)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL, lockedQuotaSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaDoesNotLockUnlimitedQuota(t *testing.T) {
	const accountID = uint32(42)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account-a", TenantID: accountID})

	bh := &backgroundExecTest{}
	bh.init()
	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(-1)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL}, bh.executedSQLs)
}
