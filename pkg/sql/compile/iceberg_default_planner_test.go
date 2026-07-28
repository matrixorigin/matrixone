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

package compile

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

func TestRegisterDefaultIcebergScanPlanner(t *testing.T) {
	var params config.IcebergParameters
	params.SetDefaultValues()
	restoreRuntimeVariableForTest(t, "", IcebergScanPlannerRuntimeKey, nil)

	require.NoError(t, RegisterDefaultIcebergScanPlanner(context.Background(), "", params))
	value, ok := moruntime.ServiceRuntime("").GetGlobalVariables(IcebergScanPlannerRuntimeKey)
	require.True(t, ok)
	_, ok = value.(api.ScanPlanner)
	require.True(t, ok)
	value, ok = moruntime.ServiceRuntime("").GetGlobalVariables(IcebergTokenResolverRuntimeKey)
	require.True(t, ok)
	_, ok = value.(EnvIcebergTokenResolver)
	require.True(t, ok)
	value, ok = moruntime.ServiceRuntime("").GetGlobalVariables(api.CacheInvalidatorRuntimeKey)
	require.True(t, ok)
	_, ok = value.(api.CacheInvalidationHandler)
	require.True(t, ok)
}

func TestRuntimeIcebergTokenProviderUsesRuntimeResolver(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergTokenResolverRuntimeKey, testIcebergTokenResolver{
		token: " catalog-token ",
	})
	provider := runtimeIcebergTokenProvider{}

	token, err := provider.ResolveToken(context.Background(), model.Catalog{
		Name:           "prod",
		TokenSecretRef: "secret://iceberg/prod",
	})
	require.NoError(t, err)
	require.Equal(t, "catalog-token", token)
}

func TestRuntimeIcebergTokenProviderFailsClosedWithoutResolver(t *testing.T) {
	restoreRuntimeVariableForTest(t, "", IcebergTokenResolverRuntimeKey, nil)
	provider := runtimeIcebergTokenProvider{}

	_, err := provider.ResolveToken(context.Background(), model.Catalog{
		Name:           "prod",
		TokenSecretRef: "secret://iceberg/prod",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrAuthUnauthorized))
	require.Contains(t, err.Error(), "token resolver is not configured")
}

func TestRegisterDefaultIcebergScanPlannerKeepsExistingTokenResolver(t *testing.T) {
	var params config.IcebergParameters
	params.SetDefaultValues()
	existing := testIcebergTokenResolver{token: "catalog-token"}
	restoreRuntimeVariableForTest(t, "", IcebergScanPlannerRuntimeKey, nil)
	restoreRuntimeVariableForTest(t, "", IcebergTokenResolverRuntimeKey, existing)

	require.NoError(t, RegisterDefaultIcebergScanPlanner(context.Background(), "", params))
	value, ok := moruntime.ServiceRuntime("").GetGlobalVariables(IcebergTokenResolverRuntimeKey)
	require.True(t, ok)
	require.Equal(t, existing, value)
}

func TestNewDefaultIcebergScanPlannerUsesRuntimeRefCacheRefresher(t *testing.T) {
	var params config.IcebergParameters
	params.SetDefaultValues()
	refresher := testRefCacheRefresher{}
	restoreRuntimeVariableForTest(t, "", IcebergRefCacheRefresherRuntimeKey, refresher)

	planner, err := NewDefaultIcebergScanPlanner(context.Background(), "", params)
	require.NoError(t, err)
	runtimePlanner, ok := planner.(icebergmetadata.RuntimeScanPlanner)
	require.True(t, ok)
	_, ok = runtimePlanner.RefCacheRefresher.(testRefCacheRefresher)
	require.True(t, ok)
}

func TestNewDefaultIcebergScanPlannerUsesManifestCacheBytes(t *testing.T) {
	var params config.IcebergParameters
	params.SetDefaultValues()
	params.ManifestCacheBytes = 3

	planner, err := NewDefaultIcebergScanPlanner(context.Background(), "", params)
	require.NoError(t, err)
	runtimePlanner, ok := planner.(icebergmetadata.RuntimeScanPlanner)
	require.True(t, ok)
	key := icebergmetadata.CacheKey{Kind: icebergmetadata.CacheKindManifest, Table: "oversized"}
	runtimePlanner.Cache.Put(key, icebergmetadata.CacheEntry{SizeBytes: 4})
	require.Equal(t, 0, runtimePlanner.Cache.Len())
	runtimePlanner.Cache.Put(key, icebergmetadata.CacheEntry{SizeBytes: 3})
	require.Equal(t, 1, runtimePlanner.Cache.Len())
}

func TestIcebergAllowPlainHTTPFromEnv(t *testing.T) {
	t.Setenv(IcebergAllowPlainHTTPEnv, "")
	require.False(t, IcebergAllowPlainHTTPFromEnv())

	t.Setenv(IcebergAllowPlainHTTPEnv, "true")
	require.True(t, IcebergAllowPlainHTTPFromEnv())

	t.Setenv(IcebergAllowPlainHTTPEnv, "ON")
	require.True(t, IcebergAllowPlainHTTPFromEnv())
}

func TestEnvIcebergTokenResolver(t *testing.T) {
	resolver := EnvIcebergTokenResolver{
		LookupEnv: func(name string) (string, bool) {
			require.Equal(t, "MO_ICEBERG_TOKEN_ICEBERG_PROD_TOKEN", name)
			return " bearer-token ", true
		},
	}
	token, err := resolver.ResolveIcebergToken(context.Background(), "secret://iceberg/prod-token")
	require.NoError(t, err)
	require.Equal(t, "bearer-token", token)
}

func TestEnvIcebergTokenResolverFailsClosedWhenMissing(t *testing.T) {
	resolver := EnvIcebergTokenResolver{
		LookupEnv: func(name string) (string, bool) {
			return "", false
		},
	}
	_, err := resolver.ResolveIcebergToken(context.Background(), "secret://iceberg/prod-token")
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrAuthUnauthorized))
	require.Contains(t, err.Error(), "MO_ICEBERG_TOKEN_ICEBERG_PROD_TOKEN")
}

func TestIcebergTokenEnvNameRejectsInvalidRef(t *testing.T) {
	_, err := IcebergTokenEnvName(context.Background(), "file://iceberg/prod-token", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

func restoreRuntimeVariableForTest(t *testing.T, serviceID, key string, value any) {
	t.Helper()
	rt := moruntime.ServiceRuntime(serviceID)
	old, hadOld := rt.GetGlobalVariables(key)
	rt.SetGlobalVariables(key, value)
	t.Cleanup(func() {
		if hadOld {
			rt.SetGlobalVariables(key, old)
		} else {
			rt.SetGlobalVariables(key, nil)
		}
	})
}

type testIcebergTokenResolver struct {
	token string
}

func (r testIcebergTokenResolver) ResolveIcebergToken(ctx context.Context, secretRef string) (string, error) {
	return r.token, nil
}

type testRefCacheRefresher struct{}

func (testRefCacheRefresher) RefreshRefCache(ctx context.Context, refs []model.RefCache) error {
	return nil
}
