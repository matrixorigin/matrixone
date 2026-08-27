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

package frontend

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestLifecycleFreshSystemBootstrapSQLs(t *testing.T) {
	sqls := lifecycleSystemBootstrapSQLs()
	require.Len(t, sqls, len(catalog.LifecycleClusterTableDefinitions)+1)
	require.Contains(t, sqls, catalog.MoLifecycleCleanupRootsDDL)
	require.Contains(t, sqls, MoCatalogLifecycleFeatureRegistryInitData)

	featureSQL := strings.ToLower(MoCatalogLifecycleFeatureRegistryInitData)
	require.Contains(t, featureSQL, "'lifecycle'")
	require.Contains(t, featureSQL, "'tae object lifecycle retirement'")
	require.Contains(t, featureSQL, "'{\"archive_stages\":[]}'")
	require.Contains(t, featureSQL, "false")
	require.Contains(t, featureSQL, "on duplicate key update description = description")
	require.NotContains(t, featureSQL, "on duplicate key update feature_code")

	// System-owned Cleanup Roots must not be created by the per-tenant path.
	require.NotContains(t, createSqls, catalog.MoLifecycleCleanupRootsDDL)
}
