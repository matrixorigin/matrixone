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

package lifecycle

import (
	"encoding/base64"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/stretchr/testify/require"
)

func TestBuildLifecycleSyncProtectionFilterAndResponse(t *testing.T) {
	stats := objectio.NewObjectStats()
	name := objectio.BuildObjectName(&types.Uuid{1, 2, 3}, 7)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	filter, err := buildLifecycleSyncProtectionFilter([]objectio.ObjectStats{*stats})
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(filter)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(decoded), 24)

	manager := gc.NewSyncProtectionManager()
	require.NoError(t, manager.RegisterSyncProtection(
		"lifecycle-round-trip",
		filter,
		1,
		"lifecycle-test",
	))
	require.True(t, manager.IsProtected(name.String()))

	require.NoError(t, validateLifecycleMoCtlResponse(
		`{"result":[{"ReturnStr":"{\"status\":\"ok\"}"}]}`,
	))
	require.Error(t, validateLifecycleMoCtlResponse(
		`{"result":[{"ReturnStr":"{\"status\":\"error\",\"code\":\"busy\"}"}]}`,
	))
}
