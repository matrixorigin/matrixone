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

package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDriverMonitorLabelsAreBoundedAndRedacted(t *testing.T) {
	require.Equal(t, "find", boundedCommandLabel("find"))
	require.Equal(t, "get_more", boundedCommandLabel("getMore"))
	require.Equal(t, "other", boundedCommandLabel("secret-database-command"))
	require.Equal(t, "other", boundedPoolEventLabel("mongodb.internal:27017"))
	require.Equal(t, "other", boundedPoolReasonLabel("password=secret"))
	require.Equal(t, "primary", boundedServerRole("RSPrimary"))
	require.Equal(t, "other", boundedServerRole("mongodb.internal:27017"))
}

func TestDriverMonitorResolvesRoleWithoutExportingEndpoint(t *testing.T) {
	state := &driverMonitorState{roles: map[string]string{"mongodb.internal:27017": "secondary"}}
	require.Equal(t, "secondary", state.roleForConnection("mongodb.internal:27017[-42]"))
	require.Equal(t, "unknown", state.roleForConnection("other.internal:27017[-1]"))
}
