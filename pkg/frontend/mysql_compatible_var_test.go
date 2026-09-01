// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// The planner looks this variable up by name. Nothing else connects the
// declaration here to the lookup there, so a rename on either side would
// silently leave the setting with no effect rather than failing.
func TestMysqlCompatibleSystemVariableContract(t *testing.T) {
	sysVar, ok := gSysVarsDefs[plan2.MysqlCompatibleVarName]
	require.True(t, ok,
		"planner resolves %q; it must exist as a system variable",
		plan2.MysqlCompatibleVarName)
	require.Equal(t, plan2.MysqlCompatibleVarName, sysVar.Name)

	// Off by default: MO's strict typing is the correct behavior and this only
	// relaxes it on request.
	require.Equal(t, int8(0), sysVar.Default)

	// The planner reads the value as int8 and treats anything else as off, so
	// the declared type must be the one that normalizes to int8.
	_, isBool := sysVar.Type.(SystemVariableBoolType)
	require.True(t, isBool, "must be a bool system variable")
	converted, err := sysVar.Type.Convert("on")
	require.NoError(t, err)
	require.Equal(t, int8(1), converted)
}
