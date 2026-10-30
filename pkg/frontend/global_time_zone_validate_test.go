// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
)

// SET GLOBAL time_zone rejects an unknown zone like SET SESSION does, instead
// of persisting a value every new session then fails to load (issue #31714).
func TestSetGlobalTimeZoneRejectsUnknownZone(t *testing.T) {
	ctx := context.Background()
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	setPu("", config.NewParameterUnit(sv, nil, nil, nil))
	setSessionAlloc("", NewLeakCheckAllocator())
	ses := &Session{feSessionImpl: feSessionImpl{service: ""}}
	ses.SetTenantInfo(getDefaultAccount())
	initTestSessionSysVars(ses)

	stubGlobalSysVarPersistence(t,
		sysVarSet{"time_zone", "Mars/Phobos"},
		sysVarSet{"time_zone", "+25:00"},
		sysVarSet{"time_zone", "+08:00"},
		sysVarSet{"time_zone", "UTC"},
	)

	for _, bad := range []string{"Mars/Phobos", "+25:00"} {
		require.Error(t, ses.SetGlobalSysVar(ctx, "time_zone", bad), bad)
		val, err := ses.GetGlobalSysVar("time_zone")
		require.NoError(t, err)
		require.Equal(t, "SYSTEM", val, "rejected %q must not replace the global value", bad)
	}

	for _, good := range []string{"+08:00", "UTC"} {
		require.NoError(t, ses.SetGlobalSysVar(ctx, "time_zone", good), good)
		val, err := ses.GetGlobalSysVar("time_zone")
		require.NoError(t, err)
		require.Equal(t, good, val)
	}
}
