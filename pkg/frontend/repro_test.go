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

func TestInteractiveTimeoutScopeLeak(t *testing.T) {
	ctx := context.Background()

	// Setup session
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())

	// Create a dummy session
	// ses := NewSession(ctx, "", nil, nil) // This panics if proto is nil
	ses := &Session{
		feSessionImpl: feSessionImpl{
			service: "",
		},
	}

	// Initialize system variables
	// We need to mock GSysVarsMgr behavior or just manually init for test
	// Since GSysVarsMgr depends on table access, let's manually setup gSysVars and sesSysVars
	// to mimic InitSystemVariables behavior

	gVars := &SystemVariables{mp: make(map[string]interface{})}
	// Default interactive_timeout
	gVars.Set("interactive_timeout", int64(86400))

	ses.gSysVars = gVars
	ses.sesSysVars = ses.gSysVars.Clone()

	// Verify initial state
	val, err := ses.GetGlobalSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val)

	val, err = ses.GetSessionSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val)

	// 1. Set Session Variable
	err = ses.SetSessionSysVar(ctx, "interactive_timeout", int64(30100))
	require.NoError(t, err)

	// Verify Session updated
	val, err = ses.GetSessionSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(30100), val)

	// Verify Global UNCHANGED
	val, err = ses.GetGlobalSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val, "Global variable should not change after session set")

	// 2. Set Global Variable (should fail)
	err = ses.SetGlobalSysVar(ctx, "interactive_timeout", int64(30100))
	require.Error(t, err) // Should fail with read-only

	// Verify Global UNCHANGED
	val, err = ses.GetGlobalSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val, "Global variable should not change after failed global set")
}
