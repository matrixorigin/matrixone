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

package test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// enableWorkspaceDebugForTest explicitly retains DEBUG-path coverage without
// depending on a preceding test to change the global logger.
func enableWorkspaceDebugForTest(t *testing.T) {
	t.Helper()
	previous := logutil.GetGlobalLogger()
	restore := logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: "debug"})
	t.Cleanup(func() {
		require.True(t, restore(), "the logger was reconfigured during the test")
		require.Same(t, previous, logutil.GetGlobalLogger())
	})
	require.True(t, logutil.GetSkip1Logger().Core().Enabled(zapcore.DebugLevel))
}
