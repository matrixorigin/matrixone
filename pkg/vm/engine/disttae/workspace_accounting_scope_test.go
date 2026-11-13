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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceAccountingChecksDoNotDependOnLogLevel(t *testing.T) {
	for _, tc := range []struct {
		name, level string
		engine      bool
		enabled     bool
		wantPanic   bool
	}{
		{name: "info default", level: "info", engine: true},
		{name: "info nil engine", level: "info"},
		{name: "info test option", level: "info", engine: true, enabled: true, wantPanic: true},
		{name: "debug default", level: "debug", engine: true, wantPanic: true},
		{name: "debug nil engine", level: "debug", wantPanic: true},
		{name: "debug test option", level: "debug", engine: true, enabled: true, wantPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore := logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: tc.level, DisableStore: true})
			t.Cleanup(func() { require.True(t, restore(), "unexpected concurrent logger setup") })

			txn := &Transaction{}
			if tc.engine {
				txn.engine = &Engine{}
				if tc.enabled {
					WithWorkspaceAccountingChecksForTest()(txn.engine)
				}
			}
			// Empty writes must account for no bytes. Deliberately corrupt the
			// reported total so a live invariant check must reject the state.
			txn.workspaceSize = 1
			check := func() {
				txn.Lock()
				defer txn.Unlock()
				txn.assertWorkspaceAccountingLocked()
			}
			if tc.wantPanic {
				require.Panics(t, check, "the inconsistent workspace must be detected")
			} else {
				require.NotPanics(t, check, "ordinary INFO engines keep the default behavior")
			}
			txn.workspaceSize = 0
			require.NotPanics(t, check, "a consistent empty workspace must pass")
		})
	}
}
