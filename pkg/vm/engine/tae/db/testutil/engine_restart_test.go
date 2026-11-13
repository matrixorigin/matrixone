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

package testutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestRestartCanceledContextCleansUp(t *testing.T) {
	for _, tc := range []struct {
		name      string
		disableGC bool
	}{{"restart", false}, {"restart_without_gc", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), TestCheckpointTimeout)
			defer cancel()
			engine := NewTestEngineWithDir(ctx, t.TempDir(), t, config.WithLongScanAndCKPOpts(nil))
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			previous := engine.DB
			dir, opts := previous.Dir, previous.Opts
			expired, stop := context.WithCancel(context.Background())
			stop()
			require.ErrorIs(t, engine.restart(expired, tc.disableGC), context.Canceled)
			require.Nil(t, engine.DB, "a failed reopen must not publish a partial engine")
			require.NotNil(t, previous.Closed.Load(), "the previous engine must be closed")

			// A failed open must release its directory lock and other rollback-owned
			// resources so a later generation can open the same database.
			reopened, err := db.Open(ctx, dir, opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
		})
	}
}
