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

package frontend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDoComQueryContextAfterParseFailure(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	for _, stale := range []bool{false, true} {
		for _, state := range []string{"live", "canceled", "deadline"} {
			t.Run(fmt.Sprintf("stale=%v/%s", stale, state), func(t *testing.T) {
				previous, cancelPrevious := context.WithCancel(context.Background())
				defer cancelPrevious()
				ses.proc.ReplaceTopCtx(previous)
				ses.proc.ResetQueryContext()
				if stale {
					cancelPrevious()
				}
				current := defines.AttachAccountId(context.Background(), 0)
				var cancel context.CancelFunc
				switch state {
				case "live":
					current, cancel = context.WithCancel(current)
				case "canceled":
					current, cancel = context.WithCancel(current)
					cancel()
				case "deadline":
					current, cancel = context.WithDeadline(current, time.Unix(1, 0))
				}
				defer cancel()
				// Parse failure is still a request boundary. It must neither keep
				// an earlier cancellation nor revive a canceled current request.
				// Observe inside the request: owned epoch cleanup cancels its
				// context when doComQuery returns, even after normal work.
				previousRecorder := RecordParseErrorStatement
				t.Cleanup(func() { RecordParseErrorStatement = previousRecorder })
				observed := false
				RecordParseErrorStatement = func(ctx context.Context, _ *Session, proc *process.Process,
					_ time.Time, _ []string, _ []string, _ error) (context.Context, error) {
					observed = true
					require.Equal(t, current.Err(), proc.Ctx.Err())
					require.Equal(t, current.Err(), proc.GetQueryContextError())
					cancel()
					require.Error(t, proc.Ctx.Err(), "current request cancellation must remain connected")
					return ctx, nil
				}
				err := doComQuery(ses, &ExecCtx{reqCtx: current, ses: ses}, &UserInput{sql: "select from"})
				require.Error(t, err)
				require.True(t, observed, "the real parse-error path must reach the recorder")
			})
		}
	}
}
