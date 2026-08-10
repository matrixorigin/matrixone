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

package cnservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunViewMetadataRecoveryLoopContinuesAfterFailureAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	calls, failures := 0, 0
	runViewMetadataRecoveryLoop(ctx, ticks, func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("retry next tick")
		}
		cancel()
		return nil
	}, func(error) { failures++ })
	require.Equal(t, 2, calls)
	require.Equal(t, 1, failures)
}
