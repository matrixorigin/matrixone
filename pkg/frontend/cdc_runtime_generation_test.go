// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCDCTaskExecutorPinsRuntimeGeneration(t *testing.T) {
	oldRT := runtime.NewRuntime(0, "same-cn", zap.NewNop())
	freshRT := runtime.NewRuntime(0, "same-cn", zap.NewNop())
	oldDetector := createMockTableDetector()
	freshDetector := createMockTableDetector()
	oldRT.SetGlobalVariables(runtime.CDCTableDetector, oldDetector)
	freshRT.SetGlobalVariables(runtime.CDCTableDetector, freshDetector)
	oldUpdater := cdc.InitCDCWatermarkUpdater(oldRT, nil)
	t.Cleanup(oldUpdater.Stop)
	freshUpdater := cdc.InitCDCWatermarkUpdater(freshRT, nil)
	t.Cleanup(freshUpdater.Stop)
	spec := &task.CreateCdcDetails{TaskId: "task", Accounts: []*task.Account{{Id: 1}}}
	oldExec := NewCDCTaskExecutor(oldRT, zap.NewNop(), nil, spec, "same-cn", nil, nil, nil, nil)
	freshExec := NewCDCTaskExecutor(freshRT, zap.NewNop(), nil, spec, "same-cn", nil, nil, nil, nil)
	oldDetector.Stop()
	oldUpdater.Stop()
	// Late old factory work must reject its closed generation before touching
	// catalog SQL. A nil executor makes any accidental SQL call discriminating.
	require.ErrorIs(t, oldExec.Start(context.Background()), context.Canceled)
	require.Same(t, oldDetector, oldExec.tableDetector)
	require.Same(t, oldUpdater, oldExec.watermarkUpdater)
	require.Same(t, freshDetector, freshExec.tableDetector)
	require.Same(t, freshUpdater, freshExec.watermarkUpdater)
	require.NoError(t, freshDetector.Context().Err())
	require.NoError(t, freshUpdater.Start(context.Background()))
}
