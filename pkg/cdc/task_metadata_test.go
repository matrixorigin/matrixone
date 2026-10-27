// Copyright 2024 Matrix Origin
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

package cdc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestBuildTaskMetadataProtocolSelection(t *testing.T) {
	tests := []struct {
		name   string
		noFull bool
		opts   map[string]string
		want   task.TaskCode
	}{
		{name: "legacy", want: task.TaskCode_InitCdc},
		{name: "stable snapshot", opts: map[string]string{CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolStableEpoch}, want: task.TaskCode_InitCdcStableEpoch},
		{name: "stable without full", noFull: true, opts: map[string]string{CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolStableEpoch}, want: task.TaskCode_InitCdc},
		{name: "lossless incremental", noFull: true, opts: map[string]string{CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolNoFullHLC}, want: task.TaskCode_InitCdcLosslessStart},
		{name: "lossless with full", opts: map[string]string{CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolNoFullHLC}, want: task.TaskCode_InitCdc},
		{name: "pattern precedes lossless", noFull: true, opts: map[string]string{CDCTaskExtraOptions_SourcePatternProtocol: CDCSourcePatternProtocolV1, CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolNoFullHLC}, want: task.TaskCode_InitCdcSourcePatternV1},
		{name: "generation precedes stable", opts: map[string]string{CDCTaskExtraOptions_GenerationProtocol: CDCGenerationAwareProtocolV2, CDCTaskExtraOptions_InitialSnapshotProtocol: CDCInitialSnapshotProtocolStableEpoch}, want: task.TaskCode_InitCdcSourcePatternV1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := json.Marshal(tc.opts)
			require.NoError(t, err)
			metadata := BuildTaskMetadata("task-id", string(opts), tc.noFull)
			require.Equal(t, "task-id", metadata.ID)
			require.Equal(t, tc.want, metadata.Executor)
			require.EqualValues(t, 10, metadata.Options.MaxRetryTimes)
			require.EqualValues(t, 10*time.Second, metadata.Options.RetryInterval)
			require.Zero(t, metadata.Options.DelayDuration)
			require.Zero(t, metadata.Options.Concurrency)
		})
	}
	require.Equal(t, task.TaskCode_InitCdc, BuildTaskMetadata("task-id", "{", true).Executor)
}
