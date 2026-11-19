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

func TestBuildTaskDetailsPreservesPersistedFields(t *testing.T) {
	got := BuildTaskDetails(42, "tenant", "user", "task-name", "task-id")
	want := &task.Details{
		AccountID: 42,
		Account:   "tenant",
		Username:  "user",
		Details: &task.Details_CreateCdc{CreateCdc: &task.CreateCdcDetails{
			TaskName: "task-name",
			TaskId:   "task-id",
			Accounts: []*task.Account{{Id: 42, Name: "tenant"}},
		}},
	}
	encoded, err := got.Marshal()
	require.NoError(t, err)
	wantEncoded, err := want.Marshal()
	require.NoError(t, err)
	require.Equal(t, wantEncoded, encoded)
	var decoded task.Details
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, want, &decoded)
}

func TestCDCStrToTimeFormatsAndTimezone(t *testing.T) {
	zone := time.FixedZone("offset", 2*60*60)
	got, err := CDCStrToTime("2025-01-02 03:04:05", zone)
	require.NoError(t, err)
	require.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, zone), got)
	got, err = CDCStrToTime("2025-01-02T03:04:05Z", zone)
	require.NoError(t, err)
	require.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), got)
	_, err = CDCStrToTime("2025-01-02 03:04:05", nil)
	require.Error(t, err)
	_, err = CDCStrToTime("bad", zone)
	require.Error(t, err)
	got, err = CDCStrToTime("", nil)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

// TestCDCStrToTimeZoneContract locks the documented StartTs/EndTs time zone
// contract: unzoned DateTime values follow the supplied (session) time zone,
// while RFC3339 values with an explicit offset denote the same absolute
// instant in every session. See pkg/cdc/CDC_USER_GUIDE.md "Time Range
// Options".
func TestCDCStrToTimeZoneContract(t *testing.T) {
	utc := time.UTC
	cst := time.FixedZone("CST", 8*60*60)

	// Equivalent RFC3339 inputs produce the identical absolute boundary
	// regardless of the session time zone.
	a, err := CDCStrToTime("2025-01-02T03:04:05Z", utc)
	require.NoError(t, err)
	b, err := CDCStrToTime("2025-01-02T11:04:05+08:00", cst)
	require.NoError(t, err)
	require.True(t, a.Equal(b), "zoned inputs must be zone-independent: %v vs %v", a, b)

	// The same unzoned input resolves 8 hours apart under the two sessions;
	// this is the documented session-time-zone interpretation.
	u, err := CDCStrToTime("2025-01-02 03:04:05", utc)
	require.NoError(t, err)
	c, err := CDCStrToTime("2025-01-02 03:04:05", cst)
	require.NoError(t, err)
	require.Equal(t, 8*time.Hour, u.Sub(c))

	// The persisted form carries the offset, so the stored boundary identifies
	// the absolute instant the creating session computed.
	require.Equal(t, "2025-01-02T03:04:05+08:00", c.Format(time.RFC3339))
}
