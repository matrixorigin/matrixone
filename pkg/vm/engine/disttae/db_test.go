// Copyright 2025 Matrix Origin
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

package disttae

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/require"
)

// minimal mock cluster for tn handler wiring
type mockCluster struct{}

func (m mockCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	apply(metadata.CNService{ServiceID: "not-exist"})
}
func (m mockCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
}
func (m mockCluster) GetAllTNServices() []metadata.TNService { return nil }
func (m mockCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}
func (m mockCluster) ForceRefresh(sync bool)                               {}
func (m mockCluster) Close()                                               {}
func (m mockCluster) DebugUpdateCNLabel(string, map[string][]string) error { return nil }
func (m mockCluster) DebugUpdateCNWorkState(string, int) error             { return nil }
func (m mockCluster) RemoveCN(string)                                      {}
func (m mockCluster) AddCN(metadata.CNService)                             {}
func (m mockCluster) UpdateCN(metadata.CNService)                          {}

func Test_requestSnapshotRead_Smoke(t *testing.T) {
	ctx := context.Background()

	// register a mock cluster service to avoid panic
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, mockCluster{})

	// build a minimal process for the table
	proc := testutil.NewProc(t)
	// set a benign account name for session info
	proc.Base.SessionInfo.Account = "sys"

	tbl := &txnTable{}
	tbl.proc.Store(proc)

	// A zero ts is acceptable for this no-TN response test.
	ts := types.BuildTS(0, 0)
	response, err := RequestSnapshotRead(ctx, tbl, &ts)
	require.NoError(t, err)
	resp, ok := response.(*cmd_util.SnapshotReadResp)
	require.True(t, ok)
	require.False(t, resp.Succeed)
}

func TestRequestSnapshotReadUntilReadyRetriesTemporaryCheckpointLag(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	calls := 0
	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		calls++
		return &cmd_util.SnapshotReadResp{Succeed: calls > 1}, nil
	}

	ts := types.BuildTS(10, 0)
	resp, err := requestSnapshotReadUntilReady(
		context.Background(),
		&txnTable{},
		&ts,
		time.Nanosecond,
		time.Second,
	)
	require.NoError(t, err)
	require.True(t, resp.Succeed)
	require.Equal(t, 2, calls)
}

func TestRequestSnapshotReadUntilReadyBoundsCheckpointLag(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		return &cmd_util.SnapshotReadResp{Succeed: false}, nil
	}

	ts := types.BuildTS(10, 0)
	_, err := requestSnapshotReadUntilReady(
		context.Background(),
		&txnTable{},
		&ts,
		time.Millisecond,
		20*time.Millisecond,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable), err)
}

func TestRequestSnapshotReadUntilReadyPreservesRetryTimeoutCause(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	RequestSnapshotRead = func(
		ctx context.Context,
		_ *txnTable,
		_ *types.TS,
	) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ts := types.BuildTS(10, 0)
	_, err := requestSnapshotReadUntilReady(
		context.Background(),
		&txnTable{},
		&ts,
		time.Hour,
		20*time.Millisecond,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable), err)
}

func TestRequestSnapshotReadUntilReadyHonorsCallerCancellation(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		return &cmd_util.SnapshotReadResp{Succeed: false}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ts := types.BuildTS(10, 0)
	_, err := requestSnapshotReadUntilReady(
		ctx,
		&txnTable{},
		&ts,
		time.Hour,
		time.Hour,
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRequestSnapshotReadUntilReadyDoesNotRetryRequestError(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	requestErr := errors.New("snapshot read failed")
	calls := 0
	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		calls++
		return nil, requestErr
	}

	ts := types.BuildTS(10, 0)
	_, err := requestSnapshotReadUntilReady(
		context.Background(),
		&txnTable{},
		&ts,
		time.Nanosecond,
		time.Second,
	)
	require.ErrorIs(t, err, requestErr)
	require.Equal(t, 1, calls)
}

func TestSnapshotCheckpointsCanServe(t *testing.T) {
	newEntry := func(
		start, end types.TS,
		entryType checkpoint.EntryType,
	) *checkpoint.CheckpointEntry {
		return checkpoint.NewCheckpointEntry("", start, end, entryType)
	}

	t0 := types.BuildTS(0, 0)
	t5 := types.BuildTS(5, 0)
	t6 := t5.Next()
	t10 := types.BuildTS(10, 0)
	t20 := types.BuildTS(20, 0)

	tests := []struct {
		name    string
		entries []*checkpoint.CheckpointEntry
		ts      types.TS
		want    bool
	}{
		{name: "empty", ts: t10},
		{name: "coverage ends before snapshot", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental)}, ts: t10},
		{name: "single covering checkpoint", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "missing incremental prefix", entries: []*checkpoint.CheckpointEntry{newEntry(t5, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "global boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Global), newEntry(t5, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "global next boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Global), newEntry(t6, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "compacted boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Compacted), newEntry(t5, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "compacted next boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Compacted), newEntry(t6, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "incremental boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t6, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "predecessor retained before global", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t0, t6, checkpoint.ET_Global), newEntry(t6, t20, checkpoint.ET_Incremental)}, ts: t10, want: true},
		{name: "multiple predecessors before global", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t6, t10, checkpoint.ET_Incremental), newEntry(t0, t20, checkpoint.ET_Global)}, ts: t10},
		{name: "compacted after predecessor", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t0, t20, checkpoint.ET_Compacted)}, ts: t10},
		{name: "gap", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t10, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "gap after global", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Global), newEntry(t10, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "gap after compacted", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Compacted), newEntry(t10, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "shared incremental boundary", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Incremental), newEntry(t5, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "overlap", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t10, checkpoint.ET_Incremental), newEntry(t5, t20, checkpoint.ET_Incremental)}, ts: t10},
		{name: "out of order", entries: []*checkpoint.CheckpointEntry{newEntry(t10, t20, checkpoint.ET_Incremental), newEntry(t0, t5, checkpoint.ET_Incremental)}, ts: t10},
		{name: "nil checkpoint", entries: []*checkpoint.CheckpointEntry{nil}, ts: t10},
		{name: "invalid global start", entries: []*checkpoint.CheckpointEntry{newEntry(t5, t20, checkpoint.ET_Global)}, ts: t10},
		{name: "backup checkpoint", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t20, checkpoint.ET_Backup)}, ts: t10},
		{name: "multiple bases", entries: []*checkpoint.CheckpointEntry{newEntry(t0, t5, checkpoint.ET_Global), newEntry(t0, t20, checkpoint.ET_Global)}, ts: t10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, snapshotCheckpointsCanServe(test.entries, test.ts))
		})
	}
}

func TestParseSnapshotCheckpointEntries(t *testing.T) {
	t0 := types.BuildTS(0, 0).ToTimestamp()
	t5 := types.BuildTS(5, 0).ToTimestamp()
	t5TS := types.BuildTS(5, 0)
	t6 := t5TS.Next().ToTimestamp()
	t10 := types.BuildTS(10, 0).ToTimestamp()

	tests := []struct {
		name    string
		resp    *cmd_util.SnapshotReadResp
		wantLen int
		wantMin types.TS
		wantMax types.TS
		wantErr string
	}{
		{name: "nil response", wantErr: "not ready"},
		{name: "lag response", resp: &cmd_util.SnapshotReadResp{}, wantErr: "not ready"},
		{name: "successful empty", resp: &cmd_util.SnapshotReadResp{Succeed: true}, wantMin: types.MaxTs()},
		{name: "nil entry", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{nil}}, wantErr: "index 0"},
		{name: "nil start", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{End: &t5}}}, wantErr: "index 0"},
		{name: "nil end", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{Start: &t0}}}, wantErr: "index 0"},
		{name: "inverted range", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{Start: &t10, End: &t5, EntryType: int32(checkpoint.ET_Incremental)}}}, wantErr: "invalid checkpoint range"},
		{name: "unsupported type", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{Start: &t0, End: &t5, EntryType: int32(checkpoint.ET_Backup)}}}, wantErr: "unsupported checkpoint type"},
		{name: "gapped chain", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{Start: &t0, End: &t5, EntryType: int32(checkpoint.ET_Incremental)}, {Start: &t10, End: &t10, EntryType: int32(checkpoint.ET_Incremental)}}}, wantErr: "gap or overlap"},
		{name: "valid chain", resp: &cmd_util.SnapshotReadResp{Succeed: true, Entries: []*cmd_util.CheckpointEntryResp{{Start: &t0, End: &t5, EntryType: int32(checkpoint.ET_Incremental)}, {Start: &t6, End: &t10, EntryType: int32(checkpoint.ET_Incremental)}}}, wantLen: 2, wantMin: types.TimestampToTS(t0), wantMax: types.TimestampToTS(t10)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries, minTS, maxTS, err := parseSnapshotCheckpointEntries(test.resp)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Nil(t, entries)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, test.wantLen)
			require.Equal(t, test.wantMin, minTS)
			require.Equal(t, test.wantMax, maxTS)
		})
	}
}

func TestGetOrCreateSnapPartRejectsSuccessfulCheckpointGap(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	calls := 0
	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		calls++
		start1 := types.BuildTS(0, 0).ToTimestamp()
		end1 := types.BuildTS(5, 0).ToTimestamp()
		start2 := types.BuildTS(10, 0).ToTimestamp()
		end2 := types.BuildTS(20, 0).ToTimestamp()
		return &cmd_util.SnapshotReadResp{
			Succeed: true,
			Entries: []*cmd_util.CheckpointEntryResp{
				{Start: &start1, End: &end1, EntryType: int32(checkpoint.ET_Incremental)},
				{Start: &start2, End: &end2, EntryType: int32(checkpoint.ET_Incremental)},
			},
		}, nil
	}

	tbl := newTxnTableForTest()
	tbl.tableId = 42
	tbl.tableName = "t"
	ts := types.BuildTS(10, 0)
	_, err := tbl.eng.(*Engine).getOrCreateSnapPartBy(context.Background(), tbl, ts)
	require.ErrorContains(t, err, "checkpoint gap or overlap")
	require.Equal(t, 1, calls, "a successful but gapped response is not temporary lag")
}

func TestGetOrCreateSnapPartRejectsMissingCheckpointPrefix(t *testing.T) {
	oldRequestSnapshotRead := RequestSnapshotRead
	defer func() { RequestSnapshotRead = oldRequestSnapshotRead }()

	calls := 0
	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		calls++
		start := types.BuildTS(5, 0).ToTimestamp()
		end := types.BuildTS(20, 0).ToTimestamp()
		return &cmd_util.SnapshotReadResp{
			Succeed: true,
			Entries: []*cmd_util.CheckpointEntryResp{{
				Start:     &start,
				End:       &end,
				EntryType: int32(checkpoint.ET_Incremental),
			}},
		}, nil
	}

	tbl := newTxnTableForTest()
	tbl.tableId = 42
	tbl.tableName = "t"
	ts := types.BuildTS(10, 0)
	_, err := tbl.eng.(*Engine).getOrCreateSnapPartBy(context.Background(), tbl, ts)
	require.ErrorContains(t, err, "No available checkpoints for snapshot read")
	require.Equal(t, 1, calls, "a successful malformed response is not temporary lag")
}
