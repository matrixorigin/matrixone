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

package iscp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewWireReferenceAndReplayIsolation(t *testing.T) {
	ref := &mvdefinition.Reference{Format: 1, TargetID: 100, Generation: 1, Digest: strings.Repeat("1", 64)}
	spec := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_MaterializedView), MVReference: ref, RefreshSQL: "must not be replay authority", IncrementalSpec: "must not be duplicated", SrcTable: TableInfo{DBID: 1, DBName: "db", TableID: 11, TableName: "src"}}}
	raw, err := MarshalJobSpec(spec)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &wire))
	require.Equal(t, float64(ConsumerType_IndexSync), wire["ConsumerType"])
	require.Equal(t, "", wire["IndexName"])
	require.Equal(t, "", wire["RefreshSQL"])
	require.Equal(t, "", wire["IncrementalSpec"])
	stored := encodeJSONRows(t, []string{raw})[0]
	decoded, err := UnmarshalJobSpec(stored)
	require.NoError(t, err)
	require.Equal(t, int8(ConsumerType_MaterializedView), decoded.ConsumerType)
	require.Equal(t, ref, decoded.MVReference)
	require.Equal(t, "must not be replay authority", spec.RefreshSQL, "marshaling must not mutate the live specification")
	invalidRaw := strings.Replace(raw, `"Format":1`, `"Format":99`, 1)
	invalidRaw = strings.ReplaceAll(invalidRaw, `"TableID":11`, `"TableID":999`)
	invalid := encodeJSONRows(t, []string{invalidRaw})[0]
	_, err = UnmarshalJobSpec(invalid)
	require.Error(t, err)
	exec := newRuntimeTestExecutor()
	status := encodeJSONRows(t, []string{mustMarshalJobStatus(t, 1, JobStage_Running)})[0]
	// A corrupt first job cannot establish empty DB/table metadata for later jobs.
	require.NoError(t, exec.addOrUpdateJob(0, 11, "corrupt", 1, ISCPJobState_Completed, "1-0", nil, status, 0, true))
	require.NoError(t, exec.addOrUpdateJob(0, 11, "valid", 1, ISCPJobState_Completed, "1-0", stored, status, 0, true))
	table, ok := exec.getTable(0, 11)
	require.True(t, ok)
	require.Equal(t, "db", table.dbName)
	require.Equal(t, "src", table.tableName)
	require.NoError(t, exec.addOrUpdateJob(0, 11, "invalid", 2, ISCPJobState_Completed, "1-0", invalid, status, 0, true))
	_, state, ok := table.getJobState("invalid")
	require.True(t, ok)
	require.Equal(t, int8(ISCPJobState_Error), state)
	_, state, ok = table.getJobState("valid")
	require.True(t, ok)
	require.Equal(t, int8(ISCPJobState_Completed), state)
	require.Equal(t, []TableInfo{spec.SrcTable}, table.sourceTableInfos(), "quarantined sources cannot enter a healthy job's dirty-table scan")
	_, err = NewConsumer("", nil, nil, nil, JobID{}, &ConsumerInfo{ConsumerType: 127})
	require.Error(t, err)
}
func TestMaterializedViewGenerationGCUsesTimestampUnits(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 123456000, time.UTC)
	cutoff := now.Add(-time.Hour)
	table := NewTableEntry(nil, 0, 1, 11, "db", "src")
	for _, item := range []struct {
		id   uint64
		drop types.Timestamp
	}{{1, types.UnixMicroToTimestamp(cutoff.UnixMicro())}, {2, types.UnixMicroToTimestamp(cutoff.UnixMicro() + 1)}, {3, 0}} {
		key := JobKey{JobName: "mv", JobID: item.id}
		table.jobs[key] = &JobEntry{jobName: key.JobName, jobID: key.JobID, dropAt: item.drop}
	}
	require.False(t, table.gcInMemoryJobAt(now, time.Hour))
	require.Len(t, table.jobs, 2)
	require.NotContains(t, table.jobs, JobKey{JobName: "mv", JobID: 1})
	require.Contains(t, table.jobs, JobKey{JobName: "mv", JobID: 2})
	require.Contains(t, table.jobs, JobKey{JobName: "mv", JobID: 3})
	require.False(t, table.gcInMemoryJobAt(now.Add(time.Microsecond), time.Hour))
	require.Len(t, table.jobs, 1)
}
