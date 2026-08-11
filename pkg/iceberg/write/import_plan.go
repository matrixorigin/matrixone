// Copyright 2026 Matrix Origin
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

package write

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type ImportPlanRequest struct {
	ScanPlan       api.IcebergScanPlan
	TargetDatabase string
	TargetTable    string
}

type ImportPlan struct {
	Snapshot    api.SnapshotPlan
	DataTasks   []api.DataFileTask
	DeleteTasks []api.DeleteFileTask
	Profile     ImportProfile
}

type ImportProfile struct {
	SourceSnapshotID     int64
	MetadataLocationHash string
	RowCount             int64
	FileCount            int
	Checksum             string
}

func BuildImportPlan(ctx context.Context, req ImportPlanRequest) (*ImportPlan, error) {
	if req.ScanPlan.Snapshot.SnapshotID == 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg import requires a resolved source snapshot", nil)
	}
	if req.TargetTable == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg import requires a target native table", nil)
	}
	dataTasks := cloneDataFileTasks(req.ScanPlan.DataTasks)
	deleteTasks := cloneDeleteFileTasks(req.ScanPlan.DeleteTasks)
	rowCount := int64(0)
	for _, task := range dataTasks {
		rowCount += task.DataFile.RecordCount
	}
	profile := ImportProfile{
		SourceSnapshotID:     req.ScanPlan.Snapshot.SnapshotID,
		MetadataLocationHash: req.ScanPlan.Snapshot.MetadataLocationHash,
		RowCount:             rowCount,
		FileCount:            len(dataTasks),
	}
	profile.Checksum = importChecksum(req.ScanPlan.Snapshot, dataTasks, deleteTasks)
	return &ImportPlan{
		Snapshot:    req.ScanPlan.Snapshot,
		DataTasks:   dataTasks,
		DeleteTasks: deleteTasks,
		Profile:     profile,
	}, nil
}

func importChecksum(snapshot api.SnapshotPlan, dataTasks []api.DataFileTask, deleteTasks []api.DeleteFileTask) string {
	hash := sha256.New()
	hash.Write([]byte(strconv.FormatInt(snapshot.SnapshotID, 10)))
	hash.Write([]byte{0})
	hash.Write([]byte(snapshot.MetadataLocationHash))
	for _, task := range dataTasks {
		hash.Write([]byte{0})
		hash.Write([]byte(firstNonEmpty(task.DataFile.FilePathHash, api.PathHash(task.DataFile.FilePath))))
		hash.Write([]byte{0})
		hash.Write([]byte(strconv.FormatInt(task.DataFile.RecordCount, 10)))
	}
	for _, task := range deleteTasks {
		hash.Write([]byte{0})
		hash.Write([]byte(firstNonEmpty(task.DataFile.FilePathHash, api.PathHash(task.DataFile.FilePath))))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func cloneDataFileTasks(in []api.DataFileTask) []api.DataFileTask {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.DataFileTask(nil), in...)
	for i := range out {
		out[i].DataFile = cloneDataFiles([]api.DataFile{in[i].DataFile})[0]
	}
	return out
}

func cloneDeleteFileTasks(in []api.DeleteFileTask) []api.DeleteFileTask {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.DeleteFileTask(nil), in...)
	for i := range out {
		out[i].DataFile = cloneDataFiles([]api.DataFile{in[i].DataFile})[0]
	}
	return out
}
