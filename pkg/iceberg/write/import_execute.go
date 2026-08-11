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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type NativeImportSink interface {
	ImportIceberg(ctx context.Context, req NativeImportRequest) (*NativeImportResult, error)
}

type NativeImportSinkFunc func(context.Context, NativeImportRequest) (*NativeImportResult, error)

func (f NativeImportSinkFunc) ImportIceberg(ctx context.Context, req NativeImportRequest) (*NativeImportResult, error) {
	return f(ctx, req)
}

type NativeImportRequest struct {
	TargetDatabase string
	TargetTable    string
	Snapshot       api.SnapshotPlan
	DataTasks      []api.DataFileTask
	DeleteTasks    []api.DeleteFileTask
	Profile        ImportProfile
}

type NativeImportResult struct {
	SourceSnapshotID     int64
	MetadataLocationHash string
	RowCount             int64
	FileCount            int
	Checksum             string
}

type ImportExecutor struct {
	Sink NativeImportSink
}

func (e ImportExecutor) Execute(ctx context.Context, req ImportPlanRequest) (*NativeImportResult, error) {
	if e.Sink == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg import requires a native import sink", nil)
	}
	plan, err := BuildImportPlan(ctx, req)
	if err != nil {
		return nil, err
	}
	result, err := e.Sink.ImportIceberg(ctx, NativeImportRequest{
		TargetDatabase: req.TargetDatabase,
		TargetTable:    req.TargetTable,
		Snapshot:       plan.Snapshot,
		DataTasks:      cloneDataFileTasks(plan.DataTasks),
		DeleteTasks:    cloneDeleteFileTasks(plan.DeleteTasks),
		Profile:        plan.Profile,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, api.NewError(api.ErrInternal, "Iceberg import sink returned nil result", nil)
	}
	if result.SourceSnapshotID != plan.Profile.SourceSnapshotID {
		return nil, api.NewError(api.ErrInternal, "Iceberg import sink changed pinned snapshot", map[string]string{
			"target_table": req.TargetTable,
		})
	}
	return result, nil
}
