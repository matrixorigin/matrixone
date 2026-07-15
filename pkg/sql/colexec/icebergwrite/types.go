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

package icebergwrite

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

const opName = "icebergwrite"

const (
	OperationAppend    = "append"
	OperationDelete    = "delete"
	OperationUpdate    = "update"
	OperationMerge     = "merge"
	OperationOverwrite = "overwrite"
)

type AppendRequest struct {
	Ref               *plan.ObjectRef
	TableDef          *plan.TableDef
	Attrs             []string
	AddAffectedRows   bool
	AccountID         uint32
	RoleID            uint64
	UserID            uint64
	ExternalPrincipal string
	StatementID       string
	IdempotencyKey    string
	ParallelID        int32
	MaxParallel       int32
	CatalogName       string
	Namespace         string
	Table             string
	DefaultRef        string
	ReadMode          string
	WriteMode         string
	Operation         string
	TimeZone          *time.Location

	// DataFilePathColumnIndex and RowOrdinalColumnIndex are populated for
	// row-level DML sinks. Append INSERT leaves them at -1.
	DataFilePathColumnIndex int32
	RowOrdinalColumnIndex   int32
	MergeActionColumnIndex  int32
	DMLScan                 DMLScanMetadata
}

type DMLScanMetadata struct {
	DataFiles          []api.DataFile
	BaseSnapshotID     int64
	BaseSchemaID       int
	Ref                string
	ObjectIORef        string
	OverwriteScope     string
	OverwritePartition map[string]any
}

type CoordinatorFactory interface {
	NewCoordinator(ctx context.Context, req AppendRequest) (Coordinator, error)
}

type CoordinatorFactoryFunc func(ctx context.Context, req AppendRequest) (Coordinator, error)

func (f CoordinatorFactoryFunc) NewCoordinator(ctx context.Context, req AppendRequest) (Coordinator, error) {
	return f(ctx, req)
}

type Coordinator interface {
	Begin(ctx context.Context, req AppendRequest) error
	Append(ctx context.Context, bat *batch.Batch) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context, cause error) error
}

type container struct {
	opened   bool
	finished bool
}

type IcebergWrite struct {
	ctr                    container
	input                  vm.CallResult
	Request                AppendRequest
	Coordinator            Coordinator
	Factory                CoordinatorFactory
	coordinatorFromFactory bool

	objectIORefRetained bool

	vm.OperatorBase
}

var _ vm.Operator = new(IcebergWrite)

func NewArgument(req AppendRequest) *IcebergWrite {
	return &IcebergWrite{Request: req}
}

func (w *IcebergWrite) WithCoordinator(coordinator Coordinator) *IcebergWrite {
	w.Coordinator = coordinator
	w.coordinatorFromFactory = false
	return w
}

func (w *IcebergWrite) WithCoordinatorFactory(factory CoordinatorFactory) *IcebergWrite {
	w.Factory = factory
	return w
}

func (w *IcebergWrite) RetainObjectIORef(ctx context.Context) error {
	if w == nil || w.objectIORefRetained {
		return nil
	}
	ref := strings.TrimSpace(w.Request.DMLScan.ObjectIORef)
	if ref == "" {
		return nil
	}
	if !icebergio.RetainObjectIORef(ref) {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg write object IO ref is not registered or expired", map[string]string{
			"object_io_ref": api.PathHash(ref),
		}))
	}
	w.objectIORefRetained = true
	return nil
}

func (w *IcebergWrite) ReleaseObjectIORef() {
	if w == nil || !w.objectIORefRetained {
		return
	}
	icebergio.ReleaseObjectIORef(w.Request.DMLScan.ObjectIORef)
	w.objectIORefRetained = false
}

func (w *IcebergWrite) GetOperatorBase() *vm.OperatorBase {
	return &w.OperatorBase
}

func (w *IcebergWrite) TypeName() string {
	return opName
}

func (w *IcebergWrite) Release() {}
