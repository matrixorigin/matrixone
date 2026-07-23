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

package iceberg

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

type DMLMatchedScanCollectorSpec struct {
	DataFiles               []api.DataFile
	DataFilePathColumnIndex int32
	RowOrdinalColumnIndex   int32
	EqualityFieldIDs        []int
	EqualityColumnIndexes   []int32
	PredicateStable         bool
	IncludePositionRows     bool
	MemoryBudget            *dmlMemoryBudget
}

type DMLMatchedScanCollector struct {
	spec          DMLMatchedScanCollectorSpec
	targets       map[string]*DMLMatchedDeleteTarget
	order         []string
	reservedBytes int64
}

func NewDMLMatchedScanCollector(spec DMLMatchedScanCollectorSpec) *DMLMatchedScanCollector {
	return &DMLMatchedScanCollector{
		spec:    spec,
		targets: make(map[string]*DMLMatchedDeleteTarget),
	}
}

func NewDMLMatchedScanCollectorForPlan(plan *api.IcebergScanPlan, spec DMLMatchedScanCollectorSpec) *DMLMatchedScanCollector {
	spec.DataFiles = append([]api.DataFile(nil), dataFilesFromScanPlan(plan)...)
	return NewDMLMatchedScanCollector(spec)
}

func (c *DMLMatchedScanCollector) AddBatch(ctx context.Context, bat *batch.Batch) error {
	if c == nil || bat == nil || bat.RowCount() == 0 {
		return nil
	}
	spec := c.spec
	reserved := retainedDMLCollectorBatchBytes(bat, len(spec.EqualityFieldIDs))
	// Reserve before materializing maps and copied strings so one large input
	// batch cannot transiently jump over the configured DML heap boundary.
	if err := spec.MemoryBudget.reserve(ctx, reserved); err != nil {
		return err
	}
	keepReservation := false
	defer func() {
		if !keepReservation {
			spec.MemoryBudget.release(reserved)
		}
	}()
	spec.DataFilePathColumnIndex = dmlBatchColumnIndexByName(
		bat,
		api.DMLDataFilePathColumnName,
		spec.DataFilePathColumnIndex,
	)
	spec.RowOrdinalColumnIndex = dmlBatchColumnIndexByName(
		bat,
		api.DMLRowOrdinalColumnName,
		spec.RowOrdinalColumnIndex,
	)
	targets, err := BuildDMLMatchedDeleteTargetsFromScanBatch(ctx, DMLMatchedScanBatchRequest{
		Batch:                   bat,
		DataFiles:               spec.DataFiles,
		DataFilePathColumnIndex: spec.DataFilePathColumnIndex,
		RowOrdinalColumnIndex:   spec.RowOrdinalColumnIndex,
		EqualityFieldIDs:        spec.EqualityFieldIDs,
		EqualityColumnIndexes:   spec.EqualityColumnIndexes,
		PredicateStable:         spec.PredicateStable,
		IncludePositionRows:     spec.IncludePositionRows,
	})
	if err != nil {
		return err
	}
	for idx := range targets {
		c.addTarget(targets[idx])
	}
	c.reservedBytes = saturatingDMLAdd(c.reservedBytes, reserved)
	keepReservation = true
	return nil
}

func (c *DMLMatchedScanCollector) Reset() {
	if c == nil {
		return
	}
	c.spec.MemoryBudget.release(c.reservedBytes)
	c.reservedBytes = 0
	c.targets = make(map[string]*DMLMatchedDeleteTarget)
	c.order = nil
}

func (c *DMLMatchedScanCollector) Targets() []DMLMatchedDeleteTarget {
	if c == nil || len(c.order) == 0 {
		return nil
	}
	out := make([]DMLMatchedDeleteTarget, 0, len(c.order))
	for _, path := range c.order {
		target := c.targets[path]
		if target == nil {
			continue
		}
		out = append(out, DMLMatchedDeleteTarget{
			DataFile:        target.DataFile,
			EqualityIDs:     append([]int(nil), target.EqualityIDs...),
			EqualityRows:    append([]dml.EqualityDeleteRow(nil), target.EqualityRows...),
			PositionRows:    append([]dml.PositionDeleteRow(nil), target.PositionRows...),
			PredicateStable: target.PredicateStable,
			HasRowOrdinal:   target.HasRowOrdinal,
		})
	}
	return out
}

func (c *DMLMatchedScanCollector) RetainedBytes() int64 {
	if c == nil {
		return 0
	}
	return c.reservedBytes
}

func (c *DMLMatchedScanCollector) addTarget(target DMLMatchedDeleteTarget) {
	path := strings.TrimSpace(target.DataFile.FilePath)
	if path == "" {
		return
	}
	merged := c.targets[path]
	if merged == nil {
		copyTarget := target
		copyTarget.EqualityIDs = append([]int(nil), target.EqualityIDs...)
		copyTarget.EqualityRows = append([]dml.EqualityDeleteRow(nil), target.EqualityRows...)
		copyTarget.PositionRows = append([]dml.PositionDeleteRow(nil), target.PositionRows...)
		c.targets[path] = &copyTarget
		c.order = append(c.order, path)
		return
	}
	merged.EqualityRows = append(merged.EqualityRows, target.EqualityRows...)
	merged.PositionRows = append(merged.PositionRows, target.PositionRows...)
	merged.HasRowOrdinal = merged.HasRowOrdinal || target.HasRowOrdinal
	merged.PredicateStable = merged.PredicateStable && target.PredicateStable
	if len(merged.EqualityIDs) == 0 && len(target.EqualityIDs) > 0 {
		merged.EqualityIDs = append([]int(nil), target.EqualityIDs...)
	}
}

func dmlBatchColumnIndexByName(bat *batch.Batch, name string, fallback int32) int32 {
	if bat == nil || len(bat.Attrs) != len(bat.Vecs) {
		return fallback
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return fallback
	}
	for idx, attr := range bat.Attrs {
		if strings.ToLower(strings.TrimSpace(attr)) == name {
			return int32(idx)
		}
	}
	return fallback
}

func dataFilesFromScanPlan(plan *api.IcebergScanPlan) []api.DataFile {
	if plan == nil || len(plan.DataTasks) == 0 {
		return nil
	}
	out := make([]api.DataFile, 0, len(plan.DataTasks))
	seen := make(map[string]struct{}, len(plan.DataTasks))
	for _, task := range plan.DataTasks {
		path := strings.TrimSpace(task.DataFile.FilePath)
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, task.DataFile)
	}
	return out
}
