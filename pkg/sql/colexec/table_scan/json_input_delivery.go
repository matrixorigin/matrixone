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

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// RequestJSONInputs supplies demand before Read. Only identity outputs and
// direct local column projections have an exact storage-column mapping.
func (tableScan *TableScan) RequestJSONInputs(proc *process.Process, columns []int) error {
	if proc == nil || proc.Ctx == nil {
		return moerr.NewInvalidInputNoCtx("JSON input request requires an execution context")
	}
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	if len(columns) == 0 {
		tableScan.ctr.jsonRequestMeta.Close()
		tableScan.ctr.jsonRequestMeta = nil
		tableScan.ctr.jsonDeliveryColumns = nil
		tableScan.ctr.jsonColumns = tableScan.JSONInputColumns()
		return nil
	}
	internalColumns := tableScan.JSONInputColumns()
	if len(columns) > int(^uint(0)>>1)-len(internalColumns)-1 {
		return moerr.NewInvalidInputNoCtx("JSON input request is too large")
	}
	entries := len(columns) + len(internalColumns) + 1
	if tableScan.ctr.jsonRequestMeta == nil {
		scope, err := colexec.NewJSONMetadataScope(proc, tableScan.OutputAllocation, entries)
		if err != nil {
			return err
		}
		tableScan.ctr.jsonRequestMeta = scope
	} else if err := tableScan.ctr.jsonRequestMeta.Ensure(entries); err != nil {
		return err
	}
	if tableScan.ctr.jsonDeliveryColumns == nil {
		tableScan.ctr.jsonDeliveryColumns = make(map[int]int, len(columns))
	}
	clear(tableScan.ctr.jsonDeliveryColumns)
	requested := internalColumns
	for _, output := range columns {
		source := output
		if len(tableScan.ProjectList) != 0 {
			if output < 0 || output >= len(tableScan.ProjectList) {
				continue
			}
			column := tableScan.ProjectList[output].GetCol()
			if column == nil || column.RelPos != 0 {
				continue
			}
			source = int(column.ColPos)
		}
		tableScan.ctr.jsonDeliveryColumns[output] = source
		duplicate := false
		for _, existing := range requested {
			if existing == source {
				duplicate = true
				break
			}
		}
		if !duplicate {
			requested = append(requested, source)
		}
	}
	tableScan.ctr.jsonColumns = requested
	return nil
}

// PublishJSONInputs retains the actual Read's versions and ends its scope.
// Unsupported or missing publications are admitted as ordinary mutable input.
func (tableScan *TableScan) PublishJSONInputs(proc *process.Process, input *batch.Batch, columns []int) (versions *colexec.JSONInputVersions, err error) {
	publication := tableScan.ctr.jsonPublication
	if publication == nil {
		return colexec.PublishJSONInputSnapshotsForEvaluation(proc, input, columns)
	}
	defer func() { publication.Close(); tableScan.ctr.jsonPublication = nil }()
	if len(columns) == 0 {
		return nil, nil
	}
	versions, err = colexec.NewJSONInputVersions(proc, len(columns))
	if err != nil {
		return nil, err
	}
	readers := versions.Readers
	published := versions
	defer func() {
		if err != nil {
			colexec.CloseJSONInputVersions(published)
			versions = nil
		}
	}()
	for _, column := range columns {
		if err = proc.Ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := readers[column]; exists {
			continue
		}
		if input == nil || column < 0 || column >= len(input.Vecs) || input.Vecs[column] == nil || input.Vecs[column].GetType().Oid != types.T_json {
			return nil, moerr.NewInvalidInputNoCtx("JSON publication input column mismatch")
		}
		var source vector.JSONReader
		if storageColumn, mapped := tableScan.ctr.jsonDeliveryColumns[column]; mapped {
			source = publication.Readers()[storageColumn]
		}
		if source != nil {
			if source.Length() != input.RowCount() {
				return nil, moerr.NewInvalidInputNoCtx("published JSON row mapping mismatch")
			}
			readers[column], err = source.Window(0, source.Length())
		} else {
			allocation := tableScan.OutputAllocation
			if allocation == nil {
				allocation = input.Vecs[column].AllocationAccountSelection()
			}
			var invalid bool
			readers[column], invalid, err = vector.TryNewJSONReaderSnapshotForRows(input.Vecs[column], input.RowCount(), proc.Mp(), allocation, versions.MemoryAdmission())
			if err != nil && invalid {
				err = nil
				continue
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return versions, nil
}
