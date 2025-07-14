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

package group

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type SpillManager struct {
	// proc is the process context
	proc *process.Process

	// fileService is used for spill file operations
	fileService fileservice.ReaderWriterFileService

	// spillFiles keeps track of spilled file paths
	spillFiles []string

	// fileCounter is used to generate unique file names
	fileCounter int

	// groupByTypes are the types of group by columns
	groupByTypes []types.Type

	// aggInfos contain information about aggregation functions
	aggInfos []aggexec.AggFuncExecExpression
}

// NewSpillManager creates a new SpillManager
func NewSpillManager(proc *process.Process, groupByTypes []types.Type, aggInfos []aggexec.AggFuncExecExpression) (*SpillManager, error) {
	manager := &SpillManager{
		proc:         proc,
		spillFiles:   make([]string, 0),
		fileCounter:  0,
		groupByTypes: groupByTypes,
		aggInfos:     aggInfos,
	}

	// Try to get a ReaderWriterFileService from the process
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	if rwfs, ok := fs.(fileservice.ReaderWriterFileService); !ok {
		return nil, moerr.NewInternalErrorNoCtxf("%T is not ReaderWriterFileService", fs)
	} else {
		manager.fileService = rwfs
	}

	return manager, nil
}

// SpillToDisk serializes the current group results to disk
func (sm *SpillManager) SpillToDisk(groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	filePath, err := sm.generateSpillFilePath()
	if err != nil {
		return err
	}

	writer, err := sm.fileService.NewWriter(context.Background(), filePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Count non-empty groups
	nonEmptyGroupCount := int32(0)
	for _, groupBatch := range groups {
		if groupBatch != nil && groupBatch.RowCount() > 0 {
			nonEmptyGroupCount++
		}
	}

	// Write header information
	header := struct {
		GroupCount int32
		AggCount   int32
	}{
		GroupCount: nonEmptyGroupCount,
		AggCount:   int32(len(aggs)),
	}

	if err := binary.Write(writer, binary.BigEndian, header); err != nil {
		return err
	}

	// Serialize group data
	for _, groupBatch := range groups {
		if groupBatch == nil || groupBatch.RowCount() == 0 {
			continue
		}

		// Serialize the batch
		data, err := groupBatch.MarshalBinary()
		if err != nil {
			return err
		}

		// Write batch size followed by batch data
		size := int32(len(data))
		if err := binary.Write(writer, binary.BigEndian, size); err != nil {
			return err
		}
		if _, err := writer.Write(data); err != nil {
			return err
		}
	}

	// Serialize aggregation states
	for _, agg := range aggs {
		if agg == nil {
			continue
		}

		// Serialize aggregation state
		aggData, err := aggexec.MarshalAggFuncExec(agg)
		if err != nil {
			return err
		}

		// Write aggregation size followed by data
		size := int32(len(aggData))
		if err := binary.Write(writer, binary.BigEndian, size); err != nil {
			return err
		}
		if _, err := writer.Write(aggData); err != nil {
			return err
		}
	}

	// Add file to spill list
	sm.spillFiles = append(sm.spillFiles, filePath)

	return nil
}

func (sm *SpillManager) generateSpillFilePath() (string, error) {
	sm.fileCounter++
	fileName := fmt.Sprintf("group_spill_%d_%d.dat", sm.proc.GetUnixTime(), sm.fileCounter)
	return fileName, nil
}

// ReadSpilledData reads data from a spill file
func (sm *SpillManager) ReadSpilledData(filePath string) ([]*batch.Batch, []aggexec.AggFuncExec, error) {
	reader, err := sm.fileService.NewReader(context.Background(), filePath)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()

	// Read header
	var header struct {
		GroupCount int32
		AggCount   int32
	}

	if err := binary.Read(reader, binary.BigEndian, &header); err != nil {
		return nil, nil, err
	}

	// Validate header values to prevent resource exhaustion
	const maxReasonableCount = 1000000 // 1M entries should be reasonable
	if header.GroupCount < 0 || header.GroupCount > maxReasonableCount {
		return nil, nil, moerr.NewInternalErrorf(sm.proc.Ctx, "invalid group count in spill file: %d", header.GroupCount)
	}
	if header.AggCount < 0 || header.AggCount > maxReasonableCount {
		return nil, nil, moerr.NewInternalErrorf(sm.proc.Ctx, "invalid agg count in spill file: %d", header.AggCount)
	}

	var groups []*batch.Batch
	var aggs []aggexec.AggFuncExec

	// Cleanup function for error cases
	cleanup := func() {
		for _, batch := range groups {
			if batch != nil {
				batch.Clean(sm.proc.Mp())
			}
		}
		for _, agg := range aggs {
			if agg != nil {
				agg.Free()
			}
		}
	}

	// Ensure cleanup happens even if we return early
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	// Read group batches
	for i := int32(0); i < header.GroupCount; i++ {
		var size int32
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			return nil, nil, err
		}

		// Validate size to prevent excessive memory allocation
		if size < 0 || size > 100*1024*1024 { // 100MB per batch should be reasonable
			return nil, nil, moerr.NewInternalErrorf(sm.proc.Ctx, "invalid batch size in spill file: %d", size)
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, nil, err
		}

		bat := batch.NewWithSize(0)
		if err := bat.UnmarshalBinary(data); err != nil {
			bat.Clean(sm.proc.Mp()) // Clean the batch if unmarshaling fails
			return nil, nil, err
		}
		groups = append(groups, bat)
	}

	// Read aggregation states
	for i := int32(0); i < header.AggCount; i++ {
		var size int32
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			return nil, nil, err
		}

		// Validate size to prevent excessive memory allocation
		if size < 0 || size > 100*1024*1024 { // 100MB per agg should be reasonable
			return nil, nil, moerr.NewInternalErrorf(sm.proc.Ctx, "invalid agg size in spill file: %d", size)
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, nil, err
		}

		agg, err := aggexec.UnmarshalAggFuncExec(sm.proc, data)
		if err != nil {
			return nil, nil, err
		}
		aggs = append(aggs, agg)
	}

	// Clear the error so cleanup doesn't run unnecessarily
	err = nil
	return groups, aggs, nil
}

// Cleanup removes all spill files
func (sm *SpillManager) Cleanup(ctx context.Context) error {
	_ = sm.fileService.Delete(ctx, sm.spillFiles...)
	sm.spillFiles = sm.spillFiles[:0]
	return nil
}

// SpillFileCount returns the number of spilled files
func (sm *SpillManager) SpillFileCount() int {
	return len(sm.spillFiles)
}

// HasSpilledData returns true if there is any spilled data
func (sm *SpillManager) HasSpilledData() bool {
	return len(sm.spillFiles) > 0
}

// GetSpillFileCount returns the number of spilled files
func (sm *SpillManager) GetSpillFileCount() int {
	return len(sm.spillFiles)
}

func (sm *SpillManager) Size() int64 {
	var size int64
	// Account for spill file paths
	for _, filePath := range sm.spillFiles {
		size += int64(len(filePath))
	}
	// Account for groupByTypes
	for _, typ := range sm.groupByTypes {
		size += int64(typ.ProtoSize())
	}
	// Account for aggInfos (approximate)
	size += int64(len(sm.aggInfos) * 64) // Approximate size per agg info
	// Account for fileCounter (negligible but included for completeness)
	size += 8
	return size
}

// GetGroupByTypes returns the group by column types
func (sm *SpillManager) GetGroupByTypes() []types.Type {
	return sm.groupByTypes
}

// GetAggInfos returns the aggregation function information
func (sm *SpillManager) GetAggInfos() []aggexec.AggFuncExecExpression {
	return sm.aggInfos
}
