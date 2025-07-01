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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	// spillFileCounter is used to generate unique file names for spilled data.
	spillFileCounter atomic.Uint64
)

// Spiller manages spilling data to disk for the group operator.
type Spiller struct {
	fs         fileservice.MutableFileService
	spillFiles []string
	proc       *process.Process
}

func NewSpiller(proc *process.Process) (*Spiller, error) {
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	return &Spiller{
		fs:   fs,
		proc: proc,
	}, nil
}

// spill writes a batch to a new temporary spill file.
func (s *Spiller) spillBatch(bat *batch.Batch) error {
	if bat.IsEmpty() {
		return nil
	}

	filePath := fmt.Sprintf("group_spill_%s_%d.bin", s.proc.QueryId(), spillFileCounter.Add(1))

	data, err := bat.MarshalBinary()
	if err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to marshal batch for spilling: %v", err)
	}

	vec := fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	}

	if err = s.fs.Write(s.proc.Ctx, vec); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write spill file %s: %v", filePath, err)
	}

	s.spillFiles = append(s.spillFiles, filePath)
	return nil
}

// getReaders returns a list of readers for all spilled files.
func (s *Spiller) getSpillFiles() []string {
	return s.spillFiles
}

// clean deletes all temporary spill files.
func (s *Spiller) clean() error {
	var lastErr error
	for _, filePath := range s.spillFiles {
		if err := s.fs.Delete(s.proc.Ctx, filePath); err != nil {
			lastErr = moerr.NewInternalErrorf(s.proc.Ctx, "failed to delete spill file %s: %v", filePath, err)
		}
	}
	s.spillFiles = nil
	return lastErr
}

// spillState writes the serialized hashmap, aggregation states, and group-by batch to a new temporary spill file.
func (s *Spiller) spillState(hashmapData []byte, aggStates [][]byte, groupByBatchesData [][]byte) error {
	filePath := fmt.Sprintf("group_spill_state_%s_%d.bin", s.proc.QueryId(), spillFileCounter.Add(1))

	var buffer bytes.Buffer
	// Write lengths of each component
	if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(hashmapData))); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write hashmapData length for spilling: %v", err)
	}
	if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(aggStates))); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write aggStates count for spilling: %v", err)
	}
	for _, aggData := range aggStates {
		if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(aggData))); err != nil {
			return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write aggData length for spilling: %v", err)
		}
	}
	if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(groupByBatchesData))); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write groupByBatchesData count for spilling: %v", err)
	}
	for _, batchData := range groupByBatchesData {
		if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(batchData))); err != nil {
			return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write groupByBatchData length for spilling: %v", err)
		}
	}

	// Write data of each component
	buffer.Write(hashmapData)
	for _, aggData := range aggStates {
		buffer.Write(aggData)
	}
	for _, batchData := range groupByBatchesData {
		buffer.Write(batchData)
	}

	vec := fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(buffer.Len()),
				Data:   buffer.Bytes(),
			},
		},
	}

	if err := s.fs.Write(s.proc.Ctx, vec); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to write spill file %s: %v", filePath, err)
	}

	s.spillFiles = append(s.spillFiles, filePath)
	return nil
}

// recallState reads a spill file and returns the serialized hashmap, aggregation states, and group-by batch.
func (s *Spiller) recallState(filePath string) ([]byte, [][]byte, [][]byte, error) {
	vec := fileservice.IOVector{
		FilePath: filePath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}}, // Read entire file
	}

	if err := s.fs.Read(s.proc.Ctx, &vec); err != nil {
		return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read spill file %s: %v", filePath, err)
	}

	reader := bytes.NewReader(vec.Entries[0].Data)

	var hashmapLen, aggStatesCount, groupByBatchesCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &hashmapLen); err != nil {
		return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read hashmap length from spill file: %v", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &aggStatesCount); err != nil {
		return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read aggStates count from spill file: %v", err)
	}

	aggLens := make([]uint64, aggStatesCount)
	for i := range aggLens {
		if err := binary.Read(reader, binary.LittleEndian, &aggLens[i]); err != nil {
			return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read aggData length from spill file: %v", err)
		}
	}
	if err := binary.Read(reader, binary.LittleEndian, &groupByBatchesCount); err != nil {
		return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read groupByBatches count from spill file: %v", err)
	}
	groupByBatchLens := make([]uint64, groupByBatchesCount)
	for i := range groupByBatchLens {
		if err := binary.Read(reader, binary.LittleEndian, &groupByBatchLens[i]); err != nil {
			return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read groupByBatchData length from spill file: %v", err)
		}
	}

	hashmapData := make([]byte, hashmapLen)
	if _, err := io.ReadFull(reader, hashmapData); err != nil {
		return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read hashmap data from spill file: %v", err)
	}

	aggStates := make([][]byte, aggStatesCount)
	for i, l := range aggLens {
		aggStates[i] = make([]byte, l)
		if _, err := io.ReadFull(reader, aggStates[i]); err != nil {
			return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read aggStates data from spill file: %v", err)
		}
	}

	groupByBatchesData := make([][]byte, groupByBatchesCount)
	for i, l := range groupByBatchLens {
		groupByBatchesData[i] = make([]byte, l)
		if _, err := io.ReadFull(reader, groupByBatchesData[i]); err != nil {
			return nil, nil, nil, moerr.NewInternalErrorf(s.proc.Ctx, "failed to read groupByBatch data from spill file: %v", err)
		}
	}
	return hashmapData, aggStates, groupByBatchesData, nil
}

// DeleteFile deletes a specific temporary spill file.
func (s *Spiller) DeleteFile(filePath string) error {
	if err := s.fs.Delete(s.proc.Ctx, filePath); err != nil {
		return moerr.NewInternalErrorf(s.proc.Ctx, "failed to delete spill file %s: %v", filePath, err)
	}
	// Remove the file from the list of spilled files
	s.spillFiles = slices.DeleteFunc(s.spillFiles, func(f string) bool {
		return f == filePath
	})
	return nil
}
