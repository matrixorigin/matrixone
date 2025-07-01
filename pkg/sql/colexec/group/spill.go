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
	"fmt"
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
func (s *Spiller) spill(bat *batch.Batch) error {
	if bat.IsEmpty() {
		return nil
	}

	filePath := fmt.Sprintf("group_spill_%d_%d.bin", s.proc.QueryId(), spillFileCounter.Add(1))

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
