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

package colexec

import (
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MetadataSpool owns an unlinked LOCAL spill file and its statement/CN storage
// reservations. The operator reserves each complete append before writing it.
// Reservations remain charged through replay and are retired only by Close.
type MetadataSpool struct {
	*os.File
	disk *process.HashBuildSpillDiskReservation
	fd   *process.HashBuildSpillFDReservation
}

func NewMetadataSpool(proc *process.Process) (_ *MetadataSpool, err error) {
	budget, err := proc.GetHashBuildBudget()
	if err != nil {
		return nil, err
	}
	s := &MetadataSpool{}
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	if s.disk, err = budget.ReserveSpillDisk(0); err != nil {
		return nil, err
	}
	if s.fd, err = budget.ReserveSpillFD(1); err != nil {
		return nil, err
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	s.File, err = fs.CreateAndRemoveFile(proc.Ctx, "import_metadata_"+uuid.NewString())
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *MetadataSpool) Reserve(bytes uint64) error {
	if s.disk == nil {
		return os.ErrClosed
	}
	return s.disk.Grow(bytes)
}

func (s *MetadataSpool) Close() (err error) {
	if s.disk == nil && s.fd == nil {
		return nil
	}
	if s.File != nil {
		err = s.File.Close()
	}
	s.disk.Release()
	s.disk = nil
	s.fd.Release()
	s.fd = nil
	return err
}
