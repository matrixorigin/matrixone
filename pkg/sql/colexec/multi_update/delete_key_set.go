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

package multi_update

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/maphash"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// deleteKeySet retains transaction-local row-id deduplication on disk. Its
// bucket heads are sparse file pages rather than a growing Go map/bitmap.
// Randomized hashing avoids a data-controlled fixed-bucket collision pattern.
// Only one fixed record buffer is live per lookup, regardless of row count.
type deleteKeySet struct {
	file *colexec.MetadataSpool
	seed maphash.Seed
	end  int64
}

const deleteKeyBuckets = 1 << 16
const deleteKeyHeadBytes = deleteKeyBuckets * 8
const deleteKeyRecordBytes = 8 + types.RowidSize

func (s *deleteKeySet) add(proc *process.Process, key types.Rowid) (bool, error) {
	ctx := proc.Ctx
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	if s.file == nil {
		f, err := colexec.NewMetadataSpool(proc)
		if err != nil {
			return false, err
		}
		if err = f.Reserve(deleteKeyHeadBytes); err != nil {
			f.Close()
			return false, err
		}
		if err = f.Truncate(deleteKeyHeadBytes); err != nil {
			f.Close()
			return false, err
		}
		s.file = f
		s.seed = maphash.MakeSeed()
		s.end = deleteKeyHeadBytes
	}
	bucket := int64(maphash.Bytes(s.seed, key[:])&(deleteKeyBuckets-1)) * 8
	var head [8]byte
	if _, err := s.file.ReadAt(head[:], bucket); err != nil {
		return false, err
	}
	start := binary.LittleEndian.Uint64(head[:])
	for next := start; next != 0; {
		if err := context.Cause(ctx); err != nil {
			return false, err
		}
		var rec [deleteKeyRecordBytes]byte
		if _, err := s.file.ReadAt(rec[:], int64(next)); err != nil {
			return false, err
		}
		if bytes.Equal(rec[8:], key[:]) {
			return false, nil
		}
		next = binary.LittleEndian.Uint64(rec[:8])
	}
	if err := s.file.Reserve(deleteKeyRecordBytes); err != nil {
		return false, err
	}
	var rec [deleteKeyRecordBytes]byte
	binary.LittleEndian.PutUint64(rec[:8], start)
	copy(rec[8:], key[:])
	if n, err := s.file.WriteAt(rec[:], s.end); err != nil {
		return false, err
	} else if n != len(rec) {
		return false, io.ErrShortWrite
	}
	binary.LittleEndian.PutUint64(head[:], uint64(s.end))
	if n, err := s.file.WriteAt(head[:], bucket); err != nil {
		return false, err
	} else if n != len(head) {
		return false, io.ErrShortWrite
	}
	s.end += int64(len(rec))
	return true, nil
}

func (s *deleteKeySet) close() error {
	if s == nil || s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	s.end = 0
	return err
}
