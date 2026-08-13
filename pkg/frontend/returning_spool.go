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

package frontend

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	returningSpoolBufferSize = 64 * 1024
	returningSpoolMaxBatch   = 64 << 20
	returningSpoolMagic      = uint64(0x444D4C5245545552) // "DMLRETUR"
)

type returningSpoolState uint8

const (
	returningSpoolIdle returningSpoolState = iota
	returningSpoolWriting
	returningSpoolSealed
	returningSpoolClosed
)

type returningSpool struct {
	mu sync.Mutex

	state      returningSpoolState
	generation uint64
	rows       uint64
	file       *os.File
	writer     *bufio.Writer
	// bytes.Buffer is Go-heap storage. The exact HashBuild allocation account
	// covers allocator-visible MPool capacity only, so this bounded buffer must
	// not create an estimated charge in that ledger.
	buf bytes.Buffer
	mp  *mpool.MPool

	diskReservation *process.HashBuildSpillDiskReservation
	fdReservation   *process.HashBuildSpillFDReservation
}

func (s *returningSpool) BeginAttempt(ctx context.Context, generation uint64, proc *process.Process) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == returningSpoolClosed {
		return moerr.NewInternalError(ctx, "DML RETURNING spool is closed")
	}
	if s.state != returningSpoolIdle {
		return moerr.NewInternalError(ctx, "DML RETURNING spool already has an active generation")
	}
	budget, err := proc.GetHashBuildBudget()
	if err != nil {
		return err
	}
	disk, err := budget.ReserveSpillDisk(0)
	if err != nil {
		return err
	}
	fd, err := budget.ReserveSpillFD(1)
	if err != nil {
		disk.Release()
		return err
	}
	spillFS, err := proc.GetSpillFileService()
	if err != nil {
		fd.Release()
		disk.Release()
		return err
	}
	file, err := spillFS.CreateAndRemoveFile(ctx, fmt.Sprintf("dml_returning_%s", uuid.NewString()))
	if err != nil {
		fd.Release()
		disk.Release()
		return err
	}
	s.file = file
	s.writer = bufio.NewWriterSize(file, returningSpoolBufferSize)
	s.diskReservation = disk
	s.fdReservation = fd
	s.mp = proc.Mp()
	s.generation = generation
	s.rows = 0
	s.state = returningSpoolWriting
	return nil
}

func (s *returningSpool) Write(generation uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != returningSpoolWriting || generation != s.generation {
		return moerr.NewInternalErrorNoCtxf("DML RETURNING write generation mismatch: got %d, active %d", generation, s.generation)
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	estimated, err := estimateReturningBatchBytes(bat)
	if err != nil {
		return err
	}
	s.buf.Reset()
	s.buf.Grow(int(estimated))
	var size uint64
	s.buf.Write(types.EncodeUint64(&size))
	payloadStart := s.buf.Len()
	if _, err = bat.MarshalBinaryWithPrepareParamKinds(&s.buf, false); err != nil {
		return err
	}
	payloadSize := s.buf.Len() - payloadStart
	if payloadSize > returningSpoolMaxBatch {
		return moerr.NewInternalErrorNoCtxf("DML RETURNING batch exceeds %d bytes", returningSpoolMaxBatch)
	}
	size = uint64(payloadSize)
	copy(s.buf.Bytes()[:8], types.EncodeUint64(&size))
	magic := returningSpoolMagic
	s.buf.Write(types.EncodeUint64(&magic))
	rows := uint64(bat.RowCount())
	if math.MaxUint64-s.rows < rows {
		return moerr.NewInternalErrorNoCtx("DML RETURNING row count overflow")
	}
	s.buf.Write(types.EncodeUint64(&rows))

	oldSize := s.diskReservation.Size()
	if err := s.diskReservation.Grow(uint64(s.buf.Len())); err != nil {
		return err
	}
	if n, err := s.writer.Write(s.buf.Bytes()); err != nil {
		_, _ = s.diskReservation.ReconcileDown(oldSize)
		return err
	} else if n != s.buf.Len() {
		_, _ = s.diskReservation.ReconcileDown(oldSize)
		return io.ErrShortWrite
	}
	s.rows += rows
	return nil
}

func (s *returningSpool) SealAttempt(generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != returningSpoolWriting || generation != s.generation {
		return moerr.NewInternalErrorNoCtxf("DML RETURNING seal generation mismatch: got %d, active %d", generation, s.generation)
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}
	s.buf = bytes.Buffer{}
	s.writer = nil
	s.state = returningSpoolSealed
	return nil
}

func (s *returningSpool) AbortAttempt(generation uint64, _ error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != returningSpoolWriting || generation != s.generation {
		return moerr.NewInternalErrorNoCtxf("DML RETURNING abort generation mismatch: got %d, active %d", generation, s.generation)
	}
	err := s.releaseAttemptLocked()
	s.state = returningSpoolIdle
	return err
}

func (s *returningSpool) RowCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

func (s *returningSpool) Replay(ctx context.Context, consume func(*batch.Batch, *perfcounter.CounterSet) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != returningSpoolSealed || s.file == nil {
		return moerr.NewInternalError(ctx, "DML RETURNING spool is not sealed")
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(s.file, returningSpoolBufferSize)
	readBatch := batch.NewWithSize(0)
	defer readBatch.Clean(s.mp)
	var rows uint64
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		var size uint64
		size, err = types.ReadUint64(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if size > returningSpoolMaxBatch {
			return moerr.NewInternalErrorf(ctx, "DML RETURNING spool batch is too large: %d", size)
		}
		readBatch.CleanOnlyData()
		limited := &io.LimitedReader{R: reader, N: int64(size)}
		if err = readBatch.UnmarshalFromReaderWithPrepareParamKinds(limited, int64(size), s.mp); err != nil {
			return err
		}
		if limited.N != 0 {
			return moerr.NewInternalErrorf(ctx, "DML RETURNING spool batch left %d bytes", limited.N)
		}
		magic, err := types.ReadUint64(reader)
		if err != nil {
			return err
		}
		if magic != returningSpoolMagic {
			return moerr.NewInternalError(ctx, "DML RETURNING spool is corrupted")
		}
		recordRows, err := types.ReadUint64(reader)
		if err != nil {
			return err
		}
		if recordRows != uint64(readBatch.RowCount()) {
			return moerr.NewInternalError(ctx, "DML RETURNING spool row count is corrupted")
		}
		if math.MaxUint64-rows < recordRows {
			return moerr.NewInternalError(ctx, "DML RETURNING spool replay row count overflow")
		}
		rows += recordRows
		// Replay runs after compile analysis has finished, so its protocol output
		// wait belongs to the statement root rather than an operator counter.
		if err = consume(readBatch, nil); err != nil {
			return err
		}
	}
	if rows != s.rows {
		return moerr.NewInternalErrorf(ctx, "DML RETURNING spool row count mismatch: sealed %d, replayed %d", s.rows, rows)
	}
	return nil
}

func (s *returningSpool) releaseAttemptLocked() error {
	var err error
	if s.writer != nil {
		// A sealed attempt has already flushed and cleared writer. Reaching this
		// branch means the generation is being aborted or closed, so buffered
		// bytes are deliberately discarded instead of turning cleanup into an
		// unnecessary write failure.
		s.writer = nil
	}
	if s.file != nil {
		err = errors.Join(err, s.file.Close())
		s.file = nil
	}
	if s.diskReservation != nil {
		s.diskReservation.Release()
		s.diskReservation = nil
	}
	if s.fdReservation != nil {
		s.fdReservation.Release()
		s.fdReservation = nil
	}
	s.buf = bytes.Buffer{}
	s.rows = 0
	return err
}

func estimateReturningBatchBytes(bat *batch.Batch) (uint64, error) {
	bytes := uint64(returningSpoolBufferSize + 32 + len(bat.Vecs)*64 + len(bat.ExtraBuf))
	metadataSize, err := bat.PrepareParamKindMetadataSize()
	if err != nil {
		return 0, err
	}
	if math.MaxUint64-bytes < uint64(metadataSize) {
		return 0, moerr.NewInternalErrorNoCtx("DML RETURNING batch size overflow")
	}
	bytes += uint64(metadataSize)
	for _, attr := range bat.Attrs {
		if math.MaxUint64-bytes < uint64(len(attr)+4) {
			return 0, moerr.NewInternalErrorNoCtx("DML RETURNING batch size overflow")
		}
		bytes += uint64(len(attr) + 4)
	}
	dataBytes := bat.Size()
	if allocated := bat.Allocated(); allocated > dataBytes {
		dataBytes = allocated
	}
	if dataBytes < 0 || math.MaxUint64-bytes < uint64(dataBytes) {
		return 0, moerr.NewInternalErrorNoCtx("DML RETURNING batch size overflow")
	}
	bytes += uint64(dataBytes)
	if bytes > returningSpoolMaxBatch+returningSpoolBufferSize+24 {
		return 0, moerr.NewInternalErrorNoCtxf("DML RETURNING batch exceeds %d bytes", returningSpoolMaxBatch)
	}
	return bytes, nil
}

func (s *returningSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == returningSpoolClosed {
		return nil
	}
	err := s.releaseAttemptLocked()
	s.state = returningSpoolClosed
	return err
}

type returningState struct {
	spool        *returningSpool
	columns      []any
	affectedRows uint64
	stagedSaver  StagedBinaryWriter
}

func (s *returningState) Close(execCtx *ExecCtx) error {
	if s == nil {
		return nil
	}
	var err error
	if s.stagedSaver != nil && execCtx != nil {
		err = s.stagedSaver.Abort(execCtx)
	}
	if s.spool != nil {
		err = errors.Join(err, s.spool.Close())
	}
	return err
}
