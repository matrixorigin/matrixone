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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	dataBranchOutputSpoolBufferSize = 64 * 1024
	// A branch table name and diff kind are catalog identifiers, never payloads.
	// Keep the corruption guard deliberately far above any legal identifier.
	dataBranchOutputSpoolMaxMetadataSize = 1 << 20
	dataBranchOutputSpoolMagic           = uint64(0x444246464F555441) // "DBFFOUTA"
)

// dataBranchOutputSpool persists OUTPUT AS batches while the diff producer is
// reading from its transaction.  The output table is materialized only after
// the producer has completed, so the output DDL/DML never advances that
// transaction's statement state during a diff read.
type dataBranchOutputSpool struct {
	file   *os.File
	writer *bufio.Writer
	reader *bufio.Reader
	buf    bytes.Buffer

	readBatch *batch.Batch
	mp        *mpool.MPool
}

func newDataBranchOutputSpool(ctx context.Context, proc *process.Process) (*dataBranchOutputSpool, error) {
	spillFS, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	file, err := spillFS.CreateAndRemoveFile(ctx, fmt.Sprintf("data_branch_output_%s", uuid.NewString()))
	if err != nil {
		return nil, err
	}
	return &dataBranchOutputSpool{
		file:   file,
		writer: bufio.NewWriterSize(file, dataBranchOutputSpoolBufferSize),
		mp:     proc.Mp(),
	}, nil
}

func (spool *dataBranchOutputSpool) append(wrapped batchWithKind) error {
	if spool == nil || spool.writer == nil {
		return moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS spool is not writable")
	}

	spool.buf.Reset()
	if err := writeDataBranchOutputSpoolString(&spool.buf, wrapped.name); err != nil {
		return err
	}
	if err := writeDataBranchOutputSpoolString(&spool.buf, wrapped.kind); err != nil {
		return err
	}

	batchSizeOffset := spool.buf.Len()
	var batchSize uint64
	spool.buf.Write(types.EncodeUint64(&batchSize))
	batchStart := spool.buf.Len()
	if _, err := wrapped.batch.MarshalBinaryWithBuffer(&spool.buf, false); err != nil {
		return err
	}
	batchSize = uint64(spool.buf.Len() - batchStart)
	copy(spool.buf.Bytes()[batchSizeOffset:], types.EncodeUint64(&batchSize))

	magic := dataBranchOutputSpoolMagic
	spool.buf.Write(types.EncodeUint64(&magic))
	if n, err := spool.writer.Write(spool.buf.Bytes()); err != nil {
		return err
	} else if n != spool.buf.Len() {
		return io.ErrShortWrite
	}
	return nil
}

func (spool *dataBranchOutputSpool) rewind() error {
	if spool == nil || spool.file == nil || spool.writer == nil {
		return moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS spool is not ready to replay")
	}
	if err := spool.writer.Flush(); err != nil {
		return err
	}
	spool.writer = nil
	if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	spool.reader = bufio.NewReaderSize(spool.file, dataBranchOutputSpoolBufferSize)
	return nil
}

func (spool *dataBranchOutputSpool) next() (batchWithKind, bool, error) {
	if spool == nil || spool.reader == nil {
		return batchWithKind{}, false, moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS spool is not readable")
	}

	name, err := readDataBranchOutputSpoolString(spool.reader)
	if errors.Is(err, io.EOF) {
		return batchWithKind{}, false, nil
	}
	if err != nil {
		return batchWithKind{}, false, err
	}
	kind, err := readDataBranchOutputSpoolString(spool.reader)
	if err != nil {
		return batchWithKind{}, false, err
	}
	batchSize, err := types.ReadUint64(spool.reader)
	if err != nil {
		return batchWithKind{}, false, err
	}
	if batchSize > math.MaxInt64 {
		return batchWithKind{}, false, moerr.NewInternalErrorNoCtxf(
			"DATA BRANCH OUTPUT AS spool batch is too large: %d", batchSize,
		)
	}

	if spool.readBatch == nil {
		spool.readBatch = batch.NewWithSize(0)
	} else {
		spool.readBatch.CleanOnlyData()
	}
	limitedReader := &io.LimitedReader{R: spool.reader, N: int64(batchSize)}
	if err = spool.readBatch.UnmarshalFromReader(limitedReader, spool.mp); err != nil {
		return batchWithKind{}, false, err
	}
	if limitedReader.N != 0 {
		return batchWithKind{}, false, moerr.NewInternalErrorNoCtxf(
			"DATA BRANCH OUTPUT AS spool batch did not consume %d bytes", limitedReader.N,
		)
	}
	magic, err := types.ReadUint64(spool.reader)
	if err != nil {
		return batchWithKind{}, false, err
	}
	if magic != dataBranchOutputSpoolMagic {
		return batchWithKind{}, false, moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS spool is corrupted")
	}
	return batchWithKind{name: name, kind: kind, batch: spool.readBatch}, true, nil
}

func (spool *dataBranchOutputSpool) close() error {
	if spool == nil {
		return nil
	}
	var err error
	if spool.writer != nil {
		err = errors.Join(err, spool.writer.Flush())
		spool.writer = nil
	}
	if spool.file != nil {
		err = errors.Join(err, spool.file.Close())
		spool.file = nil
	}
	if spool.readBatch != nil {
		spool.readBatch.Clean(spool.mp)
		spool.readBatch = nil
	}
	return err
}

func writeDataBranchOutputSpoolString(buf *bytes.Buffer, value string) error {
	if len(value) > dataBranchOutputSpoolMaxMetadataSize {
		return moerr.NewInternalErrorNoCtxf("DATA BRANCH OUTPUT AS spool metadata is too large: %d", len(value))
	}
	size := int32(len(value))
	buf.Write(types.EncodeInt32(&size))
	buf.WriteString(value)
	return nil
}

func readDataBranchOutputSpoolString(reader io.Reader) (string, error) {
	size, err := types.ReadInt32AsInt(reader)
	if err != nil {
		return "", err
	}
	if size < 0 {
		return "", moerr.NewInternalErrorNoCtxf("DATA BRANCH OUTPUT AS spool has negative string length: %d", size)
	}
	if size > dataBranchOutputSpoolMaxMetadataSize {
		return "", moerr.NewInternalErrorNoCtxf("DATA BRANCH OUTPUT AS spool metadata is too large: %d", size)
	}
	value := make([]byte, size)
	if _, err = io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return string(value), nil
}

// dataBranchOutputAsTablePhase owns the spill file and records the sole
// transition that permits output-table DDL/DML: all diff producer work has
// finished.  It is deliberately separate from the spool drainer because the
// latter runs concurrently with the producer.
type dataBranchOutputAsTablePhase struct {
	spool *dataBranchOutputSpool
	done  chan struct{}
	once  sync.Once
}

func newDataBranchOutputAsTablePhase(ctx context.Context, proc *process.Process) (*dataBranchOutputAsTablePhase, error) {
	spool, err := newDataBranchOutputSpool(ctx, proc)
	if err != nil {
		return nil, err
	}
	return &dataBranchOutputAsTablePhase{
		spool: spool,
		done:  make(chan struct{}),
	}, nil
}

func (phase *dataBranchOutputAsTablePhase) drain(
	ctx context.Context,
	cancel context.CancelFunc,
	retPool *retBatchList,
	retCh <-chan batchWithKind,
) (firstErr error) {
	for wrapped := range retCh {
		if firstErr == nil && ctx.Err() != nil {
			firstErr = ctx.Err()
		}
		if firstErr == nil {
			firstErr = phase.spool.append(wrapped)
		}
		retPool.releaseRetBatch(wrapped.batch, false)
		if firstErr != nil {
			cancel()
		}
	}
	return firstErr
}

func (phase *dataBranchOutputAsTablePhase) markProducerDone() {
	phase.once.Do(func() {
		close(phase.done)
	})
}

func (phase *dataBranchOutputAsTablePhase) materialize(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) error {
	select {
	case <-phase.done:
	default:
		return moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS materialization started before diff production completed")
	}
	if err := phase.spool.rewind(); err != nil {
		return err
	}
	return materializeDiffOutputAsTable(ctx, cancel, ses, bh, stmt, tblStuff, phase.spool)
}

func (phase *dataBranchOutputAsTablePhase) close() error {
	if phase == nil {
		return nil
	}
	return phase.spool.close()
}
