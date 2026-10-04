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

package group

import (
	"context"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// groupSpillReader keeps read-ahead physical storage under the Group owner.
// The buffer is an optimization: if the statement cannot admit it, reads
// continue directly from the spill file rather than turning optional I/O
// acceleration into a query failure.
type groupSpillReader struct {
	ctr    *container
	fd     *os.File
	ctx    context.Context
	buffer reusableSpillBuffer

	bufferPos  int
	pendingErr error
	disabled   bool
	position   int64
}

func newGroupSpillReader(
	ctr *container,
	fd *os.File,
	ctx context.Context,
) (*groupSpillReader, error) {
	if ctr == nil || ctr.mp == nil || fd == nil || ctx == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return &groupSpillReader{ctr: ctr, fd: fd, ctx: ctx}, nil
}

func (r *groupSpillReader) Reset(fd *os.File) error {
	if r == nil || fd == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	r.fd = fd
	r.bufferPos = 0
	r.pendingErr = nil
	r.disabled = false
	r.position = 0
	if r.buffer != nil {
		r.buffer.Reset()
	}
	return nil
}

func (r *groupSpillReader) Read(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if r == nil || r.fd == nil {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.buffer != nil && r.bufferPos < r.buffer.Len() {
		n := copy(value, r.buffer.Bytes()[r.bufferPos:])
		r.bufferPos += n
		r.position += int64(n)
		return n, nil
	}
	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil
		return 0, err
	}
	if err := r.ensureBuffer(); err != nil {
		return 0, err
	}
	if r.disabled || len(value) >= spillIOBufSize {
		n, err := r.fd.Read(value)
		r.position += int64(n)
		return n, err
	}
	if err := r.buffer.Resize(spillIOBufSize); err != nil {
		return 0, err
	}
	n, err := r.fd.Read(r.buffer.Bytes())
	r.bufferPos = 0
	if resizeErr := r.buffer.Resize(n); resizeErr != nil {
		return 0, resizeErr
	}
	if n == 0 {
		if err == nil {
			return 0, io.EOF
		}
		return 0, err
	}
	r.pendingErr = err
	return r.Read(value)
}

func (r *groupSpillReader) Position() int64 {
	if r == nil {
		return 0
	}
	return r.position
}

// Rewind restores the logical record boundary even when the physical file
// descriptor has advanced past it through read-ahead.
func (r *groupSpillReader) Rewind(position int64) error {
	if r == nil || r.fd == nil || position < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if _, err := r.fd.Seek(position, io.SeekStart); err != nil {
		return err
	}
	if r.buffer != nil {
		r.buffer.Reset()
	}
	r.bufferPos = 0
	r.pendingErr = nil
	r.position = position
	return nil
}

// DisableReadAheadAndRewind returns optional buffer capacity before replaying
// a record under pressure. The remainder of the bucket uses direct reads.
func (r *groupSpillReader) DisableReadAheadAndRewind(position int64) (bool, error) {
	if r == nil || r.buffer == nil || r.buffer.Cap() == 0 || r.disabled {
		return false, nil
	}
	r.buffer.Free()
	r.buffer = nil
	r.disabled = true
	return true, r.Rewind(position)
}

func (r *groupSpillReader) DropReadAhead() {
	if r == nil || r.buffer == nil {
		return
	}
	r.buffer.Free()
	r.buffer = nil
	r.bufferPos = 0
	r.pendingErr = nil
}

func (r *groupSpillReader) ensureBuffer() error {
	if r.disabled {
		return nil
	}
	if r.buffer == nil {
		buffer, err := newGroupSpillBuffer(r.ctr, GroupAllocationSiteSpillRead)
		if err != nil {
			return err
		}
		r.buffer = buffer
	}
	if r.buffer.Cap() >= spillIOBufSize {
		return nil
	}
	if err := r.buffer.Resize(spillIOBufSize); err != nil {
		if !mpool.IsRetryableAllocationCapacity(err) {
			return err
		}
		r.buffer.Free()
		r.buffer = nil
		r.disabled = true
	}
	return nil
}

func (r *groupSpillReader) Free() {
	if r == nil {
		return
	}
	if r.buffer != nil {
		r.buffer.Free()
	}
	r.buffer = nil
	r.ctr = nil
	r.fd = nil
	r.ctx = nil
	r.bufferPos = 0
	r.pendingErr = nil
	r.disabled = true
	r.position = 0
}

// groupSpillWriter restores the historical 64 KiB write coalescing without a
// Go-heap buffer. At most spillNumBuckets instances are live. Its optional
// storage uses ordinary statement capacity and falls back to direct writes
// under pressure, leaving the mandatory recovery floor to spill scratch.
type groupSpillWriter struct {
	ctr      *container
	target   io.Writer
	ctx      context.Context
	buffer   reusableSpillBuffer
	disabled bool
	failed   error
}

func newGroupSpillWriter(
	ctr *container,
	target io.Writer,
	ctx context.Context,
	disk *process.ExecutionSpillDiskReservation,
) (*groupSpillWriter, error) {
	if ctr == nil || ctr.mp == nil || target == nil || ctx == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return &groupSpillWriter{
		ctr:    ctr,
		target: spillutil.NewDiskReservationWriter(target, disk),
		ctx:    ctx,
	}, nil
}

func (w *groupSpillWriter) ensureBuffer() error {
	if w.disabled || w.buffer != nil {
		return nil
	}
	buffer, err := newGroupSpillBuffer(w.ctr, GroupAllocationSiteSpillMetadata)
	if err != nil {
		return err
	}
	if err = buffer.Resize(spillWrBufSize); err != nil {
		buffer.Free()
		if mpool.IsRetryableAllocationCapacity(err) {
			// Coalescing is optional. Direct writes preserve bounded recovery
			// semantics when a deliberately tiny account cannot admit it.
			w.disabled = true
			return nil
		}
		return err
	}
	buffer.Reset()
	w.buffer = buffer
	return nil
}

func (w *groupSpillWriter) Write(value []byte) (int, error) {
	if w == nil || w.target == nil {
		return 0, io.ErrClosedPipe
	}
	if len(value) == 0 {
		return 0, nil
	}
	if w.failed != nil {
		return 0, w.failed
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.ensureBuffer(); err != nil {
		return 0, err
	}
	if w.disabled {
		return w.writePhysical(value)
	}
	written := 0
	for len(value) != 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		space := spillWrBufSize - w.buffer.Len()
		if space == 0 {
			if err := w.Flush(); err != nil {
				return written, err
			}
			space = spillWrBufSize
		}
		n := min(space, len(value))
		accepted, err := w.buffer.Write(value[:n])
		written += accepted
		value = value[accepted:]
		if err != nil {
			return written, err
		}
		if accepted != n {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// WriteSelectedFixedRows appends one sparse fixed-width selection to the
// existing coalescing buffer in bounded chunks. The bytes are identical to
// writing every selected value separately; only the call and bounds-check
// overhead on the spill hot path changes.
func (w *groupSpillWriter) WriteSelectedFixedRows(
	data []byte,
	width int,
	rows []int32,
) (int, error) {
	if w == nil || w.target == nil {
		return 0, io.ErrClosedPipe
	}
	if width < 0 || width > spillWrBufSize ||
		(width != 0 && len(data)%width != 0) {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	if width == 0 || len(rows) == 0 {
		return 0, nil
	}
	if w.failed != nil {
		return 0, w.failed
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.ensureBuffer(); err != nil {
		return 0, err
	}
	if w.disabled {
		written := 0
		for _, selected := range rows {
			row := int(selected)
			if row < 0 || row >= len(data)/width {
				return written, mpool.ErrAllocationAccountInvalid
			}
			n, err := w.writePhysical(data[row*width : (row+1)*width])
			written += n
			if err != nil {
				return written, err
			}
		}
		return written, nil
	}

	written := 0
	rowCount := len(data) / width
	for len(rows) != 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		spaceRows := (spillWrBufSize - w.buffer.Len()) / width
		if spaceRows == 0 {
			if err := w.Flush(); err != nil {
				return written, err
			}
			spaceRows = spillWrBufSize / width
		}
		chunkRows := min(spaceRows, len(rows))
		oldLength := w.buffer.Len()
		chunkBytes := chunkRows * width
		if err := w.buffer.Resize(oldLength + chunkBytes); err != nil {
			return written, err
		}
		output := w.buffer.Bytes()[oldLength:]
		for outputRow, selected := range rows[:chunkRows] {
			row := int(selected)
			if row < 0 || row >= rowCount {
				_ = w.buffer.Resize(oldLength)
				return written, mpool.ErrAllocationAccountInvalid
			}
			copy(output[outputRow*width:], data[row*width:(row+1)*width])
		}
		written += chunkBytes
		rows = rows[chunkRows:]
	}
	return written, nil
}

func (w *groupSpillWriter) Flush() error {
	if w == nil || w.target == nil {
		return nil
	}
	if w.failed != nil {
		return w.failed
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.buffer == nil || w.buffer.Len() == 0 {
		return nil
	}
	if _, err := w.writePhysical(w.buffer.Bytes()); err != nil {
		w.failed = err
		return err
	}
	w.buffer.Reset()
	return nil
}

func (w *groupSpillWriter) Free() {
	if w == nil {
		return
	}
	if w.buffer != nil {
		w.buffer.Free()
	}
	w.buffer = nil
	w.ctr = nil
	w.target = nil
	w.ctx = nil
	w.disabled = true
	w.failed = nil
}

// writePhysical admits disk capacity immediately before one physical write.
// Codec writes are coalesced above this boundary so accounting does not
// serialize every small logical fragment on the shared execution budget.
func (w *groupSpillWriter) writePhysical(value []byte) (int, error) {
	return writeGroupSpillBytes(w.target, value)
}

func writeGroupSpillBytes(target io.Writer, value []byte) (int, error) {
	n, err := target.Write(value)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return n, err
}
