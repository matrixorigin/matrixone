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

package spillutil

import (
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// AccountedWriter coalesces small spill writes in one optional off-heap
// buffer. A capacity rejection disables coalescing and preserves progress via
// direct writes; all other errors remain terminal.
type AccountedWriter struct {
	target     io.Writer
	ctx        context.Context
	bufferSize int
	buffer     *mpool.AccountedBuffer
	disabled   bool
	failed     error
}

// NewAccountedWriter constructs an optional spill coalescer. A nil account
// retains no scratch and writes directly; an account makes every retained
// buffer byte use the supplied owner and site.
func NewAccountedWriter(
	ctx context.Context,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	site mpool.AllocationSite,
	target io.Writer,
	bufferSize int,
) (*AccountedWriter, error) {
	if ctx == nil || target == nil || bufferSize <= 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	writer := &AccountedWriter{
		target:     target,
		ctx:        ctx,
		bufferSize: bufferSize,
		disabled:   account == nil,
	}
	if account == nil {
		return writer, nil
	}
	buffer, err := mpool.NewAccountedBuffer(mp, account, owner, site)
	if err != nil {
		return nil, err
	}
	writer.buffer = buffer
	return writer, nil
}

func (w *AccountedWriter) ensureBuffer() error {
	if w.disabled || w.buffer.Cap() >= w.bufferSize {
		return nil
	}
	if err := w.buffer.EnsureCapacity(w.bufferSize); err != nil {
		if !mpool.IsRetryableAllocationCapacity(err) {
			return err
		}
		w.buffer.Free()
		w.buffer = nil
		w.disabled = true
	}
	return nil
}

func (w *AccountedWriter) Write(value []byte) (int, error) {
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
		return writeAll(w.target, value)
	}
	written := 0
	for len(value) > 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		space := w.bufferSize - w.buffer.Len()
		if space == 0 {
			if err := w.Flush(); err != nil {
				return written, err
			}
			space = w.bufferSize
		}
		chunk := min(space, len(value))
		accepted, err := w.buffer.Write(value[:chunk])
		written += accepted
		value = value[accepted:]
		if err != nil {
			return written, err
		}
		if accepted != chunk {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (w *AccountedWriter) Flush() error {
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
	if _, err := writeAll(w.target, w.buffer.Bytes()); err != nil {
		w.failed = err
		return err
	}
	w.buffer.Reset()
	return nil
}

// Free releases the optional buffer without flushing unpublished bytes.
func (w *AccountedWriter) Free() {
	if w == nil {
		return
	}
	if w.buffer != nil {
		w.buffer.Free()
	}
	w.buffer = nil
	w.target = nil
	w.ctx = nil
	w.bufferSize = 0
	w.disabled = true
	w.failed = nil
}

func writeAll(target io.Writer, value []byte) (int, error) {
	written, err := target.Write(value)
	if written < 0 {
		written = 0
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if written > len(value) {
		written = len(value)
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if err == nil && written != len(value) {
		err = io.ErrShortWrite
	}
	return written, err
}
