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

package spillutil

import (
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// accountedFileReader restores buffered sequential spill reads without
// reintroducing the untracked Go-heap buffer removed by allocation-owned
// admission. The buffer is optional: under allocation pressure the reader
// releases it and continues directly from the file.
type accountedFileReader struct {
	fd         *os.File
	mp         *mpool.MPool
	allocation *SpillAllocationAccount
	buffer     *mpool.AccountedBuffer

	offset     int64
	fdOffset   int64
	bufferPos  int
	pendingErr error
	disabled   bool
}

func newAccountedFileReader(
	mp *mpool.MPool,
	allocation *SpillAllocationAccount,
	fd *os.File,
) (*accountedFileReader, error) {
	if mp == nil || allocation == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	reader := &accountedFileReader{
		mp:         mp,
		allocation: allocation,
	}
	if err := reader.Reset(fd); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *accountedFileReader) Read(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if r == nil || r.fd == nil {
		return 0, io.EOF
	}
	if available := r.buffered(); available > 0 {
		if available > len(value) {
			available = len(value)
		}
		copy(value, r.buffer.Bytes()[r.bufferPos:r.bufferPos+available])
		r.bufferPos += available
		r.offset += int64(available)
		return available, nil
	}
	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil
		return 0, err
	}
	if err := r.ensureBuffer(); err != nil {
		return 0, err
	}
	if r.disabled || len(value) >= spillReadBufferSize {
		return r.readDirect(value)
	}
	if err := r.fill(); err != nil {
		return 0, err
	}
	return r.Read(value)
}

func (r *accountedFileReader) Offset() int64 {
	if r == nil {
		return 0
	}
	return r.offset
}

func (r *accountedFileReader) Reset(fd *os.File) error {
	if r == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	r.fd = fd
	r.offset = 0
	r.fdOffset = 0
	r.bufferPos = 0
	r.pendingErr = nil
	r.disabled = false
	if r.buffer != nil {
		r.buffer.Reset()
	}
	if fd == nil {
		return nil
	}
	offset, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	r.offset = offset
	r.fdOffset = offset
	return nil
}

// DisableBufferAt releases optional read-ahead capacity and restores the
// physical descriptor to the unpublished record offset before a decode retry.
func (r *accountedFileReader) DisableBufferAt(offset int64) error {
	if r == nil || r.fd == nil || offset < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	_, seekErr := r.fd.Seek(offset, io.SeekStart)
	if r.buffer != nil {
		r.buffer.Free()
		r.buffer = nil
	}
	r.offset = offset
	r.fdOffset = offset
	r.bufferPos = 0
	r.pendingErr = nil
	r.disabled = true
	return seekErr
}

func (r *accountedFileReader) Free() {
	if r == nil {
		return
	}
	if r.buffer != nil {
		r.buffer.Free()
		r.buffer = nil
	}
	r.fd = nil
	r.mp = nil
	r.allocation = nil
	r.offset = 0
	r.fdOffset = 0
	r.bufferPos = 0
	r.pendingErr = nil
	r.disabled = true
}

func (r *accountedFileReader) buffered() int {
	if r == nil || r.buffer == nil {
		return 0
	}
	return r.buffer.Len() - r.bufferPos
}

func (r *accountedFileReader) ensureBuffer() error {
	if r.disabled {
		return nil
	}
	if r.buffer == nil {
		buffer, err := r.allocation.newBuffer(
			r.mp,
			SpillAllocationSiteReadBuffer,
		)
		if err != nil {
			return err
		}
		r.buffer = buffer
	}
	if r.buffer.Cap() >= spillReadBufferSize {
		return nil
	}
	if err := r.buffer.EnsureCapacity(spillReadBufferSize); err != nil {
		if !mpool.IsRetryableAllocationCapacity(err) {
			return err
		}
		r.buffer.Free()
		r.buffer = nil
		r.disabled = true
	}
	return nil
}

func (r *accountedFileReader) fill() error {
	if r.buffer == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := r.syncFileOffset(); err != nil {
		return err
	}
	if err := r.buffer.Resize(spillReadBufferSize); err != nil {
		return err
	}
	n, err := r.fd.Read(r.buffer.Bytes())
	r.fdOffset += int64(n)
	r.bufferPos = 0
	if resizeErr := r.buffer.Resize(n); resizeErr != nil {
		return resizeErr
	}
	if n == 0 {
		if err == nil {
			return io.EOF
		}
		return err
	}
	r.pendingErr = err
	return nil
}

func (r *accountedFileReader) readDirect(value []byte) (int, error) {
	if err := r.syncFileOffset(); err != nil {
		return 0, err
	}
	n, err := r.fd.Read(value)
	r.offset += int64(n)
	r.fdOffset += int64(n)
	return n, err
}

func (r *accountedFileReader) syncFileOffset() error {
	if r.fdOffset == r.offset {
		return nil
	}
	if _, err := r.fd.Seek(r.offset, io.SeekStart); err != nil {
		return err
	}
	r.fdOffset = r.offset
	return nil
}
