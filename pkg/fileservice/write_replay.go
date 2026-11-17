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

package fileservice

import (
	"context"
	"io"
)

// replayDataReader is used only after canReplayWriteEntries has validated
// contiguous, in-memory entries. It keeps a cursor instead of cloning every
// entry, and emits at most the bytes actually consumed by the upload.
type replayDataReader struct {
	ctx       context.Context
	entries   []IOEntry
	index     int
	offset    int
	remaining int64
}

func (r *replayDataReader) Read(dst []byte) (n int, err error) {
	for {
		select {
		case <-r.ctx.Done():
			return n, r.ctx.Err()
		default:
		}
		if r.remaining <= 0 {
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}
		if r.index == len(r.entries) {
			return n, io.ErrUnexpectedEOF
		}
		if len(dst) == 0 {
			return n, nil
		}
		data := r.entries[r.index].Data[r.offset:]
		if len(data) == 0 {
			r.index++
			r.offset = 0
			continue
		}
		if int64(len(data)) > r.remaining {
			data = data[:int(r.remaining)]
		}
		copied := copy(dst, data)
		r.offset += copied
		r.remaining -= int64(copied)
		n += copied
		dst = dst[copied:]
	}
}

// WriteTo writes large contiguous spans directly and coalesces small spans in
// the existing bounded I/O buffer pool. This keeps fragmented vectors from
// issuing one system call per entry without splitting large spans needlessly.
func (r *replayDataReader) WriteTo(dst io.Writer) (written int64, err error) {
	var scratch []byte
	put := ioBufferPool.Get(&scratch)
	defer put.Put()
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		n, writeErr := dst.Write(scratch[:pending])
		if n < 0 || n > pending {
			return io.ErrShortWrite
		}
		written += int64(n)
		if writeErr != nil {
			return writeErr
		}
		if n != pending {
			return io.ErrShortWrite
		}
		pending = 0
		return nil
	}
	for {
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			if err := flush(); err != nil {
				return written, err
			}
			return written, ctxErr
		}
		if r.remaining <= 0 {
			return written, flush()
		}
		if r.index == len(r.entries) {
			if err := flush(); err != nil {
				return written, err
			}
			return written, io.ErrUnexpectedEOF
		}
		data := r.entries[r.index].Data[r.offset:]
		if len(data) == 0 {
			r.index++
			r.offset = 0
			continue
		}
		if int64(len(data)) > r.remaining {
			data = data[:int(r.remaining)]
		}
		if pending == 0 && len(data) >= len(scratch) {
			n, writeErr := dst.Write(data)
			if n < 0 || n > len(data) {
				return written, io.ErrShortWrite
			}
			written += int64(n)
			r.offset += n
			r.remaining -= int64(n)
			if writeErr != nil {
				return written, writeErr
			}
			if n != len(data) {
				return written, io.ErrShortWrite
			}
			continue
		}
		n := copy(scratch[pending:], data)
		pending += n
		r.offset += n
		r.remaining -= int64(n)
		if pending == len(scratch) {
			if err := flush(); err != nil {
				return written, err
			}
		}
	}
}
