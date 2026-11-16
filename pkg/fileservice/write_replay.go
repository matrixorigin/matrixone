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
