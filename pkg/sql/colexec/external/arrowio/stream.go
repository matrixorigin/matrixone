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

package arrowio

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// streamMessageReader is the trust boundary for IPC Stream messages. Arrow-Go
// validates wire metadata/body lengths, but its decompressor trusts each
// buffer's 8-byte decoded-size prefix. Inspecting the complete message here
// keeps that size from reaching an allocator before policy has accepted it.
type streamMessageReader struct {
	refs      atomic.Int64
	ctx       context.Context
	stream    io.Reader
	allocator memory.Allocator
	options   Options
	current   *ipc.Message
	header    [4]byte
}

func newStreamMessageReader(
	ctx context.Context,
	stream io.Reader,
	options Options,
) *streamMessageReader {
	reader := &streamMessageReader{
		ctx: ctx, stream: stream, allocator: options.Allocator, options: options,
	}
	reader.refs.Store(1)
	return reader
}

func (r *streamMessageReader) Retain() {
	if r == nil {
		panic("retain released Arrow stream message reader")
	}
	for {
		refs := r.refs.Load()
		if refs <= 0 {
			panic("retain released Arrow stream message reader")
		}
		if r.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (r *streamMessageReader) Release() {
	if r == nil {
		return
	}
	refs := r.refs.Add(-1)
	if refs < 0 {
		panic("Arrow stream message reader release underflow")
	}
	if refs == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

// Message returns a message that remains valid until Message is called again.
func (r *streamMessageReader) Message() (message *ipc.Message, retErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message = nil
			if allocationErr, matched := recoveredAllocationError(recovered); matched {
				retErr = allocationErr
				return
			}
			retErr = moerr.NewInvalidInputf(r.ctx, "invalid Arrow IPC Stream message: %v", recovered)
		}
	}()
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}

	metadataLength, err := r.readMetadataLength()
	if err != nil {
		return nil, err
	}
	metadata := memory.NewResizableBuffer(r.allocator)
	defer metadata.Release()
	metadata.Resize(metadataLength)
	if _, err := io.ReadFull(r.stream, metadata.Bytes()); err != nil {
		return nil, moerr.NewInvalidInputf(r.ctx, "could not read Arrow IPC Stream metadata: %v", err)
	}

	inspected, err := inspectIPCMessageMetadata(
		r.ctx, metadata.Bytes(), r.options.MaxBodyBytes, -1, nil, false,
		r.options.MaxDecodedRecordBytes,
	)
	if err != nil {
		return nil, err
	}
	body := memory.NewResizableBuffer(r.allocator)
	defer body.Release()
	body.Resize(int(inspected.bodyBytes))
	if _, err := io.ReadFull(r.stream, body.Bytes()); err != nil {
		return nil, moerr.NewInvalidInputf(r.ctx, "could not read Arrow IPC Stream body: %v", err)
	}
	if _, err := inspectIPCMessageMetadata(
		r.ctx, metadata.Bytes(), r.options.MaxBodyBytes, inspected.bodyBytes, body.Bytes(), true,
		r.options.MaxDecodedRecordBytes,
	); err != nil {
		return nil, err
	}

	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.current = ipc.NewMessage(metadata, body)
	return r.current, nil
}

func (r *streamMessageReader) readMetadataLength() (int, error) {
	if _, err := io.ReadFull(r.stream, r.header[:]); err != nil {
		return 0, err
	}
	prefix := binary.LittleEndian.Uint32(r.header[:])
	if prefix == 0 {
		return 0, io.EOF
	}
	if prefix == ipcContinuationToken {
		if _, err := io.ReadFull(r.stream, r.header[:]); err != nil {
			return 0, moerr.NewInvalidInputf(r.ctx, "could not read Arrow IPC Stream message length: %v", err)
		}
		prefix = binary.LittleEndian.Uint32(r.header[:])
		if prefix == 0 {
			return 0, io.EOF
		}
	}
	if prefix < 4 || uint64(prefix) > uint64(math.MaxInt) || int64(prefix) > r.options.MaxMetadataBytes {
		return 0, moerr.NewInvalidInputf(r.ctx,
			"Arrow IPC Stream metadata length %d exceeds limit %d", prefix, r.options.MaxMetadataBytes)
	}
	return int(prefix), nil
}

var _ ipc.MessageReader = (*streamMessageReader)(nil)
