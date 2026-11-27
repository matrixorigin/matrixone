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

package mpool

import (
	"encoding/binary"
	"math"
)

// MessageBuffer is one owner's buffer for marshaling messages, one after
// another, from mp: it is allocated once and grows only when a message
// needs more. Between messages it keeps at most maxRetained bytes.
//
// Its memory is off heap. Nothing may reference the bytes after the owner
// reuses or frees them: hand a transport a copy, or wait until the transport
// has released every message that refers to them.
//
// It is an io.Writer with the typed writes and EnsureCapacity of the batch
// and vector codecs, which then encode into it without allocating.
type MessageBuffer struct {
	mp          *MPool
	data        []byte
	maxRetained int
}

// NewMessageBuffer returns an empty buffer that allocates from mp.
func NewMessageBuffer(mp *MPool, maxRetained int) MessageBuffer {
	return MessageBuffer{mp: mp, maxRetained: maxRetained}
}

// Pool returns the pool the buffer allocates from; nil for a zero buffer.
func (b *MessageBuffer) Pool() *MPool { return b.mp }

// Bytes returns the message written since the last Reset.
func (b *MessageBuffer) Bytes() []byte { return b.data }

func (b *MessageBuffer) Len() int { return len(b.data) }

// Reset starts a new message. The previous message's bytes are reused, so
// call it only once they are no longer in use. A buffer grown past
// maxRetained is freed.
func (b *MessageBuffer) Reset() {
	if cap(b.data) > b.maxRetained {
		b.Free()
		return
	}
	b.data = b.data[:0]
}

// Resize starts a new message of size bytes, with undefined contents, and
// returns it; see Reset.
func (b *MessageBuffer) Resize(size int) ([]byte, error) {
	b.data = b.data[:0]
	if err := b.EnsureCapacity(size); err != nil {
		return nil, err
	}
	b.data = b.data[:size]
	return b.data, nil
}

// Free releases the buffer's memory. The buffer can still be used: it
// allocates again on the next write.
func (b *MessageBuffer) Free() {
	if cap(b.data) != 0 {
		b.mp.Free(b.data)
	}
	b.data = nil
}

// EnsureCapacity makes the buffer hold n bytes without growing.
func (b *MessageBuffer) EnsureCapacity(n int) error {
	if n <= cap(b.data) {
		return nil
	}
	capacity, ok := GrowCapacity(int64(cap(b.data)), int64(n))
	if !ok || capacity > math.MaxInt {
		return ErrAllocationAllocatorLimit
	}
	data, err := b.mp.Alloc(int(capacity), true)
	if err != nil {
		return err
	}
	data = data[:len(b.data)]
	copy(data, b.data)
	b.Free()
	b.data = data
	return nil
}

func (b *MessageBuffer) grow(n int) ([]byte, error) {
	if n > math.MaxInt-len(b.data) {
		return nil, ErrAllocationAllocatorLimit
	}
	start := len(b.data)
	if err := b.EnsureCapacity(start + n); err != nil {
		return nil, err
	}
	b.data = b.data[:start+n]
	return b.data[start:], nil
}

func (b *MessageBuffer) Write(p []byte) (int, error) {
	dst, err := b.grow(len(p))
	if err != nil {
		return 0, err
	}
	return copy(dst, p), nil
}

func (b *MessageBuffer) WriteByte(v byte) error {
	dst, err := b.grow(1)
	if err != nil {
		return err
	}
	dst[0] = v
	return nil
}

func (b *MessageBuffer) WriteUint32(v uint32) error {
	dst, err := b.grow(4)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint32(dst, v)
	return nil
}

func (b *MessageBuffer) WriteInt32(v int32) error { return b.WriteUint32(uint32(v)) }

func (b *MessageBuffer) WriteUint64(v uint64) error {
	dst, err := b.grow(8)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint64(dst, v)
	return nil
}

func (b *MessageBuffer) WriteInt64(v int64) error { return b.WriteUint64(uint64(v)) }
