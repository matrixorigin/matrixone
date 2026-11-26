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

package api

import (
	"bytes"
	"encoding/json"
	"io"
)

// BatchEncoder writes a batch encoding whose size was fixed when the
// encoder was made.
type BatchEncoder interface {
	// EncodeTo writes the encoding into dst, whose length is the size. It
	// fails if the batch no longer encodes to that size.
	EncodeTo(dst []byte) error
}

// EncodedBatch is Entry.Bat: one batch.MarshalBinary encoding. It holds
// either the bytes or an encoder that borrows a batch and writes it straight
// into the message buffer when the Entry is marshaled. The borrowed batch
// must stay unchanged until then.
//
// Unmarshal keeps a view of the message buffer, which the receiver may
// reuse once it has handled the message. Whatever outlives that keeps a
// Detach copy.
type EncodedBatch struct {
	data []byte
	enc  BatchEncoder
	size int
	// view marks data as a view of a message buffer.
	view bool
}

// NewEncodedBatch returns the encoding data.
func NewEncodedBatch(data []byte) EncodedBatch {
	return EncodedBatch{data: data, size: len(data)}
}

// NewBorrowedEncodedBatch returns the size-byte encoding that enc writes.
func NewBorrowedEncodedBatch(enc BatchEncoder, size int) EncodedBatch {
	return EncodedBatch{enc: enc, size: size}
}

// Bytes returns the encoding. Bytes of an encoder's encoding writes it into
// a new slice, and returns nil if the encoder fails.
func (b EncodedBatch) Bytes() []byte {
	if b.enc == nil {
		return b.data
	}
	data, err := b.Marshal()
	if err != nil {
		return nil
	}
	return data
}

// Len returns the encoding's length in bytes.
func (b EncodedBatch) Len() int { return b.size }

func (b EncodedBatch) Size() int { return b.size }

func (b EncodedBatch) ProtoSize() int { return b.size }

func (b EncodedBatch) Marshal() ([]byte, error) {
	data := make([]byte, b.size)
	if _, err := b.MarshalTo(data); err != nil {
		return nil, err
	}
	return data, nil
}

// MarshalTo writes the encoding at the start of data, which gogoproto sizes
// from ProtoSize.
func (b EncodedBatch) MarshalTo(data []byte) (int, error) {
	if len(data) < b.size {
		return 0, io.ErrShortBuffer
	}
	if b.enc == nil {
		return copy(data, b.data), nil
	}
	if err := b.enc.EncodeTo(data[:b.size:b.size]); err != nil {
		return 0, err
	}
	return b.size, nil
}

// Unmarshal keeps data, a view of the message buffer, without copying it.
func (b *EncodedBatch) Unmarshal(data []byte) error {
	*b = EncodedBatch{data: data[:len(data):len(data)], size: len(data), view: true}
	return nil
}

// Detach returns the encoding as bytes of its own, for a user that outlives
// the message buffer or the borrowed batch: a view is copied and an
// encoder's encoding written out; bytes already owned are returned as is.
func (b EncodedBatch) Detach() EncodedBatch {
	switch {
	case b.enc != nil:
		return NewEncodedBatch(b.Bytes())
	case b.view:
		return NewEncodedBatch(bytes.Clone(b.data))
	}
	return b
}

// IsView reports whether the encoding is a view of a message buffer.
func (b EncodedBatch) IsView() bool { return b.view }

func (b EncodedBatch) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.Bytes())
}

func (b *EncodedBatch) UnmarshalJSON(data []byte) error {
	var raw []byte
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*b = NewEncodedBatch(raw)
	return nil
}

func (b EncodedBatch) Equal(other EncodedBatch) bool {
	return b.size == other.size && bytes.Equal(b.Bytes(), other.Bytes())
}

func (b EncodedBatch) Compare(other EncodedBatch) int {
	return bytes.Compare(b.Bytes(), other.Bytes())
}
