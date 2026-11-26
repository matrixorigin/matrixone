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
)

// EncodedBatch is Entry.Bat: one batch.MarshalBinary encoding held as the
// concatenation of its parts. An encoder can pass the vectors' own data and
// area buffers as parts, so marshaling the Entry copies them once, straight
// into the message buffer. The parts are borrowed: they must stay unchanged
// until the Entry is marshaled or dropped.
//
// After Unmarshal it holds one part, its own copy of the message bytes.
type EncodedBatch struct {
	// data is the encoding when it is one part; parts is nil then.
	data  []byte
	parts [][]byte
	size  int
}

// NewEncodedBatch returns the encoding made of parts, in order.
func NewEncodedBatch(parts ...[]byte) EncodedBatch {
	if len(parts) == 1 {
		return EncodedBatch{data: parts[0], size: len(parts[0])}
	}
	size := 0
	for _, part := range parts {
		size += len(part)
	}
	return EncodedBatch{parts: parts, size: size}
}

// Bytes returns the encoding as one slice. A single part is returned as is;
// several parts are concatenated into a new slice.
func (b EncodedBatch) Bytes() []byte {
	switch len(b.parts) {
	case 0:
		return b.data
	case 1:
		return b.parts[0]
	}
	data := make([]byte, 0, b.size)
	for _, part := range b.parts {
		data = append(data, part...)
	}
	return data
}

// Len returns the encoding's length in bytes.
func (b EncodedBatch) Len() int { return b.size }

func (b EncodedBatch) Size() int { return b.size }

func (b EncodedBatch) ProtoSize() int { return b.size }

func (b EncodedBatch) Marshal() ([]byte, error) {
	data := make([]byte, b.size)
	_, err := b.MarshalTo(data)
	return data, err
}

func (b EncodedBatch) MarshalTo(data []byte) (int, error) {
	if b.parts == nil {
		return copy(data, b.data), nil
	}
	n := 0
	for _, part := range b.parts {
		n += copy(data[n:], part)
	}
	return n, nil
}

// Unmarshal copies data: gogoproto passes a view of the message buffer,
// which the caller may reuse. Decoders then borrow from the copy.
func (b *EncodedBatch) Unmarshal(data []byte) error {
	*b = EncodedBatch{data: bytes.Clone(data), size: len(data)}
	return nil
}

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
