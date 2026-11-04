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

package keycodec

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// KeyChunkRows is the maximum row count of one collected runtime-key vector.
const KeyChunkRows = 8192

// KeyPayload is immutable, complete runtime-filter key data. The first chunk
// is inline so a small filter needs no chunk-directory allocation. Encodings
// and the directory are transferred to the payload, not copied. The caller
// must own a reference while accessing it; each successful Retain needs Release.
type KeyPayload struct {
	first []byte
	rest  [][]byte
	typ   types.Type
	rows  uint64
	lease bufferlease.BufferLease
}

// NewKeyPayload validates before accepting ownership. On error, release remains
// the caller's responsibility. On success it runs once after the last Release.
// The caller must admit the encoded buffers and directory before constructing
// them; accountedBytes is diagnostic and does not itself reserve capacity.
func NewKeyPayload(ctx context.Context, chunks [][]byte, accountedBytes int64, release func()) (*KeyPayload, error) {
	if accountedBytes < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative runtime-key payload accounting")
	}
	p := &KeyPayload{}
	for i, data := range chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var v vector.Vector
		if err := v.UnmarshalBinary(data); err != nil {
			v.Free(nil)
			return nil, err
		}
		if v.Length() <= 0 || v.Length() > KeyChunkRows || v.IsConst() ||
			(i != 0 && !p.typ.Eq(*v.GetType())) || p.rows > math.MaxInt32-uint64(v.Length()) {
			v.Free(nil)
			return nil, moerr.NewInvalidInputNoCtx("invalid runtime-key chunk shape or cardinality")
		}
		if i == 0 {
			p.typ = *v.GetType()
		}
		p.rows += uint64(v.Length())
		v.Free(nil)
	}
	if len(chunks) != 0 {
		p.first, p.rest = chunks[0], chunks[1:]
	}
	lease, err := bufferlease.NewRefCounted(nil, accountedBytes, release)
	if err != nil {
		return nil, err
	}
	p.lease = lease
	return p, nil
}

func (p *KeyPayload) Rows() uint64 {
	if p == nil {
		return 0
	}
	return p.rows
}

func (p *KeyPayload) ChunkCount() int {
	if p == nil || len(p.first) == 0 {
		return 0
	}
	return 1 + len(p.rest)
}

func (p *KeyPayload) Type() types.Type {
	if p == nil {
		return types.Type{}
	}
	return p.typ
}

func (p *KeyPayload) Retain() bool {
	return p != nil && p.lease != nil && p.lease.Retain()
}

func (p *KeyPayload) Release() {
	if p != nil && p.lease != nil {
		p.lease.Release()
	}
}

// Visit lends one decoded vector at a time. The callback must neither mutate
// it nor retain its backing beyond its own payload reference's lifetime.
func (p *KeyPayload) Visit(ctx context.Context, fn func(*vector.Vector) error) error {
	return p.VisitEncoded(ctx, func(data []byte) error {
		var v vector.Vector
		defer v.Free(nil)
		if err := v.UnmarshalBinaryTrusted(data); err != nil {
			return err
		}
		return fn(&v)
	})
}

// VisitEncoded lends validated immutable encodings under the caller's retained
// reference. It is useful for consumers keeping independently owned views.
func (p *KeyPayload) VisitEncoded(ctx context.Context, fn func([]byte) error) error {
	if p == nil {
		return nil
	}
	visit := func(data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(data)
	}
	if len(p.first) != 0 {
		if err := visit(p.first); err != nil {
			return err
		}
	}
	for _, data := range p.rest {
		if err := visit(data); err != nil {
			return err
		}
	}
	return nil
}
