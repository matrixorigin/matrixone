// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetvariant

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

// Document shares one dictionary, cancellation context and expansion budget
// across every residual fragment and typed field of a shredded VARIANT row.
// It is local to a row and must not be shared between goroutines.
type Document struct {
	d      decoder
	fields map[string]struct{}
}

func NewDocument(ctx context.Context, metadata []byte) (*Document, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if len(metadata) > maxDecodedBytes {
		return nil, invalid("metadata exceeds decoding limit")
	}
	d := &Document{d: decoder{ctx: ctx, remaining: maxDecodedBytes}}
	if err := d.d.metadata(metadata); err != nil {
		return nil, err
	}
	return d, nil
}

// Reserve accounts for caller allocations before constructing containers.
func (d *Document) Reserve(n int) error {
	if err := context.Cause(d.d.ctx); err != nil {
		return err
	}
	return d.d.charge(n)
}

func (d *Document) DecodeValue(value []byte, depth int) (bytejson.ByteJson, error) {
	if len(value) > maxDecodedBytes || depth < 0 || depth > bytejson.JSONDocumentMaxNestingDepth {
		return bytejson.ByteJson{}, invalid("value exceeds decoding limit")
	}
	return d.d.value(value, depth)
}

func (d *Document) HasField(name string) (bool, error) {
	if d.fields == nil {
		if err := d.Reserve(len(d.d.keys) * 48); err != nil {
			return false, err
		}
		d.fields = make(map[string]struct{}, len(d.d.keys))
		for i, key := range d.d.keys {
			if i%256 == 0 {
				if err := context.Cause(d.d.ctx); err != nil {
					return false, err
				}
			}
			d.fields[key] = struct{}{}
		}
	}
	_, ok := d.fields[name]
	return ok, nil
}

// Children have already been charged by DecodeValue/Reserve. Charge the
// container table, field names and copied child payloads before assembly.
func (d *Document) Object(fields map[string]any, depth int) (bytejson.ByteJson, error) {
	if depth >= bytejson.JSONDocumentMaxNestingDepth {
		return bytejson.ByteJson{}, invalid("nesting exceeds 100 containers")
	}
	if err := d.Reserve(10 + len(fields)*64); err != nil {
		return bytejson.ByteJson{}, err
	}
	for key, child := range fields {
		bj, ok := child.(bytejson.ByteJson)
		if !ok {
			return bytejson.ByteJson{}, invalid("unaccounted typed object child")
		}
		if err := d.Reserve(max(len(bj.Data), 64)); err != nil {
			return bytejson.ByteJson{}, err
		}
		if !utf8.ValidString(key) || len(key) > math.MaxUint16 {
			return bytejson.ByteJson{}, invalid("invalid object key")
		}
		if err := d.Reserve(len(key)); err != nil {
			return bytejson.ByteJson{}, err
		}
	}
	return bytejson.CreateByteJSON(fields)
}

func (d *Document) Array(values []any, depth int) (bytejson.ByteJson, error) {
	if depth >= bytejson.JSONDocumentMaxNestingDepth {
		return bytejson.ByteJson{}, invalid("nesting exceeds 100 containers")
	}
	if err := d.Reserve(10); err != nil {
		return bytejson.ByteJson{}, err
	}
	for _, child := range values {
		bj, ok := child.(bytejson.ByteJson)
		if !ok {
			return bytejson.ByteJson{}, invalid("unaccounted typed array child")
		}
		if err := d.Reserve(max(len(bj.Data), 64)); err != nil {
			return bytejson.ByteJson{}, err
		}
	}
	return bytejson.CreateByteJSON(values)
}

// CheckElementCount prevents overflow when charging a caller's array storage.
func (d *Document) CheckElementCount(n int) error {
	if n < 0 || n > maxDecodedBytes/64 {
		return moerr.NewInvalidInputNoCtxf("parquet VARIANT array exceeds decoding limit")
	}
	return d.Reserve(n * 64)
}
