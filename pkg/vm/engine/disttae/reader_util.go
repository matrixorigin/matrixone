// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type ReaderOption func(*reader)

func WithColumns(
	seqnums []uint16, colTypes []types.Type,
) ReaderOption {
	return func(r *reader) {
		r.columns.seqnums = seqnums
		r.columns.colTypes = colTypes
	}
}

func WithBlockFilter(
	filter objectio.BlockReadFilter, seqnum uint16, typ types.Type,
) ReaderOption {
	return func(r *reader) {
		r.filterState.filter = filter
		r.filterState.seqnums = []uint16{seqnum}
		r.filterState.colTypes = []types.Type{typ}
	}
}

func WithMemFilter(filter MemPKFilter) ReaderOption {
	return func(r *reader) {
		r.memFilter = filter
	}
}

func WithTombstone() ReaderOption {
	return func(r *reader) {
		r.isTombstone = true
	}
}

func NewSimpleReader(
	ctx context.Context,
	ds engine.DataSource,
	fs fileservice.FileService,
	ts timestamp.Timestamp,
	opts ...ReaderOption,
) *reader {
	r := &reader{
		withFilterMixin: withFilterMixin{
			ctx: ctx,
			fs:  fs,
			ts:  ts,
		},
		source: ds,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func SimpleObjectReader(
	ctx context.Context,
	fs fileservice.FileService,
	obj *objectio.ObjectStats,
	ts timestamp.Timestamp, // only used for appendable object
	opts ...ReaderOption,
) engine.Reader {
	relData := NewBlockListRelationDataOfObject(obj, false)
	ds := NewRemoteDataSource(
		ctx, fs, ts, relData,
	)
	return NewSimpleReader(
		ctx, ds, fs, ts, opts...,
	)
}

func SimpleTombstoneObjectReader(
	ctx context.Context,
	fs fileservice.FileService,
	obj *objectio.ObjectStats,
	ts timestamp.Timestamp, // only used for appendable object
	opts ...ReaderOption,
) engine.Reader {
	opts = append(opts, WithTombstone())
	return SimpleObjectReader(
		ctx, fs, obj, ts, opts...,
	)
}
