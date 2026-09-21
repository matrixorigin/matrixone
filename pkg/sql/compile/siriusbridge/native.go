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

//go:build sirius && cgo && linux && amd64

package siriusbridge

/*
#include <stdlib.h>
#include <sirius_c.h>
*/
import "C"

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Available() bool { return true }

type nativeError struct {
	code    uint32
	message string
}

func (e *nativeError) Error() string { return e.message }
func status(code C.sirius_status, e *C.sirius_error) error {
	if code == C.SIRIUS_OK {
		return nil
	}
	if code == C.SIRIUS_EOF {
		return errEOF
	}
	if code == C.SIRIUS_NOT_NEEDED {
		return errNotNeeded
	}
	return &nativeError{uint32(code), C.GoStringN(&e.message[0], C.int(boundedMessage(e)))}
}
func boundedMessage(e *C.sirius_error) int {
	for i := range e.message {
		if e.message[i] == 0 {
			return i
		}
	}
	return len(e.message)
}
func millis(ctx context.Context) C.uint32_t {
	if deadline, ok := ctx.Deadline(); ok {
		ms := time.Until(deadline).Milliseconds()
		if ms <= 0 {
			return 1
		}
		if ms < 1000 {
			return C.uint32_t(ms)
		}
	}
	return 1000
}

type engine struct{ handle *C.sirius_engine_handle }
type nativeQuery struct {
	handle   *C.sirius_query_handle
	inputs   map[uint64]*nativeInput
	columns  int
	mu       sync.Mutex
	retained []*C.sirius_batch_handle
}
type nativeInput struct {
	handle  *C.sirius_input_handle
	query   *nativeQuery
	columns int
}

func New(config Config) (*Runtime, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if C.sirius_abi_version() != 1 || C.sirius_capabilities()&11 != 11 {
		return nil, moerr.NewBadConfigNoCtx("Sirius SDK lacks ABI v1 MO input/native result capability")
	}
	path := C.CString(config.ConfigPath)
	defer C.free(unsafe.Pointer(path))
	options := C.sirius_engine_options{struct_size: C.sizeof_sirius_engine_options, abi_version: 1,
		config_path: path, config_path_bytes: C.uint32_t(len(config.ConfigPath)), gpu_streams: C.uint32_t(config.GPUStreams), max_waiting_queries: C.uint32_t(config.MaxWaiting)}
	d := &engine{}
	var e C.sirius_error
	if err := status(C.sirius_engine_create(&options, &d.handle, &e), &e); err != nil {
		return nil, err
	}
	runtime := newRuntime(d)
	runtime.cleanupTimeout = config.cleanupBudget()
	if config.MaxWaiting != 0 {
		runtime.maxQueries = 1 + int(config.MaxWaiting)
	}
	return runtime, nil
}

func (d *engine) stop() error {
	var e C.sirius_error
	return status(C.sirius_engine_stop(d.handle, &e), &e)
}
func (d *engine) close(ctx context.Context) error {
	var e C.sirius_error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		code := C.sirius_engine_close(&d.handle, millis(ctx), &e)
		if code != C.SIRIUS_TIMEOUT {
			return status(code, &e)
		}
	}
}

// arena contains only C allocations; descriptor pointer graphs never contain Go
// pointers, and the ABI copies all descriptors before this arena is released.
type arena []unsafe.Pointer

func (a *arena) bytes(b []byte) unsafe.Pointer { p := C.CBytes(b); *a = append(*a, p); return p }
func (a *arena) string(s string) *C.char       { return (*C.char)(a.bytes([]byte(s))) }
func (a *arena) alloc(n uintptr) unsafe.Pointer {
	p := C.calloc(1, C.size_t(n))
	if p == nil {
		panic("Sirius descriptor allocation failed")
	}
	*a = append(*a, p)
	return p
}
func (a arena) free() {
	for _, p := range a {
		C.free(p)
	}
}
func (a *arena) column(c Column) C.sirius_column {
	nullable := C.uint32_t(0)
	if c.Nullable {
		nullable = 1
	}
	return C.sirius_column{oid: C.uint32_t(c.OID), width: C.int32_t(c.Width), scale: C.int32_t(c.Scale), nullable: nullable, name: a.string(c.Name), name_bytes: C.uint32_t(len(c.Name))}
}

func (d *engine) prepare(ctx context.Context, req Request) (result queryDriver, resultErr error) {
	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadlineCause(ctx, req.Deadline, moerr.NewInternalErrorNoCtx("Sirius preparation deadline exceeded"))
		defer cancel()
	}
	var q *nativeQuery
	defer func() {
		if recovered := recover(); recovered != nil {
			if q != nil && q.handle != nil {
				result = q
			}
			resultErr = moerr.NewInternalErrorNoCtxf("Sirius descriptor preparation failed: %v", recovered)
		}
	}()
	var memory arena
	defer func() { memory.free() }()
	var e C.sirius_error
	options := C.sirius_query_options{struct_size: C.sizeof_sirius_query_options, abi_version: 1}
	if !req.Deadline.IsZero() {
		ms := time.Until(req.Deadline).Milliseconds()
		if ms <= 0 {
			return nil, context.DeadlineExceeded
		}
		if ms > int64(^uint32(0)) {
			ms = int64(^uint32(0))
		}
		options.timeout_ms = C.uint32_t(ms)
	}
	q = &nativeQuery{inputs: make(map[uint64]*nativeInput), columns: len(req.Columns)}
	if err := status(C.sirius_query_create(d.handle, &options, memory.bytes(req.Plan), C.uint64_t(len(req.Plan)), &q.handle, &e), &e); err != nil {
		return nil, err
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = q.cancel(); close(callbackDone) })
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()
	columns := (*C.sirius_column)(memory.alloc(uintptr(len(req.Columns)) * C.sizeof_sirius_column))
	for i, c := range req.Columns {
		unsafe.Slice(columns, len(req.Columns))[i] = memory.column(c)
	}
	contract := C.sirius_query_contract{struct_size: C.sizeof_sirius_query_contract, abi_version: 1, account_id: C.uint32_t(req.AccountID), query_id: memory.string(string(req.QueryID)), query_id_bytes: C.uint32_t(len(req.QueryID)), output_columns: columns, output_column_count: C.uint32_t(len(req.Columns))}
	for i, b := range req.Snapshot {
		contract.snapshot_ts[i] = C.uint8_t(b)
	}
	if err := status(C.sirius_query_bind(q.handle, &contract, &e), &e); err != nil {
		return q, err
	}
	for _, read := range req.Reads {
		if len(read.Columns) == 0 {
			return q, moerr.NewInvalidInputNoCtx("empty Sirius read schema")
		}
		cols := (*C.sirius_read_column)(memory.alloc(uintptr(len(read.Columns)) * C.sizeof_sirius_read_column))
		inputCols := (*C.sirius_input_column)(memory.alloc(uintptr(len(read.Columns)) * C.sizeof_sirius_input_column))
		for i, c := range read.Columns {
			logical := memory.column(c.Column)
			unsafe.Slice(cols, len(read.Columns))[i] = C.sirius_read_column{logical: logical, physical_column_id: C.uint64_t(c.PhysicalID), sequence_number: C.uint32_t(c.Sequence)}
			unsafe.Slice(inputCols, len(read.Columns))[i] = C.sirius_input_column{oid: logical.oid, width: logical.width, scale: logical.scale, nullable: logical.nullable}
		}
		binding := C.sirius_read_binding{struct_size: C.sizeof_sirius_read_binding, abi_version: 1, binding_id: C.uint64_t(read.BindingID), source_kind: C.SIRIUS_READ_MO, database_name: memory.string(read.Database), database_name_bytes: C.uint32_t(len(read.Database)), table_name: memory.string(read.Table), table_name_bytes: C.uint32_t(len(read.Table)), schema_name: memory.string(read.Schema), schema_name_bytes: C.uint32_t(len(read.Schema)), columns: cols, column_count: C.uint32_t(len(read.Columns))}
		if len(read.TAEManifest) > 0 {
			binding.source_kind = C.SIRIUS_READ_TAE
			binding.tae_manifest = memory.bytes(read.TAEManifest)
			binding.tae_manifest_bytes = C.uint64_t(len(read.TAEManifest))
			binding.data_root = memory.string(read.DataRoot)
			binding.data_root_bytes = C.uint32_t(len(read.DataRoot))
		}
		if err := status(C.sirius_read_register(q.handle, &binding, &e), &e); err != nil {
			return q, err
		}
		if read.Producer != nil {
			input := &nativeInput{query: q, columns: len(read.Columns)}
			if err := status(C.sirius_input_register(q.handle, C.uint64_t(read.BindingID), inputCols, C.uint32_t(len(read.Columns)), &input.handle, &e), &e); err != nil {
				return q, err
			}
			q.inputs[read.BindingID] = input
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return q, err
		}
		code := C.sirius_query_prepare(q.handle, millis(ctx), &e)
		if code == C.SIRIUS_TIMEOUT {
			continue
		}
		if err := status(code, &e); err != nil {
			return q, err
		}
		break
	}
	var schema C.sirius_result_schema
	schema.struct_size = C.sizeof_sirius_result_schema
	schema.abi_version = 1
	if err := status(C.sirius_query_get_schema(q.handle, &schema, &e), &e); err != nil {
		return q, err
	}
	if int(schema.column_count) != len(req.Columns) {
		return q, moerr.NewInvalidInputNoCtx("Sirius output column count differs from admitted schema")
	}
	for i, c := range unsafe.Slice(schema.columns, int(schema.column_count)) {
		want := req.Columns[i]
		if uint32(c.oid) != want.OID || int32(c.width) != want.Width || int32(c.scale) != want.Scale || (c.nullable != 0) != want.Nullable {
			return q, moerr.NewInvalidInputNoCtx("Sirius output schema differs from admitted schema")
		}
	}
	return q, nil
}

func (q *nativeQuery) start() error {
	var e C.sirius_error
	return status(C.sirius_query_start(q.handle, &e), &e)
}
func (q *nativeQuery) cancel() error {
	var e C.sirius_error
	return status(C.sirius_query_cancel(q.handle, &e), &e)
}
func (q *nativeQuery) input(id uint64) inputDriver { return q.inputs[id] }
func (q *nativeQuery) close(ctx context.Context) error {
	var e C.sirius_error
	for len(q.retained) > 0 {
		handle := q.retained[len(q.retained)-1]
		if err := release(&handle); err != nil {
			return err
		}
		q.retained = q.retained[:len(q.retained)-1]
	}
	// Go producers/calls have joined. Input handles can close while native
	// work drains: the native query registry retains each input independently.
	// query_close itself checks quiescence and preserves the handle on failure;
	// query_wait's TIMEOUT cannot distinguish call timeout from terminal outcome.
	for id, input := range q.inputs {
		if err := status(C.sirius_input_close(&input.handle, &e), &e); err != nil {
			return err
		}
		delete(q.inputs, id)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		code := C.sirius_query_close(&q.handle, millis(ctx), &e)
		if code != C.SIRIUS_TIMEOUT {
			return status(code, &e)
		}
	}
}

func release(handle **C.sirius_batch_handle) error {
	var e C.sirius_error
	return status(C.sirius_batch_release(handle, &e), &e)
}

func (q *nativeQuery) release(handle **C.sirius_batch_handle) error {
	err := release(handle)
	if err != nil && *handle != nil {
		q.mu.Lock()
		q.retained = append(q.retained, *handle)
		q.mu.Unlock()
	}
	return err
}

func (q *nativeQuery) next(fill func(Result) error) (err error) {
	var handle *C.sirius_batch_handle
	var e C.sirius_error
	code := C.sirius_query_next_result(q.handle, 1000, &handle, &e)
	if code == C.SIRIUS_TIMEOUT {
		return nil
	}
	if err = status(code, &e); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.release(&handle)) }()
	info := C.sirius_result_batch_info{struct_size: C.sizeof_sirius_result_batch_info, abi_version: 1}
	if err = status(C.sirius_result_describe(handle, &info, &e), &e); err != nil {
		return err
	}
	if info.payload_bytes > WindowBytes || int(info.column_count) != q.columns {
		return moerr.NewInvalidInputNoCtx("invalid Sirius result descriptor")
	}
	result := Result{Rows: uint32(info.rows), Vectors: make([]Vector, q.columns)}
	// The only extra result copy is bounded by the native 64 MiB window. The
	// lease stays live while the normal MO output callback consumes these bytes.
	payload := make([]byte, int(info.payload_bytes))
	if len(payload) > 0 {
		if err = status(C.sirius_result_read(handle, 0, unsafe.Pointer(&payload[0]), info.payload_bytes, &e), &e); err != nil {
			return err
		}
	}
	slice := func(offset, size C.uint64_t) ([]byte, error) {
		if offset > info.payload_bytes || size > info.payload_bytes-offset {
			return nil, moerr.NewInvalidInputNoCtx("invalid Sirius result buffer range")
		}
		return payload[int(offset):int(offset+size)], nil
	}
	for i, c := range unsafe.Slice(info.columns, q.columns) {
		v := &result.Vectors[i]
		v.Class = uint32(c.vector_class)
		if v.Data, err = slice(c.data_offset, c.data_bytes); err != nil {
			return err
		}
		if v.Area, err = slice(c.area_offset, c.area_bytes); err != nil {
			return err
		}
		if v.Nulls, err = slice(c.null_offset, c.null_bytes); err != nil {
			return err
		}
	}
	return fill(result)
}

func (i *nativeInput) push(ctx context.Context, rows uint32, vectors []Vector) (err error) {
	if len(vectors) == 0 || len(vectors) != i.columns {
		return moerr.NewInvalidInputNoCtx("empty Sirius input schema")
	}
	var total uint64
	for _, v := range vectors {
		for _, b := range [][]byte{v.Data, v.Area, v.Nulls} {
			if uint64(len(b)) > WindowBytes-total {
				return moerr.NewInvalidInputNoCtx("Sirius input exceeds native window; split at row boundaries")
			}
			total += uint64(len(b))
		}
	}
	var handle *C.sirius_batch_handle
	var e C.sirius_error
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		// Empty and constant-NULL vectors have no payload, but still need a
		// charged native owner for their descriptors until consumption.
		code := C.sirius_input_acquire(i.handle, C.uint64_t(max(total, 1)), millis(ctx), &handle, &e)
		if code == C.SIRIUS_TIMEOUT {
			continue
		}
		if err = status(code, &e); err != nil {
			return err
		}
		break
	}
	defer func() {
		if handle != nil {
			err = errors.Join(err, i.query.release(&handle))
		}
	}()
	columns := make([]C.sirius_input_vector, len(vectors))
	var offset uint64
	for n, v := range vectors {
		c := &columns[n]
		c.vector_class = C.uint32_t(v.Class)
		for j, b := range [][]byte{v.Data, v.Area, v.Nulls} {
			switch j {
			case 0:
				c.data_offset = C.uint64_t(offset)
				c.data_bytes = C.uint64_t(len(b))
			case 1:
				c.area_offset = C.uint64_t(offset)
				c.area_bytes = C.uint64_t(len(b))
			case 2:
				c.null_offset = C.uint64_t(offset)
				c.null_bytes = C.uint64_t(len(b))
			}
			if len(b) > 0 {
				if err = status(C.sirius_input_write(handle, C.uint64_t(offset), unsafe.Pointer(&b[0]), C.uint64_t(len(b)), &e), &e); err != nil {
					return err
				}
			}
			offset += uint64(len(b))
		}
	}
	return status(C.sirius_input_publish(i.handle, &handle, C.uint32_t(rows), &columns[0], C.uint32_t(len(columns)), &e), &e)
}
func (i *nativeInput) finish() error {
	var e C.sirius_error
	return status(C.sirius_input_finish(i.handle, &e), &e)
}
func (i *nativeInput) fail(err error) error {
	message := err.Error()
	if len(message) >= 512 {
		message = message[:511]
	}
	data := C.CString(message)
	defer C.free(unsafe.Pointer(data))
	var e C.sirius_error
	return status(C.sirius_input_fail(i.handle, data, C.uint32_t(len(message)), &e), &e)
}
