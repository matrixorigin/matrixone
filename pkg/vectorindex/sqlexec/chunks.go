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

package sqlexec

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// maxChunkParamBytes bounds the pending transport values, not the enclosing
// transaction or its backend buffers. IDs count toward the same byte budget.
// Keep concurrent INSERT planning/transport peaks below the 32 MiB design ceiling.
const maxChunkParamBytes = 8 << 20

// ChunkInserter borrows a transaction and reads only ReaderAt objects supplied
// by the artifact owner. SQL receives binary values, never paths or extra file
// authority. Flush must complete before the owner reports persistence success.
type ChunkInserter struct {
	ctx     context.Context
	exec    executor.TxnExecutor
	stmt    executor.StatementOption
	prefix  string
	maxRows int
	tuples  []string
	params  []string
	bytes   int
	scratch []byte
	err     error
}

func NewChunkInserter(ctx context.Context, exec executor.TxnExecutor, stmt executor.StatementOption, db, table string, maxRows int) (*ChunkInserter, error) {
	if ctx == nil || exec == nil || maxRows <= 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid index chunk inserter")
	}
	return &ChunkInserter{
		ctx: ctx, exec: exec, stmt: stmt, maxRows: maxRows,
		prefix:  "INSERT INTO " + sqlquote.QualifiedIdent(db, table) + " (index_id, chunk_id, data, tag) VALUES ",
		scratch: make([]byte, vectorindex.MaxChunkSize),
	}, nil
}

// WriteRange preserves each range's chunk boundaries (including packed tail
// frames), while pending batches can span ranges. The reader remains borrowed.
func (w *ChunkInserter) WriteRange(src io.ReaderAt, offset, length int64, indexID string, firstChunkID int64, tag int) (int64, error) {
	if w.err != nil {
		return firstChunkID, w.err
	}
	if src == nil || offset < 0 || length < 0 || offset > math.MaxInt64-length || firstChunkID < 0 {
		return firstChunkID, w.fail(moerr.NewInvalidInputNoCtx("invalid index artifact range"))
	}
	chunks := length / vectorindex.MaxChunkSize
	if length%vectorindex.MaxChunkSize != 0 {
		chunks++
	}
	if chunks > math.MaxInt64-firstChunkID || len(indexID) > maxChunkParamBytes-vectorindex.MaxChunkSize {
		return firstChunkID, w.fail(moerr.NewInvalidInputNoCtx("index artifact range exceeds chunk limits"))
	}
	next := firstChunkID
	for length > 0 {
		if err := w.ctx.Err(); err != nil {
			return next, w.fail(err)
		}
		size := min(length, int64(vectorindex.MaxChunkSize))
		if len(w.tuples) > 0 && (len(w.tuples) >= w.maxRows || w.bytes+len(indexID)+int(size) > maxChunkParamBytes) {
			if err := w.Flush(); err != nil {
				return next, err
			}
		}
		buf := w.scratch[:size]
		n, err := src.ReadAt(buf, offset)
		if n != len(buf) {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return next, w.fail(err)
		}
		if err != nil && err != io.EOF {
			return next, w.fail(err)
		}
		// Copy before reusing scratch; execution/retry retains immutable values.
		w.params = append(w.params, indexID, string(buf))
		w.tuples = append(w.tuples, fmt.Sprintf("(?, %d, CAST(? AS LONGBLOB), %d)", next, tag))
		w.bytes += len(indexID) + len(buf)
		offset += size
		length -= size
		next++
	}
	return next, nil
}

func (w *ChunkInserter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		return w.fail(err)
	}
	if len(w.tuples) == 0 {
		return nil
	}
	res, err := w.exec.Exec(w.prefix+strings.Join(w.tuples, ", "), w.stmt.WithParams(w.params))
	res.Close()
	w.clear()
	if err != nil {
		return w.fail(err)
	}
	return nil
}

func (w *ChunkInserter) clear() {
	clear(w.params)
	clear(w.tuples)
	w.params = w.params[:0]
	w.tuples = w.tuples[:0]
	w.bytes = 0
}

func (w *ChunkInserter) fail(err error) error {
	w.err = err
	w.clear()
	return err
}

// StreamChunksToFile owns the SQL producer until every delivered result is
// closed. Callers retain ownership of the file and choose the chunk query.
func StreamChunksToFile(sqlproc *SqlProcess, sql string, dst io.WriteSeeker,
	run func(context.Context, *SqlProcess, string, chan executor.Result, chan error) (executor.Result, error)) error {
	stream := make(chan executor.Result, 2)
	errors := make(chan error, 2)
	producerDone := make(chan error, 1)
	ctx, cancel := context.WithCancelCause(sqlproc.GetTopContext())
	defer cancel(nil)

	go func() {
		var producerErr error
		defer func() {
			if p := recover(); p != nil {
				producerErr = moerr.ConvertPanicError(ctx, p)
			}
			close(stream)
			producerDone <- producerErr
		}()
		_, producerErr = run(ctx, sqlproc, sql, stream, errors)
	}()

	joined := false
	defer func() {
		if !joined {
			cancel(nil)
			for res := range stream {
				res.Close()
			}
			<-producerDone
		}
	}()

	var loopErr error
	closed := false
	for !closed && loopErr == nil {
		select {
		case res, ok := <-stream:
			if !ok {
				closed = true
				break
			}
			loopErr = writeChunkResult(dst, res)
		case loopErr = <-errors:
		case <-sqlproc.GetContext().Done():
			loopErr = moerr.NewInternalError(sqlproc.GetContext(), "context cancelled")
		case <-ctx.Done():
			loopErr = moerr.NewInternalErrorf(ctx, "context cancelled: %v", ctx.Err())
		}
	}
	if loopErr != nil {
		cancel(loopErr)
	}
	for res := range stream {
		res.Close()
	}
	producerErr := <-producerDone
	joined = true
	if loopErr != nil {
		return loopErr
	}
	select {
	case err := <-errors:
		return err
	default:
		return producerErr
	}
}

func writeChunkResult(dst io.WriteSeeker, res executor.Result) error {
	defer res.Close()
	bat := res.Batches[0]
	ids := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
	for i, id := range ids {
		data := bat.Vecs[1].GetRawBytesAt(i)
		if _, err := dst.Seek(id*vectorindex.MaxChunkSize, io.SeekStart); err != nil {
			return err
		}
		if _, err := dst.Write(data); err != nil {
			return err
		}
	}
	return nil
}
