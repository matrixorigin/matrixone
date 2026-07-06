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

package iscp

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// cuvsWriterBufferCapacity bounds the byte buffer the writer
// accumulates before Full() returns true and the framing drains via
// ToSql(). Drains happen frequently in steady state; the cap mainly
// limits worst-case memory before backpressure.
const cuvsWriterBufferCapacity = 8 * 1024 * 1024 // 8 MiB

// CuvsCdcWriter is the shared CDC writer for cuvs-backed vector
// indexes (CAGRA, IVF-PQ). One instance per (table, index) pair,
// constructed once at NewIndexConsumer time.
//
// Buffers CDC row events as a sequence of EncodeEventRecord byte
// chunks — the same binary format CagraSync.Save / IvfpqSync.Save
// eventually flushes to the storage table. No JSON, no
// VectorIndexCdc[T] round-trip.
//
// Despite the "Writer" name (and the IndexSqlWriter interface it
// satisfies), the output is not SQL — it's the binary event-record
// stream consumed by RunCuvs → sync.AppendRecords.
//
// INSERT and UPSERT are encoded with distinct op codes (CdcOpInsert /
// CdcOpUpsert) sharing the same payload layout. Replay handles them
// identically (idempotent overflow-map write — safe under MO's
// duplicate / replay-from-corruption UPSERT semantics), but the chunk
// frame's n_inserts counter tallies only CdcOpInsert so the idxcron
// gate sees a strict lower bound on growth. This mirrors HNSW's
// pattern (HnswSqlWriter preserves UPSERT distinctly all the way
// through). The synchronous in-process callers (CagraSync.Update)
// still emit DELETE+INSERT pairs as their own thing — that's a
// different code path from this CDC writer.
type CuvsCdcWriter struct {
	algoName        string // diagnostic-only ("cagra" / "ivfpq")
	tabledef        *plan.TableDef
	indexdef        []*plan.IndexDef
	pkPos           int32
	partsPos        []int32
	dimension       int32
	dbName          string
	tblName         string
	indexName       string
	includeBindings []cuvscdc.IncludeBinding
	colMetaJSON     string
	includeBytesPer int
	pendingRecords  []byte
}

// NewCuvsCdcWriter constructs a CuvsCdcWriter from the per-(table,
// index) def. algoName is for diagnostics only (the writer does not
// switch on it); dbName/tblName/indexName are typically pulled from
// the ISCP ConsumerInfo at the call site.
//
// Both CAGRA and IVF-PQ on cuvs are fp32-only with a bigint PK; this
// constructor enforces both shapes.
func NewCuvsCdcWriter(algoName, dbName, tblName, indexName string,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (*CuvsCdcWriter, error) {

	if len(tabledef.Pkey.Names) != 1 {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s cuvs writer: index table only supports one primary key", algoName))
	}
	if len(indexdefs) != 2 {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s cuvs writer: index table must have 2 secondary tables", algoName))
	}

	idxdef := indexdefs[0]
	if len(idxdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s cuvs writer: index must have exactly one vector part", algoName))
	}

	w := &CuvsCdcWriter{
		algoName:       algoName,
		tabledef:       tabledef,
		indexdef:       indexdefs,
		dbName:         dbName,
		tblName:        tblName,
		indexName:      indexName,
		pendingRecords: make([]byte, 0, 64*1024),
	}

	w.pkPos = tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	pkTyp := tabledef.Cols[w.pkPos].Typ
	if types.T(pkTyp.Id) != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s cuvs writer: primary key must be bigint", algoName))
	}

	nparts := len(idxdef.Parts)
	w.partsPos = make([]int32, nparts)
	for i, part := range idxdef.Parts {
		w.partsPos[i] = tabledef.Name2ColIndex[part]
	}
	vecTyp := tabledef.Cols[w.partsPos[0]].Typ
	if vecTyp.Id != int32(types.T_array_float32) {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s cuvs writer: vector column must be vecf32 (cuvs is fp32-only)", algoName))
	}
	w.dimension = vecTyp.Width

	// Resolve INCLUDE columns from indexAlgoParams. Returns zero
	// values when no INCLUDE columns are configured.
	bindings, colMetaJSON, ibpr, err := cuvscdc.ResolveIncludeColumns(
		includedColumnsFromAlgoParams(idxdef.IndexAlgoParams),
		tabledef.Name2ColIndex,
		func(pos int32) int32 { return tabledef.Cols[pos].Typ.Id },
	)
	if err != nil {
		return nil, err
	}
	w.includeBindings = bindings
	w.colMetaJSON = colMetaJSON
	w.includeBytesPer = ibpr

	return w, nil
}

// includedColumnsFromAlgoParams extracts the comma-separated INCLUDE
// column names from indexAlgoParams (key catalog.IncludedColumns).
// Returns "" if absent or unparseable.
func includedColumnsFromAlgoParams(indexAlgoParams string) string {
	if indexAlgoParams == "" {
		return ""
	}
	val, err := sonic.Get([]byte(indexAlgoParams), catalog.IncludedColumns)
	if err != nil {
		return ""
	}
	joined, err := val.StrictString()
	if err != nil {
		return ""
	}
	return joined
}

// Accessors used by per-algo Hooks.Run impls to build their algo's
// sync object (CagraSync / IvfpqSync) — passed the same dbName etc.
// the writer was constructed with so writer and sync agree on layout
// (especially the colMetaJSON for INCLUDE columns).
func (w *CuvsCdcWriter) DbName() string             { return w.dbName }
func (w *CuvsCdcWriter) TblName() string            { return w.tblName }
func (w *CuvsCdcWriter) IndexName() string          { return w.indexName }
func (w *CuvsCdcWriter) IndexDef() []*plan.IndexDef { return w.indexdef }
func (w *CuvsCdcWriter) Dimension() int32           { return w.dimension }
func (w *CuvsCdcWriter) ColMetaJSON() string        { return w.colMetaJSON }

// IndexSqlWriter implementation
//
// (HnswSqlWriter, IvfflatSqlWriter, FulltextSqlWriter in
// index_sqlwriter.go follow the same shape; CuvsCdcWriter is just the
// shared cuvs flavour.)

func (w *CuvsCdcWriter) Reset()                    { w.pendingRecords = w.pendingRecords[:0] }
func (w *CuvsCdcWriter) Full() bool                { return len(w.pendingRecords) >= cuvsWriterBufferCapacity }
func (w *CuvsCdcWriter) Empty() bool               { return len(w.pendingRecords) == 0 }
func (w *CuvsCdcWriter) CheckLastOp(_ string) bool { return true }

func (w *CuvsCdcWriter) Insert(ctx context.Context, row []any) error {
	return w.encodeInsertOrUpsert(ctx, row, cuvscdc.CdcOpInsert)
}

// Upsert encodes with CdcOpUpsert so the idxcron count gate can ignore
// it — only true INSERTs are guaranteed-new-row events (see package
// comment).
func (w *CuvsCdcWriter) Upsert(ctx context.Context, row []any) error {
	return w.encodeInsertOrUpsert(ctx, row, cuvscdc.CdcOpUpsert)
}

// Delete encodes a DELETE event. Only the primary key is consulted —
// the delete-row payload from ISCP carries just row[0]=pk.
func (w *CuvsCdcWriter) Delete(ctx context.Context, row []any) error {
	key, ok := row[0].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf(
			"%s cuvs writer: invalid delete key type, expected int64", w.algoName))
	}
	return w.appendDelete(key)
}

// ToSql returns a copy of the accumulated event-record bytes. The
// naming is historical (IndexSqlWriter interface). Caller takes
// ownership of the returned slice; framing immediately calls Reset()
// so the writer's internal buffer is reused for the next batch.
func (w *CuvsCdcWriter) ToSql() ([]byte, error) {
	out := make([]byte, len(w.pendingRecords))
	copy(out, w.pendingRecords)
	return out, nil
}

func (w *CuvsCdcWriter) appendDelete(key int64) error {
	out, err := cuvscdc.EncodeEventRecord(w.pendingRecords, cuvscdc.CdcOpDelete,
		key, nil, nil, int(w.dimension), w.includeBytesPer)
	if err != nil {
		return err
	}
	w.pendingRecords = out
	return nil
}

func (w *CuvsCdcWriter) encodeInsertOrUpsert(ctx context.Context, row []any, op cuvscdc.CdcOp) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf(
			"%s cuvs writer: invalid key type, expected int64", w.algoName))
	}
	rawVec := row[w.partsPos[0]]
	if rawVec == nil {
		// NULL vector — encode as DELETE (the source row no longer
		// has a vector to index).
		return w.appendDelete(key)
	}
	v, ok := rawVec.([]float32)
	if !ok {
		// A non-nil value of the wrong type is a real schema/type error, not a
		// NULL vector — surface it instead of silently dropping the row to a
		// DELETE (mirrors the HNSW sinker in index_sqlwriter.go).
		return moerr.NewInternalError(ctx, fmt.Sprintf(
			"%s cuvs writer: invalid vector type, expected []float32, got %T", w.algoName, rawVec))
	}
	if v == nil {
		// Typed-nil slice — an actually absent vector; encode as DELETE.
		return w.appendDelete(key)
	}

	includeBytes, err := cuvscdc.EncodeIncludeRow(w.includeBindings, row, w.includeBytesPer)
	if err != nil {
		return err
	}
	out, err := cuvscdc.EncodeEventRecord(w.pendingRecords, op,
		key, v, includeBytes, int(w.dimension), w.includeBytesPer)
	if err != nil {
		return err
	}
	w.pendingRecords = out
	return nil
}

// Compile-time interface check.
var _ IndexSqlWriter = (*CuvsCdcWriter)(nil)

// ---------------------------------------------------------------------------
// Cuvs ISCP consumer-side runner
//
// The writer above (producer) and the sync interface + runner below
// (consumer) are the two ends of the cuvs CDC pipe — kept together in
// one file so the wire format is the single source of truth.
// ---------------------------------------------------------------------------

// CuvsSync is the algorithm-side interface that RunCuvs drives. cuvs
// vector indexes (CAGRA, IVF-PQ) implement this via their per-package
// CagraSync / IvfpqSync types (under //go:build gpu — pkg/iscp is
// CPU-safe).
//
// Semantics:
//   - AppendRecords appends a writer-produced byte chunk (one or more
//     EncodeEventRecord records concatenated, no JSON wrapping) to the
//     sync's pending buffer.
//   - Save flushes the pending records to the storage table as tag=1
//     event chunks. Called once at channel close.
//   - Destroy releases any cuvs-side resources. Called via defer.
type CuvsSync interface {
	AppendRecords(sqlproc *sqlexec.SqlProcess, recordBytes []byte) error
	Save(sqlproc *sqlexec.SqlProcess) error
	Destroy()
}

// CuvsSyncFactory constructs a CuvsSync inside the txn provided by
// the runner. Plugin Hooks.Run impls pass a closure that calls
// cagra.NewCagraSync / ivfpq.NewIvfpqSync; the factory is what keeps
// the GPU-tagged types out of pkg/iscp.
type CuvsSyncFactory func(sqlproc *sqlexec.SqlProcess) (CuvsSync, error)

// RunCuvs drives one ISCP consumer iteration for cuvs-backed vector
// indexes (CAGRA, IVF-PQ). It reads raw EncodeEventRecord byte chunks
// from the consumer's send channel — no JSON marshal/unmarshal — and
// appends them straight into the sync's pending buffer via
// AppendRecords. On channel close it flushes via Save and updates the
// tail watermark.
//
// Used as the Run implementation by CAGRA and IVF-PQ plugin Hooks.
// HNSW stays on RunHnsw[T] / VectorIndexCdc[T] — different sync
// architecture (in-memory graph mutation, no event log).
func RunCuvs(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever, factory CuvsSyncFactory) {
	datatype := r.GetDataType()

	var sync CuvsSync
	err := c.RunTxn(ctx, r, 30*time.Minute, func(sqlproc *sqlexec.SqlProcess) error {
		s, e := factory(sqlproc)
		if e != nil {
			return e
		}
		sync = s
		return nil
	})
	if err != nil {
		// The factory may have constructed `sync` (GPU resources) inside the
		// callback before RunTxn's commit failed — release it here so a commit
		// failure doesn't leak the GPU index. (defer below isn't reached yet.)
		if sync != nil {
			sync.Destroy()
		}
		errch <- err
		return
	}
	if sync == nil {
		errch <- moerr.NewInternalErrorNoCtx("RunCuvs: factory returned nil sync")
		return
	}
	defer sync.Destroy()

	for {
		select {
		case <-ctx.Done():
			return
		case e2 := <-errch:
			errch <- e2
			return
		case recordBytes, open := <-c.sqlBufSendCh:
			if !open {
				err := c.RunTxn(ctx, r, time.Hour, func(sqlproc *sqlexec.SqlProcess) error {
					if e := sync.Save(sqlproc); e != nil {
						return e
					}
					if datatype == ISCPDataType_Tail {
						sqlctx := sqlproc.SqlCtx
						return r.UpdateWatermark(sqlproc.GetContext(), sqlctx.GetService(), sqlctx.Txn())
					}
					return nil
				})
				if err != nil {
					errch <- err
				}
				return
			}

			err := c.RunTxn(ctx, r, 30*time.Minute, func(sqlproc *sqlexec.SqlProcess) error {
				return sync.AppendRecords(sqlproc, recordBytes)
			})
			if err != nil {
				errch <- err
				return
			}
		}
	}
}
