//go:build gpu

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

// Package iscp provides CAGRA's ISCP hook layer: the CagraSqlWriter
// (a JSON CDC blob buffer) and runCagra (the consumer loop that drives
// cagra.CagraSync.Update / Save).
//
// CAGRA is GPU-only — the entire package is //go:build gpu. CREATE
// INDEX fails on CPU at the cagra_create cgo table function before
// any CDC task gets registered, so the iscp Hooks are never invoked
// in a CPU binary; gating the package keeps cagra.NewCagraSync (also
// GPU-only) out of CPU build graphs.
//
// Registered from pkg/indexplugin/iscp/import_gpu.go.
package iscp

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func init() {
	iscppkg.Register(catalog.MoIndexCagraAlgo.ToString(), Hooks{})
}

// writerCapacity bounds the CDC buffer the writer accumulates between
// Full() / ToSql() drains. Matches HNSW's writer.
const writerCapacity = 8192

// Hooks implements iscp.Hooks for CAGRA.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

// NewSqlWriter constructs a CagraSqlWriter from the per-(table,index)
// def. CAGRA is fp32-only on cuvs, so unlike HNSW there's no float64
// branch.
func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return newCagraSqlWriter(jobID, info, tabledef, indexdefs)
}

// Run drives one consumer iteration. Drains the writer's JSON CDC
// blobs, applies them to a CagraSync, then persists on close.
func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	runCagra(c, ctx, errch, r)
}

// CagraSqlWriter buffers CDC row events as a vectorindex.VectorIndexCdc
// blob and emits JSON on ToSql. Mirrors HnswSqlWriter; CAGRA's GPU
// element type is fp32 only.
type CagraSqlWriter struct {
	cdc       *vectorindex.VectorIndexCdc[float32]
	tabledef  *plan.TableDef
	indexdef  []*plan.IndexDef
	jobID     iscppkg.JobID
	info      *iscppkg.ConsumerInfo
	pkPos     int32
	pkType    *types.Type
	partsPos  []int32
	partsType []*types.Type
	dimension int32
	indexName string
	dbName    string
	tblName   string
	// colMetaJSON is reserved for INCLUDE columns; empty for now.
	// Once the catalog path threads the parsed colmeta through info,
	// populate this from indexAlgoParams to enable INCLUDE replay.
	colMetaJSON string
}

func newCagraSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (*CagraSqlWriter, error) {

	if len(tabledef.Pkey.Names) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("cagra index table only supports one primary key")
	}
	if len(indexdefs) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("cagra index table must have 2 secondary tables")
	}

	idxdef := indexdefs[0]
	if len(idxdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("cagra index must have exactly one vector part")
	}

	w := &CagraSqlWriter{
		tabledef: tabledef,
		indexdef: indexdefs,
		jobID:    jobID,
		info:     info,
		cdc:      vectorindex.NewVectorIndexCdc[float32](writerCapacity),
	}

	w.pkPos = tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	pkTyp := tabledef.Cols[w.pkPos].Typ
	w.pkType = &types.Type{Oid: types.T(pkTyp.Id), Width: pkTyp.Width, Scale: pkTyp.Scale}
	if w.pkType.Oid != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtx("CagraSqlWriter: primary key must be bigint")
	}

	nparts := len(idxdef.Parts)
	w.partsPos = make([]int32, nparts)
	w.partsType = make([]*types.Type, nparts)
	for i, part := range idxdef.Parts {
		w.partsPos[i] = tabledef.Name2ColIndex[part]
		t := tabledef.Cols[w.partsPos[i]].Typ
		w.partsType[i] = &types.Type{Oid: types.T(t.Id), Width: t.Width, Scale: t.Scale}
	}
	vecTyp := tabledef.Cols[w.partsPos[0]].Typ
	if vecTyp.Id != int32(types.T_array_float32) {
		return nil, moerr.NewInternalErrorNoCtx("CagraSqlWriter: vector column must be vecf32 (cuvs CAGRA is fp32-only)")
	}
	w.dimension = vecTyp.Width

	w.indexName = info.IndexName
	w.dbName = info.DBName
	w.tblName = info.TableName

	return w, nil
}

func (w *CagraSqlWriter) Reset() {
	w.cdc.Data = w.cdc.Data[:0]
}

func (w *CagraSqlWriter) Full() bool {
	return len(w.cdc.Data) >= cap(w.cdc.Data)
}

func (w *CagraSqlWriter) Empty() bool {
	return len(w.cdc.Data) == 0
}

func (w *CagraSqlWriter) CheckLastOp(_ string) bool { return true }

func (w *CagraSqlWriter) Insert(ctx context.Context, row []any) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "cagra writer: invalid key type, expected int64")
	}
	if row[w.partsPos[0]] == nil {
		w.cdc.Delete(key)
		return nil
	}
	v, ok := row[w.partsPos[0]].([]float32)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("cagra writer: invalid vector type, expected []float32, got %T", row[w.partsPos[0]]))
	}
	if v == nil {
		w.cdc.Delete(key)
		return nil
	}
	w.cdc.Insert(key, v, nil)
	return nil
}

func (w *CagraSqlWriter) Upsert(ctx context.Context, row []any) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "cagra writer: invalid key type, expected int64")
	}
	if row[w.partsPos[0]] == nil {
		w.cdc.Delete(key)
		return nil
	}
	v, ok := row[w.partsPos[0]].([]float32)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("cagra writer: invalid vector type, expected []float32, got %T", row[w.partsPos[0]]))
	}
	if v == nil {
		w.cdc.Delete(key)
		return nil
	}
	w.cdc.Upsert(key, v, nil)
	return nil
}

func (w *CagraSqlWriter) Delete(ctx context.Context, row []any) error {
	// First column is the primary key in delete rows.
	key, ok := row[0].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "cagra writer: invalid key type, expected int64")
	}
	w.cdc.Delete(key)
	return nil
}

func (w *CagraSqlWriter) ToSql() ([]byte, error) {
	js, err := w.cdc.ToJson()
	if err != nil {
		return nil, err
	}
	return []byte(js), nil
}

// newSync builds a CagraSync from this writer's metadata.
func (w *CagraSqlWriter) newSync(sqlproc *sqlexec.SqlProcess) (*cagra.CagraSync, error) {
	return cagra.NewCagraSync(sqlproc, w.dbName, w.tblName, w.indexName, w.indexdef, w.dimension, w.colMetaJSON)
}

// runCagra drives the CDC consumer loop for CAGRA — equivalent to
// runHnsw but instantiating cagra.CagraSync instead of hnsw.HnswSync.
// Reads JSON CDC blobs from the writer's send channel, applies them
// via CagraSync.Update, then persists on close via Save and updates
// the tail watermark.
func runCagra(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	datatype := r.GetDataType()

	w, ok := c.SqlWriter().(*CagraSqlWriter)
	if !ok {
		errch <- moerr.NewInternalError(ctx, fmt.Sprintf("runCagra: unexpected writer type %T", c.SqlWriter()))
		return
	}

	var sync *cagra.CagraSync
	err := c.RunTxn(ctx, r, 30*time.Minute, func(sqlproc *sqlexec.SqlProcess) error {
		s, e := w.newSync(sqlproc)
		if e != nil {
			return e
		}
		sync = s
		return nil
	})
	if err != nil {
		errch <- err
		return
	}
	if sync == nil {
		errch <- moerr.NewInternalErrorNoCtx("runCagra: failed to create CagraSync")
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
		case sql, open := <-c.SqlBufSendCh():
			if !open {
				// Channel closed — persist model + update watermark.
				err := c.RunTxn(ctx, r, time.Hour, func(sqlproc *sqlexec.SqlProcess) error {
					if e := sync.Save(sqlproc); e != nil {
						return e
					}
					if datatype == iscppkg.ISCPDataType_Tail {
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

			var cdc vectorindex.VectorIndexCdc[float32]
			if err := sonic.Unmarshal(sql, &cdc); err != nil {
				errch <- err
				return
			}

			err := c.RunTxn(ctx, r, 30*time.Minute, func(sqlproc *sqlexec.SqlProcess) error {
				return sync.Update(sqlproc, &cdc)
			})
			if err != nil {
				errch <- err
				return
			}
		}
	}
}
