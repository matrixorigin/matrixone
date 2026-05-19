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

// Package iscp provides IVF-PQ's ISCP hook layer. Mirrors CAGRA's
// plugin/iscp package — see pkg/vectorindex/cagra/plugin/iscp/iscp.go
// for the architectural commentary.
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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const writerCapacity = 8192

func init() {
	iscppkg.Register(catalog.MoIndexIvfpqAlgo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for IVF-PQ.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return newIvfpqSqlWriter(jobID, info, tabledef, indexdefs)
}

func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	runIvfpq(c, ctx, errch, r)
}

// IvfpqSqlWriter buffers CDC row events as a vectorindex.VectorIndexCdc
// blob and emits JSON on ToSql. cuvs IVF-PQ is fp32-only.
type IvfpqSqlWriter struct {
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
	colMetaJSON string
}

func newIvfpqSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (*IvfpqSqlWriter, error) {

	if len(tabledef.Pkey.Names) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq index table only supports one primary key")
	}
	if len(indexdefs) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq index table must have 2 secondary tables")
	}

	idxdef := indexdefs[0]
	if len(idxdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq index must have exactly one vector part")
	}

	w := &IvfpqSqlWriter{
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
		return nil, moerr.NewInternalErrorNoCtx("IvfpqSqlWriter: primary key must be bigint")
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
		return nil, moerr.NewInternalErrorNoCtx("IvfpqSqlWriter: vector column must be vecf32 (cuvs IVF-PQ is fp32-only)")
	}
	w.dimension = vecTyp.Width

	w.indexName = info.IndexName
	w.dbName = info.DBName
	w.tblName = info.TableName

	return w, nil
}

func (w *IvfpqSqlWriter) Reset()                  { w.cdc.Data = w.cdc.Data[:0] }
func (w *IvfpqSqlWriter) Full() bool              { return len(w.cdc.Data) >= cap(w.cdc.Data) }
func (w *IvfpqSqlWriter) Empty() bool             { return len(w.cdc.Data) == 0 }
func (w *IvfpqSqlWriter) CheckLastOp(_ string) bool { return true }

func (w *IvfpqSqlWriter) Insert(ctx context.Context, row []any) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "ivfpq writer: invalid key type, expected int64")
	}
	if row[w.partsPos[0]] == nil {
		w.cdc.Delete(key)
		return nil
	}
	v, ok := row[w.partsPos[0]].([]float32)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("ivfpq writer: invalid vector type, expected []float32, got %T", row[w.partsPos[0]]))
	}
	if v == nil {
		w.cdc.Delete(key)
		return nil
	}
	w.cdc.Insert(key, v, nil)
	return nil
}

func (w *IvfpqSqlWriter) Upsert(ctx context.Context, row []any) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "ivfpq writer: invalid key type, expected int64")
	}
	if row[w.partsPos[0]] == nil {
		w.cdc.Delete(key)
		return nil
	}
	v, ok := row[w.partsPos[0]].([]float32)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("ivfpq writer: invalid vector type, expected []float32, got %T", row[w.partsPos[0]]))
	}
	if v == nil {
		w.cdc.Delete(key)
		return nil
	}
	w.cdc.Upsert(key, v, nil)
	return nil
}

func (w *IvfpqSqlWriter) Delete(ctx context.Context, row []any) error {
	key, ok := row[0].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "ivfpq writer: invalid key type, expected int64")
	}
	w.cdc.Delete(key)
	return nil
}

func (w *IvfpqSqlWriter) ToSql() ([]byte, error) {
	js, err := w.cdc.ToJson()
	if err != nil {
		return nil, err
	}
	return []byte(js), nil
}

func (w *IvfpqSqlWriter) newSync(sqlproc *sqlexec.SqlProcess) (*ivfpq.IvfpqSync, error) {
	return ivfpq.NewIvfpqSync(sqlproc, w.dbName, w.tblName, w.indexName, w.indexdef, w.dimension, w.colMetaJSON)
}

// runIvfpq drives the CDC consumer loop for IVF-PQ. Mirrors runCagra
// — see pkg/vectorindex/cagra/plugin/iscp/iscp.go.
func runIvfpq(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	datatype := r.GetDataType()

	w, ok := c.SqlWriter().(*IvfpqSqlWriter)
	if !ok {
		errch <- moerr.NewInternalError(ctx, fmt.Sprintf("runIvfpq: unexpected writer type %T", c.SqlWriter()))
		return
	}

	var sync *ivfpq.IvfpqSync
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
		errch <- moerr.NewInternalErrorNoCtx("runIvfpq: failed to create IvfpqSync")
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
