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

package indexfile

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var scanColumns = []string{
	catalog.IndexStorage_TblCol_Index_Id,
	catalog.IndexStorage_TblCol_Chunk_Id,
	catalog.IndexStorage_TblCol_Data,
	catalog.IndexStorage_TblCol_Tag,
}

// openRelation opens ref's storage table in env's transaction.
func openRelation(ctx context.Context, env *Env, ref *Ref) (engine.Relation, error) {
	db, err := env.Engine.Database(ctx, ref.DB, env.Txn)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(ctx, ref.Table, env.Proc)
	if err != nil {
		return nil, err
	}
	return engine.NewRelationHandle(rel), nil
}

// scanChunks streams to fn the rows of ref's chunk range visible to env's transaction.
// data is valid only during the call.
func scanChunks(ctx context.Context, env *Env, rel engine.Relation, ref *Ref, fn func(chunkID int64, data []byte) error) (err error) {
	if ref.numChunks() == 0 {
		return nil
	}
	tableDef := rel.GetTableDef(ctx)
	if tableDef == nil {
		return moerr.NewInvalidStateNoCtxf("index file: %s.%s has no table definition", ref.DB, ref.Table)
	}
	filter, err := chunkRangeFilter(ctx, tableDef, ref)
	if err != nil {
		return err
	}
	txnOffset := workspaceOffset(env)
	relData, err := rel.Ranges(ctx, engine.RangesParam{
		BlockFilters: []*plan.Expr{filter},
		TxnOffset:    txnOffset,
		Policy:       engine.Policy_CollectAllData,
	})
	if err != nil {
		return err
	}
	readers, err := rel.BuildReaders(ctx, env.Proc, filter, relData, 1, txnOffset, false,
		engine.Policy_CheckAll, engine.FilterHint{})
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range readers {
			if closeErr := r.Close(); err == nil {
				err = closeErr
			}
		}
	}()

	mp := env.Proc.Mp()
	for _, r := range readers {
		for {
			bat, err := newScanBatch(tableDef)
			if err != nil {
				return err
			}
			end, err := r.Read(ctx, scanColumns, filter, mp, bat)
			if err == nil && !end {
				err = forEachChunk(bat, ref, fn)
			}
			bat.Clean(mp)
			if err != nil {
				return err
			}
			if end {
				break
			}
		}
	}
	return nil
}

// workspaceOffset is the workspace position a SQL SELECT on env.Proc would read to: the
// whole workspace for borrowed SQL that opens no statement, otherwise the statement
// boundary.
func workspaceOffset(env *Env) int {
	ws := env.Txn.GetWorkspace()
	if env.Proc.IncrStatementDisabled() {
		return int(ws.WriteOffset())
	}
	return ws.GetSnapshotWriteOffset()
}

func forEachChunk(bat *batch.Batch, ref *Ref, fn func(chunkID int64, data []byte) error) error {
	ids, chunks, datas, tags := bat.Vecs[0], bat.Vecs[1], bat.Vecs[2], bat.Vecs[3]
	chunkIDs := vector.MustFixedColNoTypeCheck[int64](chunks)
	tagVals := vector.MustFixedColNoTypeCheck[int64](tags)
	indexID := []byte(ref.IndexID)
	for i := 0; i < bat.RowCount(); i++ {
		if tagVals[i] != ref.Tag || !bytes.Equal(ids.GetBytesAt(i), indexID) {
			continue
		}
		if err := fn(chunkIDs[i], datas.GetBytesAt(i)); err != nil {
			return err
		}
	}
	return nil
}

func newScanBatch(tableDef *plan.TableDef) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(scanColumns))
	bat.Attrs = append(bat.Attrs, scanColumns...)
	for i, name := range scanColumns {
		pos, ok := tableDef.Name2ColIndex[name]
		if !ok || int(pos) >= len(tableDef.Cols) {
			bat.Clean(nil)
			return nil, moerr.NewInternalErrorNoCtxf("index file: column %q not found in %s", name, tableDef.Name)
		}
		typ := tableDef.Cols[pos].Typ
		bat.Vecs[i] = vector.NewVec(types.New(types.T(typ.Id), typ.Width, typ.Scale))
	}
	return bat, nil
}

// chunkRangeFilter is prefix_between(cpkey, serial(index_id, first), serial(index_id, last)).
func chunkRangeFilter(ctx context.Context, tableDef *plan.TableDef, ref *Ref) (*plan.Expr, error) {
	pos, ok := tableDef.Name2ColIndex[catalog.CPrimaryKeyColName]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("index file: %s has no composite primary key", tableDef.Name)
	}
	typ := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Charset: uint32(types.CharsetBinary)}
	col := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: catalog.CPrimaryKeyColName, ColPos: pos}}}
	lo := bytesExpr(serialKey(ref.IndexID, ref.FirstChunk), typ)
	hi := bytesExpr(serialKey(ref.IndexID, ref.FirstChunk+ref.numChunks()-1), typ)
	argTypes := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}
	fn, err := function.GetFunctionByName(ctx, "prefix_between", argTypes)
	if err != nil {
		return nil, err
	}
	rt := fn.GetReturnType()
	return &plan.Expr{
		Typ: plan.Type{Id: int32(rt.Oid), Width: rt.Width, Scale: rt.Scale},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "prefix_between"},
			Args: []*plan.Expr{col, lo, hi},
		}},
	}, nil
}

// serialKey is the composite primary key serial(index_id, chunk_id).
func serialKey(indexID string, chunkID int64) []byte {
	p := types.NewPacker()
	defer p.Close()
	p.EncodeStringType([]byte(indexID))
	p.EncodeInt64(chunkID)
	return append([]byte(nil), p.Bytes()...)
}

func bytesExpr(b []byte, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ:  typ,
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: string(b)}}},
	}
}
