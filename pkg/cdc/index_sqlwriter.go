package cdc

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

type IndexSqlWriter interface {
	CheckLastOp(op string) bool
	Upsert(ctx context.Context, row []any) error
	Insert(ctx context.Context, row []any) error
	Delete(ctx context.Context, row []any) error
	Full() bool
	ToSql() ([]byte, error)
}

type BaseIndexSqlWriter struct {
	lastCdcOp string
	vbuf      []byte
	param     string
	tabledef  *plan.TableDef
	indexdef  []*plan.IndexDef
	algo      string
	pkPos     int32
	pkType    *types.Type
	partsPos  []int32
	partsType []*types.Type
	srcPos    []int32
	srcType   []*types.Type
}

type FulltextSqlWriter struct {
	BaseIndexSqlWriter
}

// check FulltextSqlWriter is the interface of IndexSqlWriter
var _ IndexSqlWriter = new(FulltextSqlWriter)

// check algo type to return the correct sql writer
func NewIndexSqlWriter(algo string, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	switch algo {
	case "fulltext":
		return NewFulltextSqlWriter(algo, tabledef, indexdef)
	case "ivfflat":
	default:
		return IndexSqlWriter(nil), moerr.NewInternalErrorNoCtx("IndexSqlWriter: invalid algo type")

	}
	return IndexSqlWriter(nil), moerr.NewInternalErrorNoCtx("IndexSqlWriter: invalid algo type")
}

func NewFulltextSqlWriter(algo string, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	w := &FulltextSqlWriter{BaseIndexSqlWriter: BaseIndexSqlWriter{algo: algo, tabledef: tabledef, indexdef: indexdef, vbuf: make([]byte, 0, 1024)}}

	w.pkPos = tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	typ := tabledef.Cols[w.pkPos].Typ
	w.pkType = &types.Type{Oid: types.T(typ.Id), Width: typ.Width, Scale: typ.Scale}

	nparts := len(w.indexdef[0].Parts)
	w.partsPos = make([]int32, nparts)
	w.partsType = make([]*types.Type, nparts)

	for i, part := range w.indexdef[0].Parts {
		w.partsPos[i] = tabledef.Name2ColIndex[part]
		typ = tabledef.Cols[w.partsPos[i]].Typ
		w.partsType[i] = &types.Type{Oid: types.T(typ.Id), Width: typ.Width, Scale: typ.Scale}
	}

	w.srcPos = make([]int32, nparts+1)
	w.srcType = make([]*types.Type, nparts+1)

	w.srcPos[0] = w.pkPos
	w.srcType[0] = w.pkType
	for i := range w.partsType {
		w.srcPos[i+1] = w.partsPos[i]
		w.srcType[i+1] = w.partsType[i]
	}

	return w, nil
}

func (w *BaseIndexSqlWriter) Full() bool {
	return false
}

// return true when last op is empty or last op == current op
func (w *BaseIndexSqlWriter) CheckLastOp(op string) bool {
	return len(w.lastCdcOp) == 0 || w.lastCdcOp == op
}

func (w *BaseIndexSqlWriter) writeRow(ctx context.Context, row []any) error {
	var err error

	w.vbuf = appendString(w.vbuf, "ROW(")

	// pk
	w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkType, w.vbuf)
	if err != nil {
		return err
	}

	for i, t := range w.partsType {
		w.vbuf = appendString(w.vbuf, ",")
		pos := w.partsPos[i]
		w.vbuf, err = convertColIntoSql(ctx, row[pos], t, w.vbuf)
		if err != nil {
			return err
		}
	}

	w.vbuf = appendString(w.vbuf, ")")
	return nil
}

func (w *BaseIndexSqlWriter) Upsert(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_UPSERT
		return w.writeRow(ctx, row)

	}

	if w.lastCdcOp != vectorindex.CDC_UPSERT {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("FulltextSqlWriter.Upsert: append different op")
	}

	// same as previous operation and append to VALUES ROW(), ROW(),...
	w.vbuf = appendString(w.vbuf, ",")
	return w.writeRow(ctx, row)
}

func (w *BaseIndexSqlWriter) Insert(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_INSERT
		return w.writeRow(ctx, row)

	}

	if w.lastCdcOp != vectorindex.CDC_INSERT {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("FulltextSqlWriter.Insert: append different op")
	}

	// same as previous operation and append to VALUES ROW(), ROW(),...
	w.vbuf = appendString(w.vbuf, ",")
	return w.writeRow(ctx, row)

	return nil
}

func (w *BaseIndexSqlWriter) Delete(ctx context.Context, row []any) error {
	var err error

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_DELETE
		w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkType, w.vbuf)
		if err != nil {
			return err
		}

	}

	if w.lastCdcOp != vectorindex.CDC_DELETE {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("FulltextSqlWriter.Delete: append different op")
	}

	// same as previous operation and append to IN ()
	w.vbuf = appendString(w.vbuf, ",")
	w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkType, w.vbuf)
	if err != nil {
		return err
	}

	return nil
}

// with src as (select cast(serial(cast(column_0 as bigint), cast(column_1 as bigint)) as varchar) as id, column_2 as body, column_3 as title from
// (values row(1, 2, 'body', 'title'), row(2, 3, 'body is heavy', 'I do not know'))) select f.* from src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 61, id, body, title) as f;
func (w *FulltextSqlWriter) ToFulltextSql() ([]byte, error) {

	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
	case vectorindex.CDC_UPSERT:
		return w.ToFulltextUpsert(true)
	case vectorindex.CDC_INSERT:
		return w.ToFulltextUpsert(false)
	default:
		return nil, moerr.NewInternalErrorNoCtx("FulltextSqlWriter: invalid CDC type")
	}

	return nil, nil
}

func (w *FulltextSqlWriter) ToFulltextUpsert(upsert bool) ([]byte, error) {

	var sql string

	coldefs := make([]string, 0, len(w.srcPos))
	cnames := make([]string, 0, len(w.srcPos))
	for i, pos := range w.srcPos {
		typstr := w.srcType[i].DescString()
		coldefs = append(coldefs, fmt.Sprintf("CAST(column_%d as %s) as %s", i, typstr, w.tabledef.Cols[pos].Name))
		cnames = append(cnames, w.tabledef.Cols[pos].Name)
	}

	cols := strings.Join(coldefs, ", ")
	cnames_str := strings.Join(cnames, ", ")

	/*
		if upsert {
			sql += fmt.Sprintf("REPLACE INTO %s ", tablename)
		} else {
			sql += fmt.Sprintf("INSERT INTO %s ", tablename)
		}
	*/

	sql += fmt.Sprintf("WITH src as (SELECT %s FROM (VALUES %s)) ", cols, string(w.vbuf))
	sql += fmt.Sprintf("SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('%s', %d, %s) as f", w.param, w.pkType.Oid, cnames_str)

	fmt.Printf("SQL :%s\n", sql)
	return []byte(sql), nil
}

// REPLACE INTO __mo_index_secondary_0197786c-285f-70cb-9337-e484a3ff92c4(__mo_index_centroid_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col, __mo_index_centroid_fk_entry)
// with centroid as (select * from __mo_index_secondary_0197786c-285f-70bb-b277-2cef56da590a where __mo_index_centroid_version = 0),
// src as (select column_0 as id, cast(column_1 as vecf32(3)) as embed from (values row(2005,'[0.4532634, 0.7297859, 0.48885703]'), row(2009, '[0.68150306, 0.6950923, 0.16590895] ')))
// select __mo_index_centroid_version, __mo_index_centroid_id, id, embed from src centroidx('vector_l2_ops') join centroid using (__mo_index_centroid, embed);
func (w *FulltextSqlWriter) ToIvfflatSql() ([]byte, error) {
	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
	case vectorindex.CDC_UPSERT:
	case vectorindex.CDC_INSERT:
	default:
		return nil, moerr.NewInternalErrorNoCtx("FulltextSqlWriter: invalid CDC type")
	}

	return nil, nil
}

func (w *FulltextSqlWriter) ToSql() ([]byte, error) {
	defer func() {
		w.lastCdcOp = ""
	}()
	switch w.algo {
	case "fulltext":
		return w.ToFulltextSql()
	case "ivfflat":
		return w.ToIvfflatSql()
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid algorithm type.")

	}

	return nil, nil
}
