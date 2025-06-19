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

type IndexSqlWriter struct {
	param     string
	tabledef  *plan.TableDef
	algo      string
	pkPos     int
	pkType    *plan.Type
	partsPos  []int
	partsType []*plan.Type
	srcPos    []int
	lastCdcOp string
	buf       []byte
	vbuf      []byte
}

func NewIndexSqlWriter(algo string, tabledef *plan.TableDef, param string) *IndexSqlWriter {
	return &IndexSqlWriter{algo: algo, tabledef: tabledef, param: param, vbuf: make([]byte, 0, 1024)}
	w.srcPos[0] = w.pkPos
	for i := range w.partsType {
		w.srcPos[i+1] = w.partsPos[i]
	}
}

// return true when last op is empty or last op == current op
func (w *IndexSqlWriter) checkLastOp(op string) bool {
	return len(w.lastCdcOp) == 0 || w.lastCdcOp == op
}

func (w *IndexSqlWriter) writeRow(ctx context.Context, row []any) error {

	w.vbuf = appendString(w.vbuf, "ROW(")

	// pk
	w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkTyp, w.vbuf)
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
}

func (w *IndexSqlWriter) Upsert(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_UPSERT
		return w.writeRow(ctx, row)

	}

	if w.lastCdcOp != vectorindex.CDC_UPSERT {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("IndexSqlWriter.Upsert: append different op")
	}

	// same as previous operation and append to VALUES ROW(), ROW(),...
	w.vbuf = appendString(w.vbuf, ",")
	return w.writeRow(ctx, row)
}

func (w *IndexSqlWriter) Insert(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_INSERT
		return w.writeRow(ctx, row)

	}

	if w.lastCdcOp != vectorindex.CDC_INSERT {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("IndexSqlWriter.Insert: append different op")
	}

	// same as previous operation and append to VALUES ROW(), ROW(),...
	w.vbuf = appendString(w.vbuf, ",")
	return w.writeRow(ctx, row)

	return nil
}

func (w *IndexSqlWriter) Delete(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_DELETE
		w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkTyp, w.vbuf)
		if err != nil {
			return err
		}

	}

	if w.lastCdcOp != vectorindex.CDC_DELETE {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("IndexSqlWriter.Delete: append different op")
	}

	// same as previous operation and append to IN ()
	w.vbuf = appendString(w.vbuf, ",")
	w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkTyp, w.vbuf)
	if err != nil {
		return err
	}

	return nil
}

// with src as (select cast(serial(cast(column_0 as bigint), cast(column_1 as bigint)) as varchar) as id, column_2 as body, column_3 as title from
// (values row(1, 2, 'body', 'title'), row(2, 3, 'body is heavy', 'I do not know'))) select f.* from src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 61, id, body, title) as f;
func (w *IndexSqlWriter) ToFullTextSql() ([]byte, error) {

	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
	case vectorindex.CDC_UPSERT:
	case vectorindex.CDC_INSERT:
	default:
		return moerr.NewInternalErrorNoCtx("IndexSqlWriter: invalid CDC type")
	}

	return nil, nil
}

func (w *IndexSqlWriter) ToIvfflatUpsert() ([]byte, error) {

	var sql string

	coldefs := make([]string, 0, len(w.srcPos))
	cnames := make([]string, 0, len(w.srcPos))
	for i, pos := range w.srcPos {
		typ := w.tabledef.Cols[pos].Typ
		newtype := types.New(types.T(typ.Id), typ.Width, typ.Scale)
		typstr := t.DescString()
		coldefs := append(coldef, fmt.Sprintf("CAST(column_%d as %s) as %d", i, typstr, w.tabledef.Cols[pos].Name))
		cnames := append(cnames, w.tabledef.Cols[pos].Name)
	}

	cols := strings.Join(coldefs, ", ")
	cnames_str := strings.Join(cnames, ", ")

	sql += fmt.Sprintf("REPLACE INTO %s ", tablename)
	sql += fmt.Sprintf("WITH src as (SELECT %s FROM (VALUES %s))", cols, string(w.vbuf))
	sql += fmt.Sprintf(" SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('%s', %d, %s) as f", param, w.pkType.Id, cnames_str)
}

// REPLACE INTO __mo_index_secondary_0197786c-285f-70cb-9337-e484a3ff92c4(__mo_index_centroid_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col, __mo_index_centroid_fk_entry)
// with centroid as (select * from __mo_index_secondary_0197786c-285f-70bb-b277-2cef56da590a where __mo_index_centroid_version = 0),
// src as (select column_0 as id, cast(column_1 as vecf32(3)) as embed from (values row(2005,'[0.4532634, 0.7297859, 0.48885703]'), row(2009, '[0.68150306, 0.6950923, 0.16590895] ')))
// select __mo_index_centroid_version, __mo_index_centroid_id, id, embed from src centroidx('vector_l2_ops') join centroid using (__mo_index_centroid, embed);
func (w *IndexSqlWriter) ToIvfflatSql() ([]byte, error) {
	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
	case vectorindex.CDC_UPSERT:
	case vectorindex.CDC_INSERT:
	default:
		return moerr.NewInternalErrorNoCtx("IndexSqlWriter: invalid CDC type")
	}

	return nil, nil
}

func (w *IndexSqlWriter) ToSql() ([]byte, error) {
	w.lastCdcOp = ""
	switch w.algo {
	case "fulltext":
		return w.ToFullTextSql()
	case "ivfflat":
		return w.ToIvfflatSql()
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid algorithm type.")

	}

	return nil, nil
}
