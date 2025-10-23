// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	MAX_CDC_DATA_SIZE = 8192
)

// IndexSqlWriter interface
type IndexSqlWriter interface {
	CheckLastOp(op string) bool
	Upsert(ctx context.Context, row []any) error
	Insert(ctx context.Context, row []any) error
	Delete(ctx context.Context, row []any) error
	Full() bool
	ToSql() ([]byte, error)
	Reset()
	Empty() bool
}

// Base implementation of IVFFLAT and FULLTEXT.  Their implementation are simliar.
type BaseIndexSqlWriter struct {
	lastCdcOp string
	vbuf      []byte
	ndata     int
	param     string
	tabledef  *plan.TableDef
	indexdef  []*plan.IndexDef
	jobID     JobID
	info      *ConsumerInfo
	algo      string
	pkPos     int32
	pkType    *types.Type
	partsPos  []int32
	partsType []*types.Type
	srcPos    []int32
	srcType   []*types.Type
	dbName    string
}

// Fulltext Sql Writer.  Only one hidden secondary index table
type FulltextSqlWriter struct {
	BaseIndexSqlWriter
	indexTableName string
}

// Ivfflat Sql writer. Three hidden secondary index tables
type IvfflatSqlWriter struct {
	BaseIndexSqlWriter
	centroids_tbl string
	entries_tbl   string
	meta_tbl      string
	ivfparam      vectorindex.IvfParam
}

// Hnsw Sql Writer.  Use the vectorindex.VectorIndeXCdc JSON format
type HnswSqlWriter[T types.RealNumbers] struct {
	cdc       *vectorindex.VectorIndexCdc[T]
	meta      vectorindex.HnswCdcParam
	tabledef  *plan.TableDef
	indexdef  []*plan.IndexDef
	jobID     JobID
	info      *ConsumerInfo
	pkPos     int32
	pkType    *types.Type
	partsPos  []int32
	partsType []*types.Type
	srcPos    []int32
	srcType   []*types.Type
	dbName    string
}

// check FulltextSqlWriter is the interface of IndexSqlWriter
var _ IndexSqlWriter = new(FulltextSqlWriter)
var _ IndexSqlWriter = new(IvfflatSqlWriter)
var _ IndexSqlWriter = new(HnswSqlWriter[float32])

// check algo type to return the correct sql writer
func NewIndexSqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	algo = catalog.ToLower(algo)
	switch algo {
	case catalog.MOIndexFullTextAlgo.ToString():
		return NewFulltextSqlWriter(algo, jobID, info, tabledef, indexdef)
	case catalog.MoIndexIvfFlatAlgo.ToString():
		return NewIvfflatSqlWriter(algo, jobID, info, tabledef, indexdef)
	case catalog.MoIndexHnswAlgo.ToString():
		return NewHnswSqlWriter(algo, jobID, info, tabledef, indexdef)
	default:
		return IndexSqlWriter(nil), moerr.NewInternalErrorNoCtx(fmt.Sprintf("IndexSqlWriter: invalid algo type: %s", algo))

	}
}

// Implementation of Base Index SqlWriter
func (w *BaseIndexSqlWriter) Full() bool {
	return w.ndata >= MAX_CDC_DATA_SIZE
}

// return true when last op is empty or last op == current op
func (w *BaseIndexSqlWriter) CheckLastOp(op string) bool {
	return len(w.lastCdcOp) == 0 || w.lastCdcOp == op
}

func (w *BaseIndexSqlWriter) writeRow(ctx context.Context, row []any) error {
	var err error

	w.vbuf = appendString(w.vbuf, "ROW(")

	// pk
	if w.tabledef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		cpkType := &types.Type{Oid: types.T_varbinary, Width: w.pkType.Width, Scale: w.pkType.Scale}
		w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], cpkType, w.vbuf)
		if err != nil {
			return err
		}
	} else {
		w.vbuf, err = convertColIntoSql(ctx, row[w.pkPos], w.pkType, w.vbuf)
		if err != nil {
			return err
		}
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
	w.ndata += 1
	return nil
}

func (w *BaseIndexSqlWriter) writeDeleteRow(ctx context.Context, row []any) error {
	var err error

	if w.tabledef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		cpkType := &types.Type{Oid: types.T_varbinary, Width: w.pkType.Width, Scale: w.pkType.Scale}
		w.vbuf, err = convertColIntoSql(ctx, row[0], cpkType, w.vbuf)
		if err != nil {
			return err
		}
	} else {
		w.vbuf, err = convertColIntoSql(ctx, row[0], w.pkType, w.vbuf)
		if err != nil {
			return err
		}

	}
	w.ndata += 1
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
}

func (w *BaseIndexSqlWriter) Delete(ctx context.Context, row []any) error {

	if len(w.lastCdcOp) == 0 {
		// init
		w.lastCdcOp = vectorindex.CDC_DELETE
		return w.writeDeleteRow(ctx, row)
	}

	if w.lastCdcOp != vectorindex.CDC_DELETE {
		// different from previous operation and generate SQL before append new UPSERT
		return moerr.NewInternalErrorNoCtx("FulltextSqlWriter.Delete: append different op")
	}

	// same as previous operation and append to IN ()
	w.vbuf = appendString(w.vbuf, ",")
	return w.writeDeleteRow(ctx, row)
}

func (w *BaseIndexSqlWriter) Reset() {
	w.lastCdcOp = ""
	w.vbuf = w.vbuf[:0]
	w.ndata = 0
}

func (w *BaseIndexSqlWriter) Empty() bool {
	return len(w.vbuf) == 0
}

// New Fulltext Sql Writer
func NewFulltextSqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	w := &FulltextSqlWriter{BaseIndexSqlWriter: BaseIndexSqlWriter{algo: algo, tabledef: tabledef, indexdef: indexdef, jobID: jobID, info: info, vbuf: make([]byte, 0, 1024)}}

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

	w.indexTableName = w.indexdef[0].IndexTableName
	w.dbName = tabledef.DbName

	return w, nil
}

// with src as (select cast(serial(cast(column_0 as bigint), cast(column_1 as bigint)) as varchar) as id, column_2 as body, column_3 as title from
// (values row(1, 2, 'body', 'title'), row(2, 3, 'body is heavy', 'I do not know'))) select f.* from src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 61, id, body, title) as f;
func (w *FulltextSqlWriter) ToSql() ([]byte, error) {
	defer w.Reset()

	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
		return w.toFulltextDelete()
	case vectorindex.CDC_UPSERT:
		return w.toFulltextUpsert(true)
	case vectorindex.CDC_INSERT:
		return w.toFulltextUpsert(false)
	default:
		return nil, moerr.NewInternalErrorNoCtx("FulltextSqlWriter: invalid CDC type")
	}
}

func (w *FulltextSqlWriter) toFulltextDelete() ([]byte, error) {
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (%s)", w.info.DBName, w.indexTableName, catalog.FullTextIndex_TabCol_Id, string(w.vbuf))
	return []byte(sql), nil
}

func (w *FulltextSqlWriter) toFulltextUpsert(upsert bool) ([]byte, error) {

	var sql string

	coldefs := make([]string, 0, len(w.srcPos))
	cnames := make([]string, 0, len(w.srcPos))
	for i, pos := range w.srcPos {
		typstr := w.srcType[i].DescString()
		coldefs = append(coldefs, fmt.Sprintf("CAST(column_%d as %s) as `%s`", i, typstr, w.tabledef.Cols[pos].Name))
		cnames = append(cnames, w.tabledef.Cols[pos].Name)
	}

	cols := strings.Join(coldefs, ", ")
	cnames_str := strings.Join(cnames, ", ")

	if upsert {
		sql += fmt.Sprintf("REPLACE INTO `%s`.`%s` ", w.dbName, w.indexTableName)
	} else {
		// IMPORTANT: even it is a INSERT but we still use REPLACE
		// sql += fmt.Sprintf("INSERT INTO `%s`.`%s` ", w.dbName, w.indexTableName)
		sql += fmt.Sprintf("REPLACE INTO `%s`.`%s` ", w.dbName, w.indexTableName)
	}

	sql += fmt.Sprintf("WITH src as (SELECT %s FROM (VALUES %s)) ", cols, string(w.vbuf))
	sql += fmt.Sprintf("SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('%s', %d, %s) as f", w.param, w.pkType.Oid, cnames_str)

	return []byte(sql), nil
}

func NewGenericHnswSqlWriter[T types.RealNumbers](algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {

	// get the first indexdef as they are the same
	idxdef := indexdef[0]
	writer_capacity := 8192

	w := &HnswSqlWriter[T]{tabledef: tabledef, indexdef: indexdef, jobID: jobID, info: info, cdc: vectorindex.NewVectorIndexCdc[T](writer_capacity)}

	paramstr := idxdef.IndexAlgoParams
	var meta, storage string
	for _, idx := range indexdef {
		if idx.IndexAlgoTableType == catalog.Hnsw_TblType_Metadata {
			meta = idx.IndexTableName
		}
		if idx.IndexAlgoTableType == catalog.Hnsw_TblType_Storage {
			storage = idx.IndexTableName
		}
	}

	if len(meta) == 0 || len(storage) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table either meta or storage hidden index table not exist")
	}

	var hnswparam vectorindex.HnswParam
	if len(paramstr) > 0 {
		err := json.Unmarshal([]byte(paramstr), &hnswparam)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("hnsw sync sinker. failed to convert hnsw param json")
		}
	}

	w.pkPos = tabledef.Name2ColIndex[tabledef.Pkey.PkeyColName]
	typ := tabledef.Cols[w.pkPos].Typ
	w.pkType = &types.Type{Oid: types.T(typ.Id), Width: typ.Width, Scale: typ.Scale}

	if w.pkType.Oid != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtx("NewHnswSqlWriter: primary key is not bigint")
	}

	nparts := len(idxdef.Parts)
	w.partsPos = make([]int32, nparts)
	w.partsType = make([]*types.Type, nparts)

	for i, part := range idxdef.Parts {
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

	w.dbName = tabledef.DbName

	w.meta = vectorindex.HnswCdcParam{
		MetaTbl:   meta,
		IndexTbl:  storage,
		DbName:    info.DBName,
		Table:     info.TableName,
		Params:    hnswparam,
		Dimension: tabledef.Cols[w.partsPos[0]].Typ.Width,
		VecType:   tabledef.Cols[w.partsPos[0]].Typ.Id,
	}

	return w, nil
}

// Implementation of HNSW Sql writer
func NewHnswSqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {

	// check the tabledef and indexdef
	if len(tabledef.Pkey.Names) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table only have one primary key")
	}

	if len(indexdef) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table must have 2 secondary tables")
	}

	idxdef := indexdef[0]
	if len(idxdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table only have one vector part")
	}

	// check vector column type and create IndexSqlWriter
	vecpos := tabledef.Name2ColIndex[idxdef.Parts[0]]
	vectype := tabledef.Cols[vecpos].Typ

	switch vectype.Id {
	case int32(types.T_array_float32):
		return NewGenericHnswSqlWriter[float32](algo, jobID, info, tabledef, indexdef)
	case int32(types.T_array_float64):
		return NewGenericHnswSqlWriter[float64](algo, jobID, info, tabledef, indexdef)
	default:
		return nil, moerr.NewInternalErrorNoCtx("NewHnswSqlWriter: part is not vecf32 or vecf64")
	}
}

func (w *HnswSqlWriter[T]) Reset() {
	w.cdc.Data = w.cdc.Data[:0]
}

func (w *HnswSqlWriter[T]) Full() bool {
	return len(w.cdc.Data) >= cap(w.cdc.Data)
}

func (w *HnswSqlWriter[T]) Empty() bool {
	return len(w.cdc.Data) == 0
}

func (w *HnswSqlWriter[T]) CheckLastOp(op string) bool {
	return true
}

func (w *HnswSqlWriter[T]) Insert(ctx context.Context, row []any) error {
	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid key type. not int64")
	}

	if row[w.partsPos[0]] == nil {
		// vector is nil, do Delete
		w.cdc.Delete(key)
		return nil
	}

	v, ok := row[w.partsPos[0]].([]T)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("invalid vector type. not []float32. %v", row[w.partsPos[0]]))
	}

	if v == nil {
		// vector is nil, do Delete
		w.cdc.Delete(key)
		return nil
	}

	w.cdc.Insert(key, v)
	return nil
}

func (w *HnswSqlWriter[T]) Upsert(ctx context.Context, row []any) error {

	key, ok := row[w.pkPos].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid key type. not int64")
	}

	if row[w.partsPos[0]] == nil {
		// vector is nil, do Delete
		w.cdc.Delete(key)
		return nil
	}

	v, ok := row[w.partsPos[0]].([]T)
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("invalid vector type. not []float32. %v", row[w.partsPos[0]]))
	}

	if v == nil {
		// vector is nil, do Delete
		w.cdc.Delete(key)
		return nil
	}

	w.cdc.Upsert(key, v)
	return nil
}

func (w *HnswSqlWriter[T]) Delete(ctx context.Context, row []any) error {
	// first column is the primary key
	key, ok := row[0].(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid key type. not int64")
	}
	w.cdc.Delete(key)
	return nil
}

func (w *HnswSqlWriter[T]) ToSql() ([]byte, error) {

	// generate sql from cdc
	js, err := w.cdc.ToJson()
	if err != nil {
		return nil, err
	}

	return []byte(js), nil
}

func (w *HnswSqlWriter[T]) NewSync(sqlproc *sqlexec.SqlProcess) (*hnsw.HnswSync[T], error) {
	return hnsw.NewHnswSync[T](sqlproc, w.meta.DbName, w.meta.Table, w.info.IndexName, w.meta.VecType, w.meta.Dimension)
}

// Implementation of Ivfflat Sql writer
func NewIvfflatSqlWriter(algo string, jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdef []*plan.IndexDef) (IndexSqlWriter, error) {
	w := &IvfflatSqlWriter{BaseIndexSqlWriter: BaseIndexSqlWriter{algo: algo, tabledef: tabledef, indexdef: indexdef, jobID: jobID, info: info, vbuf: make([]byte, 0, 1024)}}

	if len(indexdef) != 3 {
		return nil, moerr.NewInternalErrorNoCtx("ivf index table must have 3 secondary tables")
	}

	idxdef := indexdef[0]
	if len(idxdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("ivf index table only have one vector part")
	}

	paramstr := idxdef.IndexAlgoParams
	var centroids_tbl, entries_tbl, meta_tbl string
	for _, idx := range indexdef {
		if idx.IndexAlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Metadata {
			meta_tbl = idx.IndexTableName
		}
		if idx.IndexAlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Centroids {
			centroids_tbl = idx.IndexTableName
		}
		if idx.IndexAlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
			entries_tbl = idx.IndexTableName
		}
	}

	if len(centroids_tbl) == 0 || len(entries_tbl) == 0 || len(meta_tbl) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("ivf index table either meta or centroids or entries hidden index table not exist")
	}

	var ivfparam vectorindex.IvfParam
	if len(paramstr) > 0 {
		err := json.Unmarshal([]byte(paramstr), &ivfparam)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("ivf sync sinker. failed to convert ivf param json")
		}
	}

	w.ivfparam = ivfparam

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

	w.centroids_tbl = centroids_tbl
	w.entries_tbl = entries_tbl
	w.meta_tbl = meta_tbl

	return w, nil
}

// REPLACE INTO __mo_index_secondary_0197786c-285f-70cb-9337-e484a3ff92c4(__mo_index_centroid_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col, __mo_index_centroid_fk_entry)
// with centroid as (select * from __mo_index_secondary_0197786c-285f-70bb-b277-2cef56da590a where __mo_index_centroid_version = 0),
// src as (select column_0 as id, cast(column_1 as vecf32(3)) as embed from (values row(2005,'[0.4532634, 0.7297859, 0.48885703]'), row(2009, '[0.68150306, 0.6950923, 0.16590895] ')))
// select __mo_index_centroid_version, __mo_index_centroid_id, id, embed from src centroidx('vector_l2_ops') join centroid using (__mo_index_centroid, embed);
func (w *IvfflatSqlWriter) ToSql() ([]byte, error) {
	defer w.Reset()

	if len(w.lastCdcOp) == 0 {
		return nil, nil
	}

	switch w.lastCdcOp {
	case vectorindex.CDC_DELETE:
		return w.toIvfflatDelete()
	case vectorindex.CDC_UPSERT:
		return w.toIvfflatUpsert(true)
	case vectorindex.CDC_INSERT:
		return w.toIvfflatUpsert(false)
	default:
		return nil, moerr.NewInternalErrorNoCtx("IvfflatSqlWriter: invalid CDC type")
	}
}

// catalog.SystemSI_IVFFLAT_TblCol_Entries_version
// catalog.SystemSI_IVFFLAT_TblCol_Entries_pk
// catalog.CPrimaryKeyColName
func (w *IvfflatSqlWriter) toIvfflatDelete() ([]byte, error) {
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (%s)", w.info.DBName, w.entries_tbl,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		string(w.vbuf))
	return []byte(sql), nil

}

func (w *IvfflatSqlWriter) toIvfflatUpsert(upsert bool) ([]byte, error) {

	var sql string

	coldefs := make([]string, 0, len(w.srcPos))
	cnames := make([]string, 0, len(w.srcPos))
	for i := range w.srcPos {
		typstr := w.srcType[i].DescString()
		cnames = append(cnames, fmt.Sprintf("src%d", i))
		coldefs = append(coldefs, fmt.Sprintf("CAST(column_%d as %s) as `%s`", i, typstr, cnames[i]))
	}

	cols := strings.Join(coldefs, ", ")
	cnames_str := strings.Join(cnames, ", ")

	if upsert {
		sql += fmt.Sprintf("REPLACE INTO `%s`.`%s` ", w.info.DBName, w.entries_tbl)
	} else {
		// IMPORTANT: even it is a INSERT but we still use REPLACE
		//	sql += fmt.Sprintf("INSERT INTO `%s`.`%s` ", w.info.DBName, w.entries_tbl)
		sql += fmt.Sprintf("REPLACE INTO `%s`.`%s` ", w.info.DBName, w.entries_tbl)
	}

	sql += fmt.Sprintf("(`%s`, `%s`, `%s`, `%s`) ",
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry)

	versql := fmt.Sprintf("SELECT CAST(%s as BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version'", catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		w.info.DBName, w.meta_tbl, catalog.SystemSI_IVFFLAT_TblCol_Metadata_key)

	sql += fmt.Sprintf("WITH centroid as (SELECT * FROM `%s`.`%s` WHERE `%s` = (%s) ), ", w.info.DBName, w.centroids_tbl, catalog.SystemSI_IVFFLAT_TblCol_Centroids_version, versql)
	sql += fmt.Sprintf("src as (SELECT %s FROM (VALUES %s)) ", cols, string(w.vbuf))
	sql += fmt.Sprintf("SELECT `%s`, `%s`, %s FROM src CENTROIDX('%s') JOIN centroid using (`%s`, `%s`)",
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		cnames_str,
		w.ivfparam.OpType,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		cnames[1])

	return []byte(sql), nil
}
