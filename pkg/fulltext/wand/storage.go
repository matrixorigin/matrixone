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

package wand

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// TableConfig is the JSON config passed to the fulltext_wand_create /
// fulltext_wand_search TVFs (const string arg 0). It locates the persistent
// WAND chunk store + metadata table for an index, mirroring
// vectorindex.IndexTableConfig.
type TableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	IndexTable    string `json:"index"`    // chunk store (FullTextIndex_TblType_Storage)
	MetadataTable string `json:"metadata"` // metadata (FullTextIndex_TblType_Metadata)
	PKey          string `json:"pkey"`
}

// insertBatchRows caps how many chunk tuples go into one INSERT statement.
const insertBatchRows = 256

// ToInsertSqls serializes the model and emits the SQL to persist it: one
// metadata row (timestamp, md5 checksum, filesize) plus the index bytes split
// into <= MaxChunkSize chunks stored as (index_id, chunk_id, data, tag) rows.
// Chunk data is embedded as unhex(...) literals so the create TVF needs no
// temp files.
func (m *WandModel) ToInsertSqls(cfg TableConfig, ts int64) ([]string, error) {
	buf, err := m.Serialize()
	if err != nil {
		return nil, err
	}
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	sqls := make([]string, 0, 4)

	metaTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)
	sqls = append(sqls, fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (%s, %d, %s, %d)",
		metaTbl,
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, catalog.FullTextIndex_TblCol_Metadata_Timestamp,
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.String(m.Id), ts, sqlquote.String(checksum), filesize))

	idxTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable)
	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES ", idxTbl,
		catalog.FullTextIndex_TblCol_Storage_Index_Id, catalog.FullTextIndex_TblCol_Storage_Chunk_Id,
		catalog.FullTextIndex_TblCol_Storage_Data, catalog.FullTextIndex_TblCol_Storage_Tag)

	values := make([]string, 0, insertBatchRows)
	chunkID := int64(0)
	for offset := 0; offset < len(buf); offset += vectorindex.MaxChunkSize {
		end := offset + vectorindex.MaxChunkSize
		if end > len(buf) {
			end = len(buf)
		}
		values = append(values, fmt.Sprintf("(%s, %d, unhex('%s'), 0)",
			sqlquote.String(m.Id), chunkID, hex.EncodeToString(buf[offset:end])))
		chunkID++
		if len(values) == insertBatchRows {
			sqls = append(sqls, insertPrefix+strings.Join(values, ", "))
			values = values[:0]
		}
	}
	if len(values) > 0 {
		sqls = append(sqls, insertPrefix+strings.Join(values, ", "))
	}
	return sqls, nil
}

// DeleteSqls returns the SQL to remove an index id's chunks + metadata row
// (used before a rebuild so reindex is idempotent).
func DeleteSqls(cfg TableConfig, id string) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(id)),
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
			catalog.FullTextIndex_TblCol_Metadata_Index_Id, sqlquote.String(id)),
	}
}

// LoadFromStorage reads an index's metadata + chunks back from the WAND store,
// verifies the checksum, and deserializes it into a model. Chunks are
// reassembled by chunk_id offset (no ORDER BY needed). The full serialized
// blob is held in RAM during load; mmap-backed minimization is Phase B.
func LoadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*WandModel, error) {
	// metadata: checksum + filesize
	metaSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, sqlquote.String(id))
	mres, err := sqlexec.RunSql(sqlproc, metaSQL)
	if err != nil {
		return nil, err
	}
	var checksum string
	var filesize int64
	found := false
	for _, bat := range mres.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		checksum = bat.Vecs[0].GetStringAt(0)
		filesize = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
		found = true
		break
	}
	mres.Close()
	if !found {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("wand index %s metadata not found", id))
	}
	if filesize <= 0 {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("wand index %s has empty filesize", id))
	}

	buf := make([]byte, filesize)
	chunkSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(id))
	cres, err := sqlexec.RunSql(sqlproc, chunkSQL)
	if err != nil {
		return nil, err
	}
	var written int64
	for _, bat := range cres.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		chunkIDs := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
		for i, cid := range chunkIDs {
			data := bat.Vecs[1].GetRawBytesAt(i)
			offset := cid * int64(vectorindex.MaxChunkSize)
			if offset < 0 || offset+int64(len(data)) > filesize {
				cres.Close()
				return nil, moerr.NewInternalError(sqlproc.GetContext(),
					fmt.Sprintf("wand index %s chunk %d out of range", id, cid))
			}
			copy(buf[offset:], data)
			written += int64(len(data))
		}
	}
	cres.Close()
	if written != filesize {
		return nil, moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand index %s incomplete: wrote %d of %d bytes", id, written, filesize))
	}

	if got := vectorindex.CheckSumFromBuffer(buf); got != checksum {
		return nil, moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand index %s checksum mismatch", id))
	}
	return Deserialize(id, buf)
}
