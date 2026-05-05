//go:build gpu

// Copyright 2022 Matrix Origin
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

package ivfpq

// IvfpqSync mirrors cagra.CagraSync — see pkg/vectorindex/cagra/sync.go for
// the architectural commentary (including why CDC writes target the fixed
// vectorindex.CdcTailId sentinel rather than a per-sub-index id, and why
// the sync is stateless across flushes).

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runTxn = sqlexec.RunTxn

type IvfpqSync struct {
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	idxname string

	activeIndexId string

	dim                int
	includeBytesPerRow int
	colMetaJSON        string

	pendingRecords []byte
	pendingSizes   []int

	ninsert atomic.Int32
	ndelete atomic.Int32
	nupdate atomic.Int32
}

func NewIvfpqSync(
	sqlproc *sqlexec.SqlProcess,
	db string,
	tbl string,
	idxname string,
	idxdefs []*plan.IndexDef,
	dimension int32,
	colMetaJSON string,
) (*IvfpqSync, error) {
	if dimension <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("IvfpqSync: invalid dimension")
	}

	var idxtblcfg vectorindex.IndexTableConfig
	idxtblcfg.DbName = db
	idxtblcfg.SrcTable = tbl

	for _, idxdef := range idxdefs {
		switch idxdef.IndexAlgoTableType {
		case catalog.Ivfpq_TblType_Metadata:
			idxtblcfg.MetadataTable = idxdef.IndexTableName
		case catalog.Ivfpq_TblType_Storage:
			idxtblcfg.IndexTable = idxdef.IndexTableName
		}
	}
	if idxtblcfg.MetadataTable == "" || idxtblcfg.IndexTable == "" {
		return nil, moerr.NewInternalErrorNoCtx("IvfpqSync: missing metadata or storage table in idxdefs")
	}

	var idxcfg vectorindex.IndexConfig
	idxcfg.Type = vectorindex.IVFPQ
	idxcfg.CuvsIvfpq.Dimensions = uint(dimension)

	includeBytesPerRow, err := vectorindex.CdcIncludeBytesPerRow(colMetaJSON)
	if err != nil {
		return nil, err
	}

	s := &IvfpqSync{
		idxcfg:             idxcfg,
		tblcfg:             idxtblcfg,
		idxname:            idxname,
		dim:                int(dimension),
		includeBytesPerRow: includeBytesPerRow,
		colMetaJSON:        colMetaJSON,
		activeIndexId:      vectorindex.CdcTailId,
	}
	return s, nil
}

func (s *IvfpqSync) RunOnce(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[float32]) (err error) {
	defer s.Destroy()
	if err = s.Update(sqlproc, cdc); err != nil {
		return err
	}
	return s.Save(sqlproc)
}

func (s *IvfpqSync) Destroy() {
	s.pendingRecords = nil
	s.pendingSizes = nil
}

func (s *IvfpqSync) Update(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[float32]) error {
	start := time.Now()

	var ninsert, nupdate, ndelete int32
	for _, e := range cdc.Data {
		switch e.Type {
		case vectorindex.CDC_DELETE:
			if err := s.appendRecord(vectorindex.CdcOpDelete, e.PKey, nil, nil); err != nil {
				return err
			}
			ndelete++
		case vectorindex.CDC_INSERT:
			if err := s.appendRecord(vectorindex.CdcOpInsert, e.PKey, e.Vec, e.IncludeBytes); err != nil {
				return err
			}
			ninsert++
		case vectorindex.CDC_UPSERT:
			if err := s.appendRecord(vectorindex.CdcOpDelete, e.PKey, nil, nil); err != nil {
				return err
			}
			if err := s.appendRecord(vectorindex.CdcOpInsert, e.PKey, e.Vec, e.IncludeBytes); err != nil {
				return err
			}
			nupdate++
		default:
			return moerr.NewInternalErrorNoCtx("IvfpqSync: unknown CDC event type " + e.Type)
		}
	}

	s.ninsert.Store(ninsert)
	s.nupdate.Store(nupdate)
	s.ndelete.Store(ndelete)
	logutil.Infof("IVFPQ cdc[%p]: db=%s table=%s index=%s len=%d ins=%d del=%d upd=%d elapsed=%dms",
		s, s.tblcfg.DbName, s.tblcfg.SrcTable, s.idxname,
		len(cdc.Data), ninsert, ndelete, nupdate, time.Since(start).Milliseconds())
	return nil
}

func (s *IvfpqSync) appendRecord(op vectorindex.CdcOp, pkid int64, vec []float32, include []byte) error {
	if op == vectorindex.CdcOpInsert {
		if len(vec) != s.dim {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"IvfpqSync.appendRecord: vec length %d != dim %d", len(vec), s.dim))
		}
		if s.includeBytesPerRow > 0 && len(include) != s.includeBytesPerRow {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"IvfpqSync.appendRecord: include bytes length %d != includeBytesPerRow %d",
				len(include), s.includeBytesPerRow))
		}
		if s.includeBytesPerRow == 0 && len(include) != 0 {
			return moerr.NewInternalErrorNoCtx(
				"IvfpqSync.appendRecord: include bytes supplied but index has no INCLUDE columns")
		}
	}
	before := len(s.pendingRecords)
	out, err := vectorindex.EncodeEventRecord(s.pendingRecords, op, pkid, vec, include, s.dim, s.includeBytesPerRow)
	if err != nil {
		return err
	}
	s.pendingRecords = out
	s.pendingSizes = append(s.pendingSizes, len(s.pendingRecords)-before)
	return nil
}

func (s *IvfpqSync) Save(sqlproc *sqlexec.SqlProcess) error {
	if len(s.pendingSizes) == 0 {
		return nil
	}
	nextId, err := s.nextChunkId(sqlproc, vectorindex.Tag_CdcEvents)
	if err != nil {
		return err
	}
	sqls := vectorindex.CdcAppendEventsSql(s.tblcfg, s.activeIndexId, nextId, s.pendingRecords, s.pendingSizes)
	if len(sqls) == 0 {
		return nil
	}
	if err = s.runSqls(sqlproc, sqls); err != nil {
		return err
	}
	s.pendingRecords = s.pendingRecords[:0]
	s.pendingSizes = s.pendingSizes[:0]
	veccache.Cache.Remove(s.tblcfg.IndexTable)
	return nil
}

func (s *IvfpqSync) nextChunkId(sqlproc *sqlexec.SqlProcess, tag vectorindex.ChunkTag) (int64, error) {
	sql := vectorindex.NextChunkIdSql(s.tblcfg, s.activeIndexId, tag)
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat.RowCount() == 0 {
			continue
		}
		return vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0), nil
	}
	return 0, nil
}

func (s *IvfpqSync) runSqls(sqlproc *sqlexec.SqlProcess, sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}
	opts := executor.Options{}
	return runTxn(sqlproc, func(exec executor.TxnExecutor) error {
		for _, sql := range sqls {
			res, err := exec.Exec(sql, opts.StatementOption())
			if err != nil {
				return err
			}
			res.Close()
		}
		return nil
	})
}
