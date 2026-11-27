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

package iscp

import (
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// An empty event frame retains the index's existing chunk-generation floor
// across a full rebuild, including a small or empty source with no model tar.
// It carries INCLUDE metadata but contributes no inserts, deletes or upserts.
type cuvsSnapshotMarker struct {
	cfg    vectorindex.IndexTableConfig
	header []byte
	floor  int64
}

func (c *IndexConsumer) prepareCuvsSnapshotMarker(proc *sqlexec.SqlProcess) (*cuvsSnapshotMarker, error) {
	w, ok := c.sqlWriter.(*CuvsCdcWriter)
	if !ok {
		return nil, moerr.NewInternalError(proc.GetContext(), "full-source initializer has no snapshot-generation owner")
	}
	cfg := vectorindex.IndexTableConfig{DbName: c.jobID.DBName, SrcTable: c.jobID.TableName}
	for _, idx := range w.indexdef {
		switch idx.IndexAlgoTableType {
		case catalog.Cagra_TblType_Storage, catalog.Ivfpq_TblType_Storage:
			cfg.IndexTable = idx.IndexTableName
		}
	}
	if cfg.IndexTable == "" {
		return nil, moerr.NewInternalError(proc.GetContext(), "cuVS snapshot storage table is missing")
	}
	marker := &cuvsSnapshotMarker{cfg: cfg, header: []byte(w.colMetaJSON)}
	floor, err := marker.nextChunk(proc)
	if err != nil {
		return nil, err
	}
	marker.floor = floor
	return marker, nil
}

func (m *cuvsSnapshotMarker) nextChunk(proc *sqlexec.SqlProcess) (int64, error) {
	res, err := ExecWithResult(proc.GetContext(), cuvscdc.NextChunkIdSql(m.cfg, vectorindex.CdcTailId, vectorindex.Tag_CdcEvents), proc.SqlCtx.GetService(), proc.SqlCtx.Txn())
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat != nil && bat.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
		}
	}
	return 0, nil
}

func (m *cuvsSnapshotMarker) persist(proc *sqlexec.SqlProcess) error {
	next, err := m.nextChunk(proc)
	if err != nil {
		return err
	}
	if next < m.floor {
		next = m.floor
	}
	frame := cuvscdc.FrameCdcChunk(nil, m.header, 0, 0, 0)
	sql := fmt.Sprintf("INSERT INTO %s VALUES (%s,%d,unhex(%s),%d)", sqlquote.QualifiedIdent(m.cfg.DbName, m.cfg.IndexTable), sqlquote.String(vectorindex.CdcTailId), next, sqlquote.String(hex.EncodeToString(frame)), vectorindex.Tag_CdcEvents)
	res, err := ExecWithResult(proc.GetContext(), sql, proc.SqlCtx.GetService(), proc.SqlCtx.Txn())
	res.Close()
	return err
}

func (c *IndexConsumer) jobAdvancedInTxn(proc *sqlexec.SqlProcess, prior uint64) (bool, error) {
	account, err := proc.GetAccountID()
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT job_id FROM mo_catalog.mo_iscp_log WHERE account_id = %d AND table_id = %d AND job_name = %s AND drop_at IS NULL", account, c.tableDef.TblId, sqlquote.String(c.jobID.JobName))
	ctx := defines.AttachAccountId(proc.GetContext(), catalog.System_Account)
	res, err := ExecWithResult(ctx, query, proc.SqlCtx.GetService(), proc.SqlCtx.Txn())
	if err != nil {
		return false, err
	}
	defer res.Close()
	advanced := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			advanced = vector.GetFixedAtNoTypeCheck[uint64](cols[0], i) > prior
		}
		return true
	})
	return advanced, nil
}
