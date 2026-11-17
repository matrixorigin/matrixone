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

package table_function

import (
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The centroid owner publishes its actual training baseline in the same build
// transaction. Query diagnostics never write this record.
func writeIVFBuildInfo(proc *process.Process, sqlproc *sqlexec.SqlProcess, cfg vectorindex.IndexTableConfig, version int64, samples, lists int) error {
	txn := proc.GetTxnOperator()
	if txn == nil || samples < lists || samples == 0 {
		return nil
	}
	ts := txn.Txn().SnapshotTS
	info := diagnostic.BuildInfo{Format: 1, IndexVersion: version, BuildTS: fmt.Sprintf("%d-%d", ts.PhysicalTime, ts.LogicalTime), Rows: cfg.DataSize, TrainingSamples: int64(samples), SourceTableID: cfg.SourceTableID}
	ctx, db, err := indexStatsDatabase(proc, sqlproc, indexstats.Config{Database: cfg.DbName, Table: cfg.SrcTable})
	if err == nil {
		if rel, err := db.Relation(ctx, cfg.SrcTable, nil); err == nil {
			info.SourceTableID = rel.GetTableID(ctx)
			if provider, ok := rel.(engine.SourceCommitTSProvider); ok {
				_, err := provider.SourceCommitTS(ctx, types.TS{})
				info.Historical = err == nil
			}
		}
	}
	encoded, err := json.Marshal(info)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (%s,%s) VALUES (%s,%s) ON DUPLICATE KEY UPDATE %s=VALUES(%s)",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable), sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_key), sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val),
		sqlquote.String(diagnostic.BuildMetadataKey), sqlquote.String(string(encoded)), sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val), sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val))
	result, err := ivf_runSql(sqlproc, query)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}
