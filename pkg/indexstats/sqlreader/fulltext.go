// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlreader

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ReadFullText2(sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (indexstats.Rows, error) {
	s := &metadataState{config: cfg}
	proc := sqlproc.Proc

	summary, err := fulltext2.ReadStats(proc.Ctx, sqlproc, fulltext2.TableConfig{
		DbName: s.config.Database, SrcTable: s.config.Table, PKey: s.config.PrimaryKey, IndexTable: s.config.Entries,
		MetadataTable: s.config.Metadata, Parser: s.config.Parser, TokenFilters: s.config.TokenFilters,
	})
	if err != nil {
		return indexstats.Rows{}, err
	}
	return s.finishFullText(proc, sqlproc, summary)
}

func ReadFullText(sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (indexstats.Rows, error) {
	s := &metadataState{config: cfg}
	var summary indexstats.FullTextSummary

	table := sqlquote.QualifiedIdent(s.config.Database, s.config.Entries)
	query := fmt.Sprintf("SELECT COUNT(DISTINCT doc_id) FROM %s WHERE word = %s", table, sqlquote.String(fulltext.DOC_LEN_WORD))
	res, err := indexStatsRunSQL(sqlproc, query)
	if err != nil {
		return indexstats.Rows{}, err
	}
	for _, bat := range res.Batches {
		if bat.RowCount() > 0 {
			summary.Documents = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
		}
	}
	res.Close()
	query = fmt.Sprintf("SELECT COUNT(*), COALESCE(MIN(df), 0), COALESCE(MAX(df), 0), "+
		"CAST(COALESCE(AVG(CAST(df AS DOUBLE)), 0) AS DOUBLE) FROM "+
		"(SELECT COUNT(DISTINCT doc_id) AS df FROM %s WHERE word <> %s GROUP BY word) AS terms",
		table, sqlquote.String(fulltext.DOC_LEN_WORD))
	res, err = indexStatsRunSQL(sqlproc, query)
	if err != nil {
		return indexstats.Rows{}, err
	}
	for _, bat := range res.Batches {
		if bat.RowCount() > 0 {
			summary.Terms = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
			summary.MinDF = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
			summary.MaxDF = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[2], 0)
			summary.MeanDF = vector.GetFixedAtNoTypeCheck[float64](bat.Vecs[3], 0)
		}
	}
	res.Close()

	return s.finishFullText(sqlproc.Proc, sqlproc, summary)
}

func (s *metadataState) finishFullText(proc *process.Process, sqlproc *sqlexec.SqlProcess, summary indexstats.FullTextSummary) (indexstats.Rows, error) {

	if summary.Documents < 0 || summary.Terms < 0 || summary.MinDF < 0 || summary.MaxDF > summary.Documents {
		return indexstats.Rows{}, moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: inconsistent fulltext document frequencies")
	}

	if err := s.readMetadata(proc, sqlproc); err != nil {
		return indexstats.Rows{}, err
	}
	if err := s.readFulltextHealth(proc, sqlproc); err != nil {
		return indexstats.Rows{}, err
	}
	return indexstats.SummaryRow("fulltext", s.fullTextMetrics(summary)), nil
}

func (s *metadataState) fullTextMetrics(summary indexstats.FullTextSummary) map[string]any {
	metrics := map[string]any{
		"version": s.config.FullTextVersion, "columns": s.config.Columns, "parser": s.config.Parser,
		"maintenance_mode": "sync", "document_count": summary.Documents, "term_count": summary.Terms,
		"min_document_frequency": summary.MinDF, "max_document_frequency": summary.MaxDF,
		"avg_document_frequency": summary.MeanDF, "storage": s.storage,
	}
	if s.config.Async {
		metrics["maintenance_mode"] = "async"
		metrics["async"] = s.maintenance
	}
	return s.attachHealth(metrics)
}

func (s *metadataState) readFulltextHealth(proc *process.Process, sp *sqlexec.SqlProcess) error {

	if p, ok := indexplugin.Get(s.config.Algorithm); ok {
		if owner, ok := p.Idxcron().(idxcronplugin.HealthInspector); ok {
			var err error
			s.health, s.suggestions, err = owner.IndexHealth(sp, s.config)
			if err != nil {
				if proc.Ctx.Err() != nil {
					return proc.Ctx.Err()
				}
				s.health = map[string]any{"health_status": "maintenance_measurement_unavailable"}
			}
		}
	}
	return nil
}
