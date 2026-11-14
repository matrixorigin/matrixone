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

package table_function

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *indexStatsState) startKeyIndex(proc *process.Process, sqlproc *sqlexec.SqlProcess) error {
	res, err := indexStatsRunSQL(sqlproc, "SELECT COUNT(*) FROM "+sqlquote.QualifiedIdent(s.config.Database, s.config.Entries))
	if err != nil {
		return err
	}
	summary := &indexstats.KeySummary{}
	for _, bat := range res.Batches {
		if bat.RowCount() > 0 {
			summary.Entries = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
		}
	}
	res.Close()
	if s.config.KeyKind == "primary" {
		// A declared primary key has exactly one distinct key per visible row.
		summary.Cardinality = summary.Entries
	} else {
		res, err = indexStatsRunSQL(sqlproc, buildKeyIndexCardinalitySQL(s.config))
		if err != nil {
			return err
		}
		var joined int64
		for _, bat := range res.Batches {
			if bat.RowCount() > 0 {
				summary.Cardinality = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
				joined = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
			}
		}
		res.Close()
		if joined != summary.Entries {
			return moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: key index entries do not match visible source rows")
		}
	}
	s.keySummary = summary
	return s.readMetadata(proc, sqlproc)
}

// Count logical keys among actual index entries. The join preserves the index's
// NULL inclusion policy. Grouping the typed key expressions applies SQL
// collation rules independently of the index's physical serialization and
// excludes the storage-only primary-key suffix from cardinality.
func buildKeyIndexCardinalitySQL(cfg indexstats.Config) string {
	parts := make([]string, len(cfg.KeyColumns))
	for i, name := range cfg.KeyColumns {
		parts[i] = sqlquote.QualifiedIdent("src", name)
		if length := cfg.PrefixLengths[name]; length > 0 {
			parts[i] = "SUBSTRING(" + parts[i] + ", 1, " + strconv.Itoa(length) + ")"
		}
	}
	return fmt.Sprintf("SELECT COUNT(*), CAST(COALESCE(SUM(n), 0) AS BIGINT) FROM "+
		"(SELECT COUNT(*) AS n FROM %s AS e JOIN %s AS src ON %s = %s GROUP BY %s) AS key_groups",
		sqlquote.QualifiedIdent(cfg.Database, cfg.Entries), sqlquote.QualifiedIdent(cfg.Database, cfg.Table),
		sqlquote.QualifiedIdent("e", catalog.IndexTablePrimaryColName), sqlquote.QualifiedIdent("src", cfg.PrimaryKey),
		strings.Join(parts, ", "))
}

func (s *indexStatsState) keyIndexMetrics() map[string]any {
	metrics := map[string]any{
		"columns": s.config.Columns, "method": s.config.Method,
		"entry_count": s.keySummary.Entries, "cardinality": s.keySummary.Cardinality, "storage": s.storage,
	}
	if len(s.config.PrefixLengths) > 0 {
		metrics["prefix_lengths"] = s.config.PrefixLengths
	}
	options := make(map[string]string)
	for name, value := range s.config.Options {
		if name != catalog.IndexAlgoParamPrefixLengths && name != catalog.IndexAlgoParamPrefixLengthsV2 {
			options[name] = value
		}
	}
	if len(options) > 0 {
		metrics["index_options"] = options
	}
	return metrics
}
