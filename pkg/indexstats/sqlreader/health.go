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

package sqlreader

import (
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	iscpmeta "github.com/matrixorigin/matrixone/pkg/iscp/metadata"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	ivfflatrt "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *ivfStats) readHealth(proc *process.Process, sp *sqlexec.SqlProcess) error {
	if s.config.TableID == 0 {
		return nil
	}

	if !s.hasVersion {
		s.health = map[string]any{"baseline_status": "baseline_missing"}
		s.suggestions = make([]indexstats.Suggestion, 0)
		if s.config.Analyze != nil {
			s.health["analysis"] = map[string]any{"status": "index_not_initialized"}
		}
		return nil
	}
	s.health = map[string]any{}
	s.suggestions = make([]indexstats.Suggestion, 0)
	raw := ""
	result, err := indexStatsRunSQL(sp, fmt.Sprintf("SELECT %s FROM %s WHERE %s=%s", sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val), sqlquote.QualifiedIdent(s.config.Database, s.config.Metadata), sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_key), sqlquote.String(diagnostic.BuildMetadataKey)))
	if err != nil {
		return err
	}
	for _, bat := range result.Batches {
		if bat.RowCount() > 0 && !bat.Vecs[0].IsNull(0) {
			raw = bat.Vecs[0].GetStringAt(0)
			break
		}
	}
	base, status := diagnostic.DecodeBuild(raw, s.version)
	s.build = base
	result.Close()
	if status != "" {
		s.health["baseline_status"] = status
	}
	count, err := NonNullSourceCount(sp, s.config.Database, s.config.Table, s.config.Column)
	if err != nil {
		if proc.Ctx.Err() != nil {
			return proc.Ctx.Err()
		}
		s.health["health_status"] = "source_count_unavailable"
	} else {
		percent, err := indexplugin.AlgoParamFloat(s.config.Options[catalog.IndexAlgoParamKmeansTrainPercent], proc.GetResolveVariableFunc(), catalog.IndexAlgoParamKmeansTrainPercent, ivfflatrt.DefaultKmeansTrainPercent)
		if err != nil {
			return err
		}
		metrics, advice := diagnostic.BuildHealth(count, s.config.Lists, percent, s.build)
		for k, v := range metrics {
			s.health[k] = v
		}
		s.suggestions = advice
	}
	if s.build != nil {
		drift := map[string]any{}
		switch {
		case !s.build.Historical:
			drift["status"] = "build_snapshot_unavailable"
		case s.build.SourceTableID != s.config.TableID:
			drift["status"] = "source_identity_changed"
		case s.sample == nil:
			drift["status"] = "geometry_unavailable"
		case s.sample.Status != "":
			drift["status"] = s.sample.Status
		case len(s.sample.Samples) == 0:
			drift["status"] = "no_samples"
		default:
			old, err := readIndexOldKeys(proc, sp, s.config, s.build, sampleKeys(s.sample.Samples))
			if err != nil {
				if proc.Ctx.Err() != nil {
					return proc.Ctx.Err()
				}
				drift["status"] = "history_unavailable"
			} else {
				drift = diagnostic.Drift(s.sample.Samples, old, s.buckets, !strings.EqualFold(s.config.Quantization, "none") && s.config.Quantization != "")
			}
		}
		s.health["drift"] = drift
	}
	if s.config.Analyze != nil {
		metrics, advice, err := s.analyzeIndex(proc, sp)
		if err != nil {
			return err
		}
		s.health["analysis"] = metrics
		for _, item := range advice {
			if item.Action == "REINDEX" {
				s.suggestions = indexstats.AddReindex(s.suggestions, item.Reason, item.Params)
			} else {
				s.suggestions = append(s.suggestions, item)
			}
		}
	}
	return nil
}

// NonNullSourceCount counts indexed source values in the caller's transaction.
func NonNullSourceCount(sp *sqlexec.SqlProcess, database, table, column string) (int64, error) {
	result, err := indexStatsRunSQL(sp, "SELECT COUNT("+sqlquote.Ident(column)+") FROM "+sqlquote.QualifiedIdent(database, table))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	for _, bat := range result.Batches {
		if bat.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
		}
	}
	return 0, moerr.NewInternalError(sp.GetContext(), "missing source count")
}

func sampleKeys(samples []diagnostic.Sample) []string {
	keys := make([]string, 0, len(samples))
	for _, s := range samples {
		keys = append(keys, s.Key)
	}
	return keys
}

func indexSourceKey(alias, pk string) string {
	return "HEX(SERIAL(" + sqlquote.QualifiedIdent(alias, pk) + "))"
}

// Lookups retain bounded keys. Typed PK joins preserve SQL collation/enum and
// composite-key semantics; vectors are never carried by their build side.
func readIndexOldKeys(proc *process.Process, sp *sqlexec.SqlProcess, cfg indexstats.Config, base *diagnostic.BuildInfo, keys []string) (map[string]bool, error) {
	ts, ok := iscpmeta.ParseWatermark(base.BuildTS)
	if !ok || base.SourceTableID != cfg.TableID || !base.Historical {
		return nil, moerr.NewInvalidInput(proc.Ctx, "historical cohort is unavailable")
	}
	historic := *sp
	snapshot := ts.ToTimestamp()
	historic.SnapshotTS = &snapshot
	ctx, db, err := Database(proc, &historic, cfg)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(ctx, cfg.Table, nil)
	if err != nil {
		return nil, err
	}
	if rel.GetTableID(ctx) != base.SourceTableID {
		return nil, moerr.NewInvalidInput(proc.Ctx, "historical source identity changed")
	}
	found := map[string]bool{}
	err = runIndexKeyLookup(&historic, cfg, keys, func(selected string) string {
		return "SELECT " + indexSourceKey("sampled", cfg.PrimaryKey) + " FROM " + selected
	}, func(bat *batch.Batch) error {
		for row := 0; row < bat.RowCount(); row++ {
			found[strings.Clone(bat.Vecs[0].GetStringAt(row))] = true
		}
		return nil
	})
	return found, err
}

func runIndexKeyLookup(sp *sqlexec.SqlProcess, cfg indexstats.Config, keys []string, query func(string) string, consume func(*batch.Batch) error) error {
	unique := map[string]bool{}
	for _, key := range keys {
		unique[key] = true
	}
	ordered := make([]string, 0, len(unique))
	for key := range unique {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	pk := sqlquote.Ident(cfg.PrimaryKey)
	predicate := pk
	if cfg.SerializedKeyType == "" {
		// A plan produced before typed lookups contains only the encoded keys.
		predicate = "HEX(SERIAL(" + pk + "))"
	}
	source := sqlquote.QualifiedIdent(cfg.Database, cfg.Table)
	for start := 0; start < len(ordered); {
		end, bytes := start, 0
		parts := make([]string, 0, len(ordered)-start)
		for end < len(ordered) {
			literal := sqlquote.String(ordered[end])
			if cfg.SerializedKeyType != "" {
				literal = "SERIAL_EXTRACT(X" + literal + ", 0 AS " + cfg.SerializedKeyType + ")"
			}
			if len(literal) > 256<<10 {
				return moerr.NewInvalidInput(sp.GetContext(), "analysis key exceeds lookup budget")
			}
			if bytes+len(literal) > 256<<10 {
				break
			}
			parts = append(parts, literal)
			bytes += len(literal) + 1
			end++
		}
		if err := func() error {
			selected := fmt.Sprintf("(SELECT %s FROM %s WHERE %s IN (%s) LIMIT %d) AS sampled", pk, source, predicate, strings.Join(parts, ","), len(parts))
			result, err := indexStatsRunSQL(sp, query(selected))
			if err != nil {
				return err
			}
			defer result.Close()
			for _, bat := range result.Batches {
				if err := consume(bat); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			return err
		}
		start = end
	}
	return nil
}
