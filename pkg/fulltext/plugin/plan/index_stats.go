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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/indexstats/sqlreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var _ planplugin.IndexStatsHooks = Hooks{}

func (Hooks) ConfigureIndexStats(ctx context.Context, source *plan.TableDef, defs []*plan.IndexDef, cfg *indexstats.Config) error {
	if source == nil || source.Pkey == nil {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete fulltext index definition")
	}
	for _, idx := range defs {
		if idx.IndexAlgoTableType == catalog.IndexIgnored_TblType {
			continue
		}
		// Pre-split catalogs used the classic algorithm token for v2. Its
		// owner recognizes that legacy spelling and delegates to the v2 owner.
		if catalog.IndexAlgoParamVersionOf(idx.IndexAlgoParams) == 2 ||
			idx.IndexAlgoTableType == catalog.FullText2Index_TblType_Metadata ||
			idx.IndexAlgoTableType == catalog.FullText2Index_TblType_Storage {
			if p, ok := indexplugin.Get(catalog.MoIndexFullText2Algo.ToString()); ok {
				if hooks, ok := p.Plan().(planplugin.IndexStatsHooks); ok {
					return hooks.ConfigureIndexStats(ctx, source, defs, cfg)
				}
			}
			return moerr.NewNotSupported(ctx, "mo_index_stats: fulltext v2 diagnostics are not supported")
		}
	}
	for _, idx := range defs {
		if idx.IndexTableName != "" {
			cfg.Tables = append(cfg.Tables, indexstats.IndexTable{Kind: idx.IndexAlgoTableType, Name: idx.IndexTableName})
		}
		if idx.IndexAlgoTableType == catalog.IndexIgnored_TblType {
			continue
		}

		options, parser, async, err := fulltext.ParseIndexOptions(idx.IndexAlgoParams)
		if err != nil {
			return err
		}
		cfg.Options, cfg.Parser = options, parser
		cfg.Columns = append([]string(nil), idx.Parts...)
		cfg.FullTextVersion = catalog.IndexAlgoParamVersionOf(idx.IndexAlgoParams)
		cfg.Async = async

		if idx.IndexAlgoTableType == "" || idx.IndexAlgoTableType == catalog.FullTextIndex_TblType {
			cfg.Entries = idx.IndexTableName
		}
	}
	if cfg.Entries == "" || len(cfg.Columns) == 0 {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete fulltext index definition")
	}
	return nil
}

func (Hooks) ReadIndexStats(sp *sqlexec.SqlProcess, cfg indexstats.Config) (indexstats.Rows, error) {
	return sqlreader.ReadFullText(sp, cfg)
}
