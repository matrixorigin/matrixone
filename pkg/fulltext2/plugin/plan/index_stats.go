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
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
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
	cfg.FullTextVersion, cfg.Async = 2, true
	cfg.Algorithm = catalog.MoIndexFullText2Algo.ToString()
	for _, idx := range defs {
		if idx.IndexTableName != "" {
			cfg.Tables = append(cfg.Tables, indexstats.IndexTable{Kind: idx.IndexAlgoTableType, Name: idx.IndexTableName})
		}
		if idx.IndexAlgoTableType == catalog.IndexIgnored_TblType {
			continue
		}

		options, parser, _, err := fulltext.ParseIndexOptions(idx.IndexAlgoParams)
		if err != nil {
			return err
		}
		cfg.Options, cfg.Parser = options, parser
		cfg.TokenFilters = fulltext2.ParseTokenFilterNames(options[catalog.IndexAlgoParamTokenFilter])
		cfg.Columns = append([]string(nil), idx.Parts...)

		switch idx.IndexAlgoTableType {
		case catalog.FullText2Index_TblType_Metadata:
			cfg.Metadata = idx.IndexTableName
		case "", catalog.FullTextIndex_TblType, catalog.FullText2Index_TblType_Storage:
			cfg.Entries = idx.IndexTableName
		}
	}
	if cfg.Metadata == "" || cfg.Entries == "" || len(cfg.Columns) == 0 {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete fulltext index definition")
	}
	return nil
}

func (Hooks) ReadIndexStats(sp *sqlexec.SqlProcess, cfg indexstats.Config) (indexstats.Rows, error) {
	return sqlreader.ReadFullText2(sp, cfg)
}
