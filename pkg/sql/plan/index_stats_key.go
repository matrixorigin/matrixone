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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func configurePrimaryIndexStats(cfg *indexstats.Config, source *plan.TableDef) {
	cfg.Index, cfg.KeyKind, cfg.Method = "PRIMARY", "primary", "btree"
	cfg.Entries = cfg.Table
	cfg.Columns = append([]string(nil), source.Pkey.Names...)
	if len(cfg.Columns) == 0 {
		cfg.Columns = []string{source.Pkey.PkeyColName}
	}
	cfg.KeyColumns = append([]string(nil), cfg.Columns...)
	// Primary keys share source-table objects. Account only the declared key
	// columns' data volume, not every column in those shared objects.
	cfg.StorageColumns = append([]string(nil), cfg.Columns...)
	cfg.Tables = []indexstats.IndexTable{{Name: cfg.Table}}
}

func configureKeyIndexStats(ctx context.Context, cfg *indexstats.Config, source *plan.TableDef, idx *plan.IndexDef) error {
	if idx.IndexAlgoTableType != "" || idx.IndexTableName == "" {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete key index definition")
	}
	cfg.KeyKind, cfg.Method = "secondary", "btree"
	if idx.Unique {
		cfg.KeyKind = "unique"
	}
	cfg.Entries = idx.IndexTableName
	cfg.Tables = []indexstats.IndexTable{{Name: idx.IndexTableName}}
	var err error
	cfg.PrefixLengths, err = catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
	if err != nil {
		return err
	}
	if idx.IndexAlgoParams != "" {
		cfg.Options, err = catalog.IndexParamsStringToMap(idx.IndexAlgoParams)
		if err != nil {
			return err
		}
	}
	for _, name := range idx.Parts {
		if catalog.IsAlias(name) {
			continue // storage-only primary-key suffix is not a logical key part
		}
		cfg.KeyColumns = append(cfg.KeyColumns, name)
		display := name
		for _, col := range source.Cols {
			if col.Name == name && col.Hidden && col.GeneratedCol != nil {
				display = col.GeneratedCol.OriginString
				break
			}
		}
		cfg.Columns = append(cfg.Columns, display)
	}
	return nil
}
