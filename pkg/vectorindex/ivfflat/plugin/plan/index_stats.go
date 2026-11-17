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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ planplugin.IndexStatsHooks = Hooks{}

func (Hooks) ConfigureIndexStats(ctx context.Context, source *plan.TableDef, defs []*plan.IndexDef, cfg *indexstats.Config) error {
	if source == nil || source.Pkey == nil {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete IVF_FLAT index definition")
	}
	cfg.SupportsAnalysis = true
	cfg.Lists = 1
	for _, idx := range defs {
		if idx.IndexTableName != "" {
			cfg.Tables = append(cfg.Tables, indexstats.IndexTable{Kind: idx.IndexAlgoTableType, Name: idx.IndexTableName})
		}
		switch idx.IndexAlgoTableType {
		case catalog.SystemSI_IVFFLAT_TblType_Metadata:
			cfg.Metadata = idx.IndexTableName
			if len(idx.Parts) != 1 {
				return moerr.NewInvalidInput(ctx, "mo_index_stats: invalid IVF_FLAT index column")
			}
			cfg.Column = idx.Parts[0]
			params, err := catalog.IndexParamsStringToMap(idx.IndexAlgoParams)
			if err != nil {
				return err
			}
			cfg.Options = params
			cfg.OpType = params[catalog.IndexAlgoParamOpType]
			if cfg.OpType == "" {
				cfg.OpType = "vector_l2_ops"
			}
			cfg.Quantization = params[catalog.Quantization]
			if cfg.Quantization == "" {
				cfg.Quantization = "none"
			}
			if value, ok := params[catalog.IndexAlgoParamLists]; ok {
				cfg.Lists, err = strconv.ParseInt(value, 10, 64)
				if err != nil || cfg.Lists <= 0 {
					return moerr.NewInvalidInput(ctx, "mo_index_stats: invalid IVF_FLAT lists option")
				}
			}
			cfg.Async, err = catalog.IndexParamAsync(idx.IndexAlgoParams)
			if err != nil {
				return err
			}
		case catalog.SystemSI_IVFFLAT_TblType_Centroids:
			cfg.Centroids = idx.IndexTableName
		case catalog.SystemSI_IVFFLAT_TblType_Entries:
			cfg.Entries = idx.IndexTableName
		}
	}
	if cfg.Metadata == "" || cfg.Centroids == "" || cfg.Entries == "" {
		return moerr.NewInvalidInput(ctx, "mo_index_stats: incomplete IVF_FLAT index definition")
	}
	return nil
}
