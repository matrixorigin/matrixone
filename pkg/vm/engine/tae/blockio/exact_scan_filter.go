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

package blockio

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func blockDataReadExactFilter(ctx context.Context, info *objectio.BlockInfo, ds engine.DataSource,
	columns []uint16, typs []types.Type, storageRows []int64, policy fileservice.Policy,
	output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService,
	filter *engine.ReaderFilterRequest, preFilterRows *int) error {
	if ds == nil {
		return moerr.NewInvalidInputNoCtx("nil source for exact residual read")
	}
	if err := validateLateMaterializationOutput(columns, typs, output); err != nil {
		return err
	}
	rows := storageRows
	if rows == nil {
		tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
		if err != nil {
			tombstones.Release()
			return err
		}
		defer tombstones.Release()
		if !tombstones.IsEmpty() {
			visible := vector.GetSels()
			defer func() { vector.PutSels(visible) }()
			for row := 0; row < int(info.MetaLocation().Rows()); row++ {
				if !tombstones.Contains(uint64(row)) {
					visible = append(visible, int64(row))
				}
			}
			rows = visible
		}
	}
	stats, err := ioutil.LoadColumnsDataByExactFilter(ctx, columns, typs, fs, info.MetaLocation(), output.Vecs[:len(columns)], rows, filter.Exact, mp, policy)
	if stats.Applied && filter.Observe != nil {
		filter.Observe(stats)
	}
	if preFilterRows != nil {
		*preFilterRows = stats.InputRows
	}
	if err != nil {
		return err
	}
	output.SetRowCount(stats.OutputRows)
	if !stats.Applied && !output.IsEmpty() {
		result, filterErr := filter.Apply(output, nil)
		if filterErr != nil {
			return filterErr
		}
		// Eager callbacks do not return a row mapping. They still cannot
		// expand the input or claim All after changing its row count.
		if output.RowCount() > stats.InputRows || (result.All && output.RowCount() != stats.InputRows) {
			return moerr.NewInvalidInputNoCtx("invalid exact-read generic fallback row count")
		}
	}
	for i := range columns {
		if output.Vecs[i].Length() != output.RowCount() {
			return moerr.NewInvalidInputNoCtx("exact read left inconsistent output rows")
		}
	}
	return nil
}
