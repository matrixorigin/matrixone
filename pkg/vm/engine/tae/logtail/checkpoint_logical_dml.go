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

package logtail

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// V14's metadata object keeps its range index in SchemaData and stores logical
// bounds separately in its first checkpoint sub-schema. Neither is user data.
func newCheckpointLogicalDMLBatch() *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Attrs = []string{"database_id", "table_id", "logical_dml_bound"}
	bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	return bat
}

func appendCheckpointLogicalDML(bat *batch.Batch, database, table uint64, bound types.TS, mp *mpool.MPool) error {
	if err := vector.AppendFixed(bat.Vecs[0], database, false, mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(bat.Vecs[1], table, false, mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(bat.Vecs[2], bound, false, mp); err != nil {
		return err
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func (data *CheckpointData_V2) FormatVersion() uint32 {
	if data.logicalDML != nil {
		return CheckpointVersion14
	}
	return CheckpointCurrentVersion
}

// Legacy checkpoints do not prove a known-empty history. Their caller installs
// a conservative recovery floor instead; V14 must contain its complete block.
func ReadCheckpointLogicalDML(ctx context.Context, version uint32, location objectio.Location, fs fileservice.FileService, mp *mpool.MPool) (result *batch.Batch, err error) {
	if version < CheckpointVersion14 {
		return nil, nil
	}
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, err
	}
	logical, ok := meta.SubMeta(0)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "logical DML checkpoint metadata missing")
	}
	blocks := logical.BlockCount()
	if blocks == 0 || uint32(logical.BlockHeader().StartID())+blocks > math.MaxUint16 || logical.BlockHeader().ColumnCount() != 3 {
		return nil, moerr.NewInternalError(ctx, "invalid logical DML checkpoint block count")
	}
	result = newCheckpointLogicalDMLBatch()
	defer func() {
		if err != nil {
			result.Clean(mp)
			result = nil
		}
	}()
	seen := make(map[uint64]struct{})
	var coverage types.TS
	for block := uint32(0); block < blocks; block++ {
		var loaded *batch.Batch
		loaded, err = objectio.ReadOneBlockAllColumns(ctx, &logical, location.Name().String(),
			uint32(logical.BlockHeader().StartID())+block, []uint16{0, 1, 2}, fileservice.SkipAllCache, fs)
		if err != nil {
			return
		}
		err = func() error {
			defer loaded.Clean(mp)
			if len(loaded.Vecs) != 3 || loaded.RowCount() == 0 {
				return moerr.NewInternalError(ctx, "invalid logical DML checkpoint shape")
			}
			for i, typ := range []types.T{types.T_uint64, types.T_uint64, types.T_TS} {
				if loaded.Vecs[i] == nil || loaded.Vecs[i].GetType().Oid != typ || loaded.Vecs[i].Length() != loaded.RowCount() || loaded.Vecs[i].HasNull() {
					return moerr.NewInternalError(ctx, "invalid logical DML checkpoint column")
				}
			}
			dbs := vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[0])
			tables := vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[1])
			bounds := vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2])
			for i, table := range tables {
				if result.RowCount() == 0 {
					if dbs[i] != 0 || table != 0 || bounds[i].IsEmpty() {
						return moerr.NewInternalError(ctx, "invalid logical DML checkpoint coverage")
					}
					coverage = bounds[i]
				} else {
					if dbs[i] == 0 || table == 0 || bounds[i].GT(&coverage) {
						return moerr.NewInternalError(ctx, "invalid logical DML checkpoint identity or bound")
					}
					if _, duplicate := seen[table]; duplicate {
						return moerr.NewInternalError(ctx, "duplicate logical DML checkpoint table")
					}
					seen[table] = struct{}{}
				}
				if err := appendCheckpointLogicalDML(result, dbs[i], table, bounds[i], mp); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return
		}
	}
	return
}

func capCheckpointLogicalDML(bat *batch.Batch, end types.TS) {
	bounds := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
	for i := range bounds {
		if bounds[i].GT(&end) {
			bounds[i] = end
		}
	}
}

// LogicalDMLCheckpoint identifies one input to checkpoint compaction. It carries
// no catalog pointers: reconstruction and table identity remain ID-based.
type LogicalDMLCheckpoint struct {
	Version  uint32
	Location objectio.Location
	End      types.TS
}

// MergeLogicalDML preserves bounds when GC compacts checkpoint indexes. Old
// unknown history contributes a conservative floor; it never becomes known-zero.
func (data *CheckpointData_V2) MergeLogicalDML(ctx context.Context, inputs []LogicalDMLCheckpoint, end types.TS, fs fileservice.FileService) (err error) {
	if data.logicalDML != nil || end.IsEmpty() {
		return moerr.NewInternalError(ctx, "invalid logical DML checkpoint merge owner or coverage")
	}
	type bound struct {
		database  uint64
		timestamp types.TS
	}
	bounds := make(map[uint64]bound)
	var legacyFloor types.TS
	known := false
	for _, input := range inputs {
		if input.End.IsEmpty() || input.End.GT(&end) {
			return moerr.NewInternalError(ctx, "logical DML checkpoint input is outside merge coverage")
		}
		if input.Version < CheckpointVersion14 {
			if input.End.GT(&legacyFloor) {
				legacyFloor = input.End
			}
			continue
		}
		bat, loadErr := ReadCheckpointLogicalDML(ctx, input.Version, input.Location, fs, data.allocator)
		if loadErr != nil {
			return loadErr
		}
		loadErr = func() error {
			defer bat.Clean(data.allocator)
			dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
			tables := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[1])
			values := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			if !values[0].Equal(&input.End) {
				return moerr.NewInternalError(ctx, "logical DML checkpoint input coverage mismatch")
			}
			for i := 1; i < bat.RowCount(); i++ {
				old, exists := bounds[tables[i]]
				if exists && old.database != dbs[i] {
					return moerr.NewInternalError(ctx, "logical DML checkpoint table changed database identity")
				}
				if !exists || values[i].GT(&old.timestamp) {
					bounds[tables[i]] = bound{dbs[i], values[i]}
				}
			}
			return nil
		}()
		if loadErr != nil {
			return loadErr
		}
		known = true
	}
	if !known {
		return nil
	}
	result := newCheckpointLogicalDMLBatch()
	defer func() {
		if err != nil {
			result.Clean(data.allocator)
		}
	}()
	if err = appendCheckpointLogicalDML(result, 0, 0, end, data.allocator); err != nil {
		return err
	}
	for table, value := range bounds {
		if value.timestamp.LT(&legacyFloor) {
			value.timestamp = legacyFloor
		}
		if err = appendCheckpointLogicalDML(result, value.database, table, value.timestamp, data.allocator); err != nil {
			return err
		}
	}
	data.logicalDML = result
	return nil
}
