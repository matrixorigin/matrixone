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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCheckpointLogicalDMLRoundTrip(t *testing.T) {
	for _, tables := range []int{-1, 0, 2, DefaultCheckpointBlockRows + 1} {
		t.Run(fmt.Sprint(tables), func(t *testing.T) {
			ctx := context.Background()
			fs, _, mp := makeCheckpointMetaFixture(t, nil)
			data := NewCheckpointData_V2(mp, 0, fs)
			defer data.Close()
			if tables >= 0 {
				data.logicalDML = newCheckpointLogicalDMLBatch()
				require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 0, 0, types.BuildTS(500, 0), mp))
				for i := 0; i < tables; i++ {
					bound := types.TS{}
					if i%2 == 0 {
						bound = types.BuildTS(300, 0)
					}
					require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 1, uint64(i+1), bound, mp))
				}
			}
			loc, _, err := data.Sync(ctx, fs)
			require.NoError(t, err)
			if tables < 0 {
				require.Equal(t, uint32(13), data.FormatVersion())
				legacy, err := ReadCheckpointLogicalDML(ctx, 13, loc, fs, mp)
				require.NoError(t, err)
				require.Nil(t, legacy)
				missing, err := ReadCheckpointLogicalDML(ctx, 14, loc, fs, mp)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
				require.ErrorContains(t, err, "logical DML checkpoint metadata missing")
				require.Nil(t, missing)
				return
			}
			require.Equal(t, uint32(14), data.FormatVersion())
			loaded, err := ReadCheckpointLogicalDML(ctx, 14, loc, fs, mp)
			require.NoError(t, err)
			defer loaded.Clean(mp)
			require.Equal(t, tables+1, loaded.RowCount())
			require.Equal(t, vector.MustFixedColNoTypeCheck[uint64](data.logicalDML.Vecs[0]), vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[0]))
			require.Equal(t, vector.MustFixedColNoTypeCheck[uint64](data.logicalDML.Vecs[1]), vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[1]))
			require.Equal(t, vector.MustFixedColNoTypeCheck[types.TS](data.logicalDML.Vecs[2]), vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2]))
			// The backup rewrite uses the same metadata block, with a bound
			// capped at the restored checkpoint's new coverage timestamp.
			capCheckpointLogicalDML(loaded, types.BuildTS(100, 0))
			copy := NewCheckpointData_V2(mp, 0, fs)
			defer copy.Close()
			copy.logicalDML = loaded
			defer func() { copy.logicalDML = nil }()
			rewritten, _, err := copy.Sync(ctx, fs)
			require.NoError(t, err)
			readBack, err := ReadCheckpointLogicalDML(ctx, copy.FormatVersion(), rewritten, fs, mp)
			require.NoError(t, err)
			defer readBack.Clean(mp)
			require.Equal(t, vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2]), vector.MustFixedColNoTypeCheck[types.TS](readBack.Vecs[2]))
		})
	}
}

func TestCheckpointLogicalDMLRejectsMalformedMetadata(t *testing.T) {
	for _, scenario := range []string{"missing_coverage", "duplicate", "future_bound", "null", "wrong_type"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			fs, _, mp := makeCheckpointMetaFixture(t, nil)
			data := NewCheckpointData_V2(mp, 0, fs)
			defer data.Close()
			data.logicalDML = newCheckpointLogicalDMLBatch()
			if scenario != "missing_coverage" {
				require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 0, 0, types.BuildTS(500, 0), mp))
			}
			require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 1, 10, types.BuildTS(400, 0), mp))
			switch scenario {
			case "duplicate":
				require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 1, 10, types.BuildTS(400, 0), mp))
			case "future_bound":
				require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 1, 11, types.BuildTS(600, 0), mp))
			case "null":
				data.logicalDML.Vecs[1].GetNulls().Add(1)
			case "wrong_type":
				data.logicalDML.Vecs[1].GetType().Oid = types.T_int64
			}
			loc, _, err := data.Sync(ctx, fs)
			require.NoError(t, err)
			loaded, err := ReadCheckpointLogicalDML(ctx, 14, loc, fs, mp)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
			require.Nil(t, loaded)
		})
	}
}

func TestCheckpointLogicalDMLMerge(t *testing.T) {
	ctx := context.Background()
	fs, _, mp := makeCheckpointMetaFixture(t, nil)
	makeInput := func(end, first int64, db uint64) LogicalDMLCheckpoint {
		data := NewCheckpointData_V2(mp, 0, fs)
		defer data.Close()
		data.logicalDML = newCheckpointLogicalDMLBatch()
		require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 0, 0, types.BuildTS(end, 0), mp))
		require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, db, 10, types.BuildTS(first, 0), mp))
		require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, db, 20, types.TS{}, mp))
		loc, _, err := data.Sync(ctx, fs)
		require.NoError(t, err)
		return LogicalDMLCheckpoint{14, loc, types.BuildTS(end, 0)}
	}
	first, second := makeInput(100, 80, 1), makeInput(200, 150, 1)
	for _, scenario := range []string{"known", "mixed_legacy", "legacy_only", "wrong_identity", "wrong_coverage"} {
		t.Run(scenario, func(t *testing.T) {
			data := NewCheckpointData_V2(mp, 0, fs)
			defer data.Close()
			inputs := []LogicalDMLCheckpoint{first, second}
			legacy := LogicalDMLCheckpoint{Version: 13, End: types.BuildTS(180, 0)}
			switch scenario {
			case "mixed_legacy":
				inputs = append(inputs, legacy)
			case "legacy_only":
				inputs = []LogicalDMLCheckpoint{legacy}
			case "wrong_identity":
				inputs = append(inputs, makeInput(200, 100, 2))
			case "wrong_coverage":
				inputs[1].End = types.BuildTS(190, 0)
			}
			err := data.MergeLogicalDML(ctx, inputs, types.BuildTS(200, 0), fs)
			if scenario == "wrong_identity" || scenario == "wrong_coverage" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
				require.Nil(t, data.logicalDML)
				return
			}
			require.NoError(t, err)
			if scenario == "legacy_only" {
				require.Equal(t, uint32(13), data.FormatVersion())
				return
			}
			loc, _, err := data.Sync(ctx, fs)
			require.NoError(t, err)
			loaded, err := ReadCheckpointLogicalDML(ctx, data.FormatVersion(), loc, fs, mp)
			require.NoError(t, err)
			defer loaded.Clean(mp)
			tables := vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[1])
			bounds := vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2])
			got := map[uint64]types.TS{}
			for i, table := range tables {
				got[table] = bounds[i]
			}
			if scenario == "mixed_legacy" {
				require.Equal(t, map[uint64]types.TS{0: types.BuildTS(200, 0), 10: types.BuildTS(180, 0), 20: types.BuildTS(180, 0)}, got)
			} else {
				require.Equal(t, map[uint64]types.TS{0: types.BuildTS(200, 0), 10: types.BuildTS(150, 0), 20: {}}, got)
			}
		})
	}
}

func TestCheckpointLogicalDMLBackupRewrite(t *testing.T) {
	ctx := context.Background()
	src, _, mp := makeCheckpointMetaFixture(t, nil)
	dst, _, _ := makeCheckpointMetaFixture(t, nil)
	data := NewCheckpointData_V2(mp, 0, src)
	defer data.Close()
	data.logicalDML = newCheckpointLogicalDMLBatch()
	require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 0, 0, types.BuildTS(500, 0), mp))
	require.NoError(t, appendCheckpointLogicalDML(data.logicalDML, 1, 10, types.BuildTS(300, 0), mp))
	loc, _, err := data.Sync(ctx, src)
	require.NoError(t, err)
	reader, err := GetCheckpointReader(ctx, "logical-backup", src, loc, data.FormatVersion())
	require.NoError(t, err)
	cutoff := types.BuildTS(200, 0)
	rewritten, _, files, err := ReWriteCheckpointAndBlockFromKey(ctx, "logical-backup", src, dst, loc, reader, data.FormatVersion(), cutoff)
	require.NoError(t, err)
	require.NotEmpty(t, files)
	loaded, err := ReadCheckpointLogicalDML(ctx, 14, rewritten, dst, mp)
	require.NoError(t, err)
	defer loaded.Clean(mp)
	require.Equal(t, []uint64{0, 10}, vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[1]))
	require.Equal(t, []types.TS{cutoff, cutoff}, vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2]))
}
