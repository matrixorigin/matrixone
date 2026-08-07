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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type tpch1TFixture struct {
	Database string                        `json:"database"`
	Tables   map[string]tpch1TFixtureStats `json:"tables"`
}

type tpch1TFixtureStats struct {
	TableCnt             float64            `json:"table_cnt"`
	BlockNumber          int64              `json:"block_number"`
	ApproxObjectNumber   int64              `json:"approx_object_number"`
	AccurateObjectNumber int64              `json:"accurate_object_number"`
	NDVMap               map[string]float64 `json:"ndv_map"`
	MinValMap            map[string]float64 `json:"min_val_map"`
	MaxValMap            map[string]float64 `json:"max_val_map"`
	NullCntMap           map[string]uint64  `json:"null_cnt_map"`
	SizeMap              map[string]uint64  `json:"size_map"`
}

type tpch1TCompilerContext struct {
	*MockCompilerContext
	database   string
	statsCache *StatsCache
}

func (ctx *tpch1TCompilerContext) DefaultDatabase() string {
	return ctx.database
}

func (ctx *tpch1TCompilerContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func (ctx *tpch1TCompilerContext) Stats(obj *planpb.ObjectRef, _ *planpb.Snapshot) (*statspb.StatsInfo, error) {
	if obj == nil {
		return nil, nil
	}
	tableDef := ctx.tables[obj.ObjName]
	if tableDef == nil {
		return nil, nil
	}
	wrapper := ctx.statsCache.Get(tableDef.TblId)
	return wrapper.GetStats(), nil
}

func loadTPCH1TFixture(t *testing.T) *tpch1TCompilerContext {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "tpch_1t_fixture.json"))
	require.NoError(t, err)
	fixture := new(tpch1TFixture)
	require.NoError(t, json.Unmarshal(data, fixture))
	require.Len(t, fixture.Tables, 8)

	mock := NewMockCompilerContext(true)
	mock.ctx = context.Background()
	mock.dbs = map[string]bool{fixture.Database: true}
	mock.objects = make(map[string]*planpb.ObjectRef, len(fixture.Tables))
	mock.id2name = make(map[uint64]string, len(fixture.Tables))

	tableNames := make([]string, 0, len(fixture.Tables))
	for tableName := range fixture.Tables {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	cache := NewStatsCache()
	for i, tableName := range tableNames {
		stats := fixture.Tables[tableName]
		tableDef := mock.tables[tableName]
		require.NotNilf(t, tableDef, "missing schema for %s", tableName)
		tableID := uint64(20_000 + i)
		tableDef.TblId = tableID
		tableDef.DbName = fixture.Database
		mock.objects[tableName] = &planpb.ObjectRef{
			Db:         1,
			Obj:        int64(tableID),
			DbName:     fixture.Database,
			SchemaName: fixture.Database,
			ObjName:    tableName,
		}
		mock.id2name[tableID] = tableName

		pkIndexes := mock.pks[tableName]
		if len(pkIndexes) > 0 {
			pkNames := make([]string, 0, len(pkIndexes))
			pkCols := make([]uint64, 0, len(pkIndexes))
			for _, colIdx := range pkIndexes {
				pkNames = append(pkNames, tableDef.Cols[colIdx].Name)
				pkCols = append(pkCols, uint64(colIdx))
			}
			tableDef.Pkey.Names = pkNames
			tableDef.Pkey.Cols = pkCols
		}
		cache.Set(tableDef.TblId, &statspb.StatsInfo{
			TableName:            fixture.Database + "." + tableName,
			TableCnt:             stats.TableCnt,
			BlockNumber:          stats.BlockNumber,
			ApproxObjectNumber:   stats.ApproxObjectNumber,
			AccurateObjectNumber: stats.AccurateObjectNumber,
			NdvMap:               stats.NDVMap,
			MinValMap:            stats.MinValMap,
			MaxValMap:            stats.MaxValMap,
			NullCntMap:           stats.NullCntMap,
			SizeMap:              stats.SizeMap,
			DataTypeMap:          make(map[string]uint64),
			ShuffleRangeMap:      make(map[string]*statspb.ShuffleRange),
		})
	}

	return &tpch1TCompilerContext{
		MockCompilerContext: mock,
		database:            fixture.Database,
		statsCache:          cache,
	}
}

// TestTPCH1TFixturePlans builds the 22 TPC-H plans from captured SF1000
// statistics without connecting to or executing queries on a MatrixOne service.
// Set TPCH_PLAN_OUTPUT_DIR to persist the typed plans as JSON.
func TestTPCH1TFixturePlans(t *testing.T) {
	ctx := loadTPCH1TFixture(t)
	outputDir := os.Getenv("TPCH_PLAN_OUTPUT_DIR")
	if outputDir != "" {
		require.NoError(t, os.MkdirAll(outputDir, 0o755))
	}

	for queryNumber := 1; queryNumber <= 22; queryNumber++ {
		queryNumber := queryNumber
		t.Run(fmt.Sprintf("q%02d", queryNumber), func(t *testing.T) {
			queryPath := filepath.Join("tpch", fmt.Sprintf("q%d.sql", queryNumber))
			query, err := os.ReadFile(queryPath)
			require.NoError(t, err)
			stmts, err := mysql.Parse(ctx.GetContext(), string(query), 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			builtPlan, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
			require.NotNil(t, builtPlan.GetQuery())
			if outputDir == "" {
				return
			}

			planJSON, err := json.MarshalIndent(builtPlan, "", "  ")
			require.NoError(t, err)
			path := filepath.Join(outputDir, fmt.Sprintf("q%02d.json", queryNumber))
			require.NoError(t, os.WriteFile(path, append(planJSON, '\n'), 0o644))
		})
		runtime.GC()
	}
}
