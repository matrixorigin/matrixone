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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type tpcds1TFixture struct {
	Database string                         `json:"database"`
	Tables   map[string]tpcds1TFixtureTable `json:"tables"`
	Queries  map[string]string              `json:"queries"`
}

type tpcds1TFixtureTable struct {
	Columns    []tpcds1TFixtureColumn `json:"columns"`
	PrimaryKey []string               `json:"primary_key"`
	Stats      tpcds1TFixtureStats    `json:"stats"`
}

type tpcds1TFixtureColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type tpcds1TFixtureStats struct {
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

type tpcds1TCompilerContext struct {
	*MockCompilerContext
	database   string
	statsCache *StatsCache
}

func (ctx *tpcds1TCompilerContext) DefaultDatabase() string {
	return ctx.database
}

func (ctx *tpcds1TCompilerContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func (ctx *tpcds1TCompilerContext) Stats(obj *planpb.ObjectRef, _ *planpb.Snapshot) (*statspb.StatsInfo, error) {
	if obj == nil {
		return nil, nil
	}
	tableDef := ctx.tables[strings.ToLower(obj.ObjName)]
	if tableDef == nil {
		return nil, nil
	}
	wrapper := ctx.statsCache.Get(tableDef.TblId)
	return wrapper.GetStats(), nil
}

func loadTPCDS1TFixture(t *testing.T) (*tpcds1TFixture, *tpcds1TCompilerContext) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "tpcds_1t_fixture.json"))
	require.NoError(t, err)

	fixture := new(tpcds1TFixture)
	require.NoError(t, json.Unmarshal(data, fixture))

	mock := NewEmptyCompilerContext()
	mock.ctx = context.Background()
	mock.dbs = map[string]bool{fixture.Database: true}
	mock.id2name = make(map[uint64]string, len(fixture.Tables))
	mock.pks = make(map[string][]int, len(fixture.Tables))
	cache := NewStatsCache()

	tableNames := make([]string, 0, len(fixture.Tables))
	for name := range fixture.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	for i, name := range tableNames {
		fixtureTable := fixture.Tables[name]
		// Keep fixture IDs away from the reserved system-catalog ID range.
		tableID := uint64(10_000 + i)
		cols := make([]*planpb.ColDef, 0, len(fixtureTable.Columns))
		name2ColIndex := make(map[string]int32, len(fixtureTable.Columns))
		for colIdx, fixtureCol := range fixtureTable.Columns {
			colType, typeErr := tpcdsFixtureColumnType(fixtureCol.Type)
			require.NoErrorf(t, typeErr, "%s.%s", name, fixtureCol.Name)
			col := &planpb.ColDef{
				ColId:      uint64(colIdx),
				Name:       fixtureCol.Name,
				OriginName: fixtureCol.Name,
				Typ:        colType,
				Default:    &planpb.Default{NullAbility: true},
			}
			cols = append(cols, col)
			name2ColIndex[fixtureCol.Name] = int32(colIdx)
		}

		tableDef := &planpb.TableDef{
			TblId:         tableID,
			Name:          name,
			DbName:        fixture.Database,
			TableType:     catalog.SystemOrdinaryRel,
			Cols:          cols,
			Name2ColIndex: name2ColIndex,
		}
		if len(fixtureTable.PrimaryKey) > 0 {
			pkeyName := fixtureTable.PrimaryKey[0]
			if len(fixtureTable.PrimaryKey) > 1 {
				pkeyName = catalog.CPrimaryKeyColName
			}
			tableDef.Pkey = &planpb.PrimaryKeyDef{
				Names:       fixtureTable.PrimaryKey,
				PkeyColName: pkeyName,
			}
		}

		mock.tables[name] = tableDef
		mock.objects[name] = &planpb.ObjectRef{
			Db:         1,
			Obj:        int64(tableID),
			DbName:     fixture.Database,
			SchemaName: fixture.Database,
			ObjName:    name,
		}
		mock.id2name[tableID] = name

		stats := fixtureTable.Stats
		cache.Set(tableID, &statspb.StatsInfo{
			TableName:            fixture.Database + "." + name,
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

	return fixture, &tpcds1TCompilerContext{
		MockCompilerContext: mock,
		database:            fixture.Database,
		statsCache:          cache,
	}
}

func tpcdsFixtureColumnType(typeName string) (planpb.Type, error) {
	upper := strings.ToUpper(typeName)
	switch {
	case upper == "BIGINT":
		return planpb.Type{Id: int32(types.T_int64)}, nil
	case upper == "INTEGER":
		return planpb.Type{Id: int32(types.T_int32)}, nil
	case upper == "DATE":
		return planpb.Type{Id: int32(types.T_date)}, nil
	case upper == "TIME":
		return planpb.Type{Id: int32(types.T_time)}, nil
	case strings.HasPrefix(upper, "VARCHAR"):
		return planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, nil
	case strings.HasPrefix(upper, "DECIMAL("):
		var width, scale int32
		_, err := fmt.Sscanf(upper, "DECIMAL(%d,%d)", &width, &scale)
		if err != nil {
			return planpb.Type{}, err
		}
		id := types.T_decimal64
		if width > 18 {
			id = types.T_decimal128
		}
		return planpb.Type{Id: int32(id), Width: width, Scale: scale}, nil
	default:
		return planpb.Type{}, fmt.Errorf("unsupported TPC-DS type %q", typeName)
	}
}

// TestTPCDS1TFixturePlans builds all 99 TPC-DS plans without connecting to a
// MatrixOne process. Set TPCDS_PLAN_OUTPUT_DIR to persist the complete plans.
func TestTPCDS1TFixturePlans(t *testing.T) {
	fixture, ctx := loadTPCDS1TFixture(t)
	queryNumbers := make([]int, 0, len(fixture.Queries))
	for queryNumber := range fixture.Queries {
		n, err := strconv.Atoi(queryNumber)
		require.NoError(t, err)
		queryNumbers = append(queryNumbers, n)
	}
	sort.Ints(queryNumbers)
	require.Len(t, queryNumbers, 99)

	outputDir := os.Getenv("TPCDS_PLAN_OUTPUT_DIR")
	if outputDir != "" {
		require.NoError(t, os.MkdirAll(outputDir, 0o755))
	}

	for _, queryNumber := range queryNumbers {
		queryNumber := queryNumber
		t.Run(fmt.Sprintf("q%02d", queryNumber), func(t *testing.T) {
			stmts, err := mysql.Parse(ctx.GetContext(), fixture.Queries[strconv.Itoa(queryNumber)], 1)
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
		// Q64 produces a large memo. Release it before building the next fixture
		// so this offline regression does not require server-sized memory.
		runtime.GC()
	}
}
