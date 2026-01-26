// Copyright 2022 Matrix Origin
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

package table_function

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Command constants for table_stats TVF
const (
	CmdGet     = "get"     // Get stats (default)
	CmdRefresh = "refresh" // Refresh stats
	CmdPatch   = "patch"   // Patch stats partially
)

type tableStatsState struct {
	simpleOneBatchState
}

func tableStatsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	var err error
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	for i := range tf.Attrs {
		tf.Attrs[i] = strings.ToUpper(tf.Attrs[i])
	}
	return &tableStatsState{}, err
}

func (s *tableStatsState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	s.startPreamble(tf, proc, nthRow)

	// Parse arguments: table_stats(table_path, command, args)
	tablePath := tf.ctr.argVecs[0].GetStringAt(nthRow)

	// command defaults to "get"
	command := CmdGet
	if len(tf.ctr.argVecs) > 1 && !tf.ctr.argVecs[1].IsConstNull() {
		command = strings.ToLower(tf.ctr.argVecs[1].GetStringAt(nthRow))
	}

	// args JSON (optional)
	var argsJSON string
	if len(tf.ctr.argVecs) > 2 && !tf.ctr.argVecs[2].IsConstNull() {
		argsJSON = tf.ctr.argVecs[2].GetStringAt(nthRow)
	}

	// Parse table path
	dbname, tablename, accountId, err := parseTablePathWithAccount(tablePath, proc)
	if err != nil {
		return err
	}

	// Get engine
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	rel, err := getRelation(e, proc, dbname, tablename)
	if err != nil {
		return err
	}

	// Build stats key
	key := pb.StatsInfoKey{
		AccId:      accountId,
		DatabaseID: rel.GetDBID(proc.Ctx),
		TableID:    rel.GetTableID(proc.Ctx),
		TableName:  tablename,
		DbName:     dbname,
	}

	// Get GlobalStats
	gs := getGlobalStats(e)
	if gs == nil {
		return moerr.NewInvalidInputNoCtx("no valid engine")
	}

	// Execute command
	switch command {
	case CmdGet, "":
		return s.executeGet(tf, proc, key, gs, argsJSON)
	case CmdRefresh:
		return s.executeRefresh(tf, proc, key, gs, argsJSON)
	case CmdPatch:
		return s.executePatch(tf, proc, key, gs, argsJSON)
	default:
		return moerr.NewInternalError(proc.Ctx,
			fmt.Sprintf("unknown command: %s (supported: get, refresh, patch)", command))
	}
}

// executeGet handles the 'get' command
// args can be "verbose" to print all shuffle range results
func (s *tableStatsState) executeGet(tf *TableFunction, proc *process.Process,
	key pb.StatsInfoKey, gs *disttae.GlobalStats, args string) error {

	verbose := strings.ToLower(args) == "verbose"
	stats := gs.Get(proc.Ctx, key, true)
	samplingRatio := gs.GetSamplingRatio(key)

	return fillStats(s.batch, tf, &key, stats, samplingRatio, verbose, proc)
}

// executeRefresh handles the 'refresh' command
// args is the refresh mode: "auto" or "full" (default: "auto")
func (s *tableStatsState) executeRefresh(tf *TableFunction, proc *process.Process,
	key pb.StatsInfoKey, gs *disttae.GlobalStats, args string) error {

	// Default mode is "auto"
	mode := "auto"
	if args != "" {
		mode = strings.ToLower(args)
	}

	// Validate mode
	if mode != "auto" && mode != "full" {
		return moerr.NewInternalError(proc.Ctx,
			fmt.Sprintf("invalid refresh mode: %s (must be 'auto' or 'full')", mode))
	}

	if err := gs.RefreshWithMode(proc.Ctx, key, mode); err != nil {
		return err
	}

	stats := gs.Get(proc.Ctx, key, true)
	samplingRatio := gs.GetSamplingRatio(key)

	return fillStats(s.batch, tf, &key, stats, samplingRatio, false, proc)
}

// executePatch handles the 'patch' command
func (s *tableStatsState) executePatch(tf *TableFunction, proc *process.Process,
	key pb.StatsInfoKey, gs *disttae.GlobalStats, argsJSON string) error {

	if argsJSON == "" {
		return moerr.NewInternalError(proc.Ctx, "patch command requires args")
	}

	var patch disttae.PatchArgs
	if err := json.Unmarshal([]byte(argsJSON), &patch); err != nil {
		return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("invalid patch args: %v", err))
	}

	if err := gs.PatchStats(key, &patch); err != nil {
		return err
	}

	stats := gs.Get(proc.Ctx, key, false)
	samplingRatio := gs.GetSamplingRatio(key)

	return fillStats(s.batch, tf, &key, stats, samplingRatio, false, proc)
}

func (s *tableStatsState) reset(tf *TableFunction, proc *process.Process) {
	s.simpleOneBatchState.reset(tf, proc)
}

func (s *tableStatsState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	s.simpleOneBatchState.free(tf, proc, pipelineFailed, err)
}

// getGlobalStats extracts GlobalStats from engine
func getGlobalStats(e engine.Engine) *disttae.GlobalStats {
	actualEngine := e
	if entireEng, ok := e.(*engine.EntireEngine); ok {
		actualEngine = entireEng.Engine
	}
	if disttaeEng, ok := actualEngine.(*disttae.Engine); ok {
		return disttaeEng.GetGlobalStats()
	}
	return nil
}

// parseTablePathWithAccount parses "db.table" or "table" or "db.table.account_id" format
// Returns: dbname, tablename, accountId, error
func parseTablePathWithAccount(path string, proc *process.Process) (string, string, uint32, error) {
	parts := strings.Split(path, ".")

	// Get current account ID for permission check
	currentAccountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return "", "", 0, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("get account id failed: %v", err))
	}

	switch len(parts) {
	case 1:
		// "table" format - use current database and account
		dbname := proc.GetSessionInfo().Database
		if dbname == "" {
			return "", "", 0, moerr.NewInternalError(proc.Ctx, "no database selected")
		}
		return dbname, parts[0], currentAccountId, nil

	case 2:
		// "db.table" format - use current account
		return parts[0], parts[1], currentAccountId, nil

	case 3:
		// "db.table.account_id" format - use specified account
		var specifiedAccountId uint32
		_, err := fmt.Sscanf(parts[2], "%d", &specifiedAccountId)
		if err != nil {
			return "", "", 0, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("invalid account_id: %s (must be a number)", parts[2]))
		}

		// Permission check: only sys account (account_id = 0) can specify different account_id
		if specifiedAccountId != currentAccountId && currentAccountId != catalog.System_Account {
			return "", "", 0, moerr.NewInternalError(proc.Ctx, "only sys account can query stats for other accounts")
		}

		return parts[0], parts[1], specifiedAccountId, nil

	default:
		return "", "", 0, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("invalid table path: %s (expected format: table, db.table, or db.table.account_id)", path))
	}
}

// getRelation gets the relation for the specified table
func getRelation(e engine.Engine, proc *process.Process, dbname, tablename string) (engine.Relation, error) {
	db, err := e.Database(proc.Ctx, dbname, proc.GetTxnOperator())
	if err != nil {
		return nil, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("database %s not found: %v", dbname, err))
	}

	rel, err := db.Relation(proc.Ctx, tablename, nil)
	if err != nil {
		return nil, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("table %s.%s not found: %v", dbname, tablename, err))
	}

	return rel, nil
}

// fillStats fills the result batch with stats information
// It handles projection by only filling the columns that are actually requested
// verbose controls whether to print all shuffle range results or just head/tail
func fillStats(bat *batch.Batch, tf *TableFunction, key *pb.StatsInfoKey, stats *pb.StatsInfo, samplingRatio float64, verbose bool, proc *process.Process) error {
	mp := proc.Mp()
	tableName := fmt.Sprintf("%s.%s", key.DbName, key.TableName)

	// Iterate through projected columns (tf.Attrs contains the column names)
	for i, colName := range tf.Attrs {
		colNameUpper := strings.ToUpper(colName)

		switch colNameUpper {
		case "TABLE_NAME":
			if err := vector.AppendBytes(bat.Vecs[i], []byte(tableName), false, mp); err != nil {
				return err
			}

		case "TABLE_CNT":
			if stats == nil {
				if err := vector.AppendFixed(bat.Vecs[i], float64(0), false, mp); err != nil {
					return err
				}
			} else {
				if err := vector.AppendFixed(bat.Vecs[i], stats.TableCnt, false, mp); err != nil {
					return err
				}
			}

		case "BLOCK_NUMBER":
			if stats == nil {
				if err := vector.AppendFixed(bat.Vecs[i], int64(0), false, mp); err != nil {
					return err
				}
			} else {
				if err := vector.AppendFixed(bat.Vecs[i], stats.BlockNumber, false, mp); err != nil {
					return err
				}
			}

		case "APPROX_OBJECT_NUMBER":
			if stats == nil {
				if err := vector.AppendFixed(bat.Vecs[i], int64(0), false, mp); err != nil {
					return err
				}
			} else {
				if err := vector.AppendFixed(bat.Vecs[i], stats.ApproxObjectNumber, false, mp); err != nil {
					return err
				}
			}

		case "ACCURATE_OBJECT_NUMBER":
			if stats == nil {
				if err := vector.AppendFixed(bat.Vecs[i], int64(0), false, mp); err != nil {
					return err
				}
			} else {
				if err := vector.AppendFixed(bat.Vecs[i], stats.AccurateObjectNumber, false, mp); err != nil {
					return err
				}
			}

		case "SAMPLING_RATIO":
			if err := vector.AppendFixed(bat.Vecs[i], samplingRatio, false, mp); err != nil {
				return err
			}

		case "NDV_MAP":
			jsonStr, err := mapToJSON(stats, func(s *pb.StatsInfo) any { return s.NdvMap })
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "MIN_VAL_MAP":
			jsonStr, err := mapToJSON(stats, func(s *pb.StatsInfo) any { return s.MinValMap })
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "MAX_VAL_MAP":
			jsonStr, err := mapToJSON(stats, func(s *pb.StatsInfo) any { return s.MaxValMap })
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "DATA_TYPE_MAP":
			// Convert type IDs to readable names
			var dataTypeNames map[string]string
			if stats != nil && len(stats.DataTypeMap) > 0 {
				dataTypeNames = make(map[string]string, len(stats.DataTypeMap))
				for colName, typeID := range stats.DataTypeMap {
					dataTypeNames[colName] = types.T(typeID).String()
				}
			}
			jsonStr, err := anyToJSON(dataTypeNames)
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "NULL_CNT_MAP":
			jsonStr, err := mapToJSON(stats, func(s *pb.StatsInfo) any { return s.NullCntMap })
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "SIZE_MAP":
			jsonStr, err := mapToJSON(stats, func(s *pb.StatsInfo) any { return s.SizeMap })
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		case "SHUFFLE_RANGE_MAP":
			// Only include serializable fields (overlap, uniform, result)
			var shuffleInfo map[string]any
			if stats != nil && len(stats.ShuffleRangeMap) > 0 {
				shuffleInfo = make(map[string]any, len(stats.ShuffleRangeMap))
				for colName, sr := range stats.ShuffleRangeMap {
					if sr != nil {
						info := map[string]any{
							"overlap": sr.Overlap,
							"uniform": sr.Uniform,
						}
						// Handle result array based on verbose flag
						if len(sr.Result) > 1 {
							if verbose {
								// Print all results
								info["result"] = sr.Result
							} else {
								// Print only head and tail (first 2 and last 2)
								info["result"] = []any{
									sr.Result[0],
									"...",
									sr.Result[len(sr.Result)-1],
								}
							}
						}
						shuffleInfo[colName] = info
					}
				}
			}
			jsonStr, err := anyToJSON(shuffleInfo)
			if err != nil {
				return err
			}
			if err := vector.AppendBytes(bat.Vecs[i], []byte(jsonStr), false, mp); err != nil {
				return err
			}

		default:
			return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unknown column name: %s", colName))
		}
	}

	bat.SetRowCount(1)
	return nil
}

// mapToJSON converts a map field from StatsInfo to JSON string
func mapToJSON(stats *pb.StatsInfo, getter func(*pb.StatsInfo) any) (string, error) {
	if stats == nil {
		return "{}", nil
	}
	m := getter(stats)
	return anyToJSON(m)
}

// anyToJSON converts any value to pretty-printed JSON string
func anyToJSON(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	// Check if it's an empty map using reflection
	switch m := v.(type) {
	case map[string]string:
		if len(m) == 0 {
			return "{}", nil
		}
	case map[string]any:
		if len(m) == 0 {
			return "{}", nil
		}
	}
	jsonBytes, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
