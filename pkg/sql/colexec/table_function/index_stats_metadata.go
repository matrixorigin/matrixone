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

package table_function

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Resolve storage under the same tenant and transaction snapshot as the entry
// scan. These names came from the authorized source definition, never SQL args.
func indexStatsDatabase(proc *process.Process, sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (context.Context, engine.Database, error) {
	ctx := proc.Ctx
	if sqlproc.SnapshotAccountID != nil {
		ctx = defines.AttachAccountId(ctx, *sqlproc.SnapshotAccountID)
	}
	e, ok := ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok {
		e = proc.GetSessionInfo().StorageEngine
	}
	if e == nil || proc.GetTxnOperator() == nil {
		return ctx, nil, moerr.NewInternalError(ctx, "mo_index_stats: storage engine or transaction is unavailable")
	}
	txn := proc.GetTxnOperator()
	if ts := sqlproc.EffectiveSnapshotTS(); ts != nil {
		txn = txn.CloneSnapshotOp(*ts)
	}
	db, err := e.Database(ctx, cfg.Database, txn)
	return ctx, db, err
}

type indexStatsStorageRelation interface {
	Size(context.Context, string) (uint64, error)
	GetColumMetadataScanInfo(context.Context, string, bool) ([]*plan.MetadataScanInfo, error)
}

func indexStatsRelationStorage(ctx context.Context, rel indexStatsStorageRelation, sizeColumns ...string) (uint64, int, int, error) {
	if len(sizeColumns) == 0 {
		sizeColumns = []string{"*"}
	}
	var size uint64
	for _, column := range sizeColumns {
		bytes, err := rel.Size(ctx, column)
		if err != nil {
			return 0, 0, 0, err
		}
		if math.MaxUint64-size < bytes {
			return 0, 0, 0, moerr.NewInvalidInput(ctx, "mo_index_stats: storage size overflow")
		}
		size += bytes
	}
	var counts [2]int
	for i, tombstones := range []bool{false, true} {
		infos, err := rel.GetColumMetadataScanInfo(ctx, "*", tombstones)
		if err != nil {
			return 0, 0, 0, err
		}
		objects := make(map[string]struct{})
		for _, info := range infos {
			objects[info.ObjectName] = struct{}{}
		}
		counts[i] = len(objects)
	}
	return size, counts[0], counts[1], nil
}

func readIndexStatsStorage(proc *process.Process, sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (map[string]any, error) {
	ctx, db, err := indexStatsDatabase(proc, sqlproc, cfg)
	if err != nil {
		return nil, err
	}
	var size uint64
	var objects int
	seen := make(map[string]struct{}, len(cfg.Tables))
	for _, table := range cfg.Tables {
		if _, ok := seen[table.Name]; ok {
			continue
		}
		seen[table.Name] = struct{}{}
		rel, err := db.Relation(ctx, table.Name, nil)
		if err != nil {
			return nil, err
		}
		bytes, data, tombstones, err := indexStatsRelationStorage(ctx, rel, cfg.StorageColumns...)
		if err != nil {
			return nil, err
		}
		if math.MaxUint64-size < bytes {
			return nil, moerr.NewInvalidInput(ctx, "mo_index_stats: storage size overflow")
		}
		size += bytes
		objects += data + tombstones
	}
	return map[string]any{
		"size_bytes":  size,
		"table_count": len(seen), "object_count": objects,
	}, nil
}

func readIndexStatsSourceCommit(proc *process.Process, sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (types.TS, error) {
	ctx, db, err := indexStatsDatabase(proc, sqlproc, cfg)
	if err != nil {
		return types.TS{}, err
	}
	rel, err := db.Relation(ctx, cfg.Table, nil)
	if err != nil {
		return types.TS{}, err
	}
	provider, ok := rel.(engine.SourceCommitTSProvider)
	if !ok {
		return types.TS{}, moerr.NewNotSupported(ctx, "source commit timestamp is unavailable")
	}
	// No early-exit hint: report an exact coverage bar, including its logical half.
	return provider.SourceCommitTS(ctx, types.TS{})
}

func readIndexStatsMaintenance(proc *process.Process, sqlproc *sqlexec.SqlProcess, cfg indexstats.Config) (map[string]any, error) {
	accountID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return nil, err
	}
	if sqlproc.SnapshotAccountID != nil {
		accountID = *sqlproc.SnapshotAccountID
	}
	query := fmt.Sprintf("SELECT CAST(job_state AS BIGINT), watermark, CAST(job_status AS VARCHAR) "+
		"FROM mo_catalog.mo_iscp_log WHERE account_id = %d AND table_id = %d AND job_name = %s "+
		"AND drop_at IS NULL ORDER BY job_id DESC LIMIT 1", accountID, cfg.TableID, sqlquote.String("index_"+cfg.Index))
	// The log is owned by sys. Only the execution identity changes; its tenant
	// filter above and the historical timestamp remain bound to the source read.
	systemSQL := *sqlproc
	systemSQL.WithExecutionIdentity(catalog.System_Account, catalog.MO_CATALOG)
	res, err := indexStatsRunSQL(&systemSQL, query)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	metrics := map[string]any{"status": "task_missing", "caught_up": nil, "lag_seconds": nil}
	for _, bat := range res.Batches {
		if bat.RowCount() == 0 {
			continue
		}
		var status struct {
			Stage     *int8
			ErrorCode int64
			ErrorMsg  string
		}
		if !bat.Vecs[2].IsNull(0) {
			if err := json.Unmarshal(bat.Vecs[2].GetBytesAt(0), &status); err != nil {
				return nil, moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: invalid async job status")
			}
		}
		state := vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
		taskState := "running"
		if state == int64(catalog.MoISCPJobStateError) {
			taskState = "stopped"
		} else if state == int64(iscp.ISCPJobState_Canceled) {
			taskState = "canceled"
		} else if status.ErrorCode != 0 {
			taskState = "retrying"
		}
		metrics["task_state"] = taskState
		if status.ErrorCode != 0 {
			metrics["error_code"] = status.ErrorCode
		}
		if status.ErrorMsg != "" {
			metrics["error_message"] = status.ErrorMsg
		}
		metrics["status"] = "checkpoint_unavailable"
		if status.Stage == nil || *status.Stage != iscp.JobStage_Init && *status.Stage != iscp.JobStage_Running {
			metrics["status"] = "job_status_unavailable"
			return metrics, nil
		}
		if *status.Stage == iscp.JobStage_Init {
			metrics["status"] = "initializing"
			metrics["caught_up"] = false
			return metrics, nil
		}
		watermark, valid := indexStatsWatermark(bat.Vecs[1].GetStringAt(0))
		if !valid {
			return metrics, nil
		}
		metrics["synced_at"] = time.Unix(0, watermark.Physical()).UTC().Format(time.RFC3339Nano)
		source, err := indexStatsSourceCommit(proc, sqlproc, cfg)
		if err != nil {
			if proc.Ctx.Err() != nil {
				return nil, proc.Ctx.Err()
			}
			metrics["status"] = "source_commit_ts_unavailable"
			return metrics, nil
		}
		delete(metrics, "status")
		metrics["caught_up"] = !source.GT(&watermark)
		metrics["lag_seconds"] = float64(max(int64(0), source.Physical()-watermark.Physical())) / float64(time.Second)
		return metrics, nil
	}
	return metrics, nil
}

func indexStatsWatermark(value string) (types.TS, bool) {
	physical, logical, ok := strings.Cut(value, "-")
	if !ok {
		return types.TS{}, false
	}
	p, err := strconv.ParseInt(physical, 10, 64)
	if err != nil || p <= 0 || p == math.MaxInt64 {
		return types.TS{}, false
	}
	l, err := strconv.ParseUint(logical, 10, 32)
	if err != nil {
		return types.TS{}, false
	}
	return types.BuildTS(p, uint32(l)), true
}
