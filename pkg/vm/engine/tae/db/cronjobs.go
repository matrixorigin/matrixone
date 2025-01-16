// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

const (
	CronJobs_Name_GCTransferTable = "GC-Transfer-Table"
	CronJobs_Name_GCDisk          = "GC-Disk"
	CronJobs_Name_GCCheckpoint    = "GC-Checkpoint"
	CronJobs_Name_GCCatalogCache  = "GC-Catalog-Cache"
	CronJobs_Name_GCLogtail       = "GC-Logtail"
	CronJobs_Name_GCLockMerge     = "GC-Lock-Merge"

	CronJobs_Name_ReportStats = "Report-Stats"

	CronJobs_Name_Checker = "Checker"
)

var CronJobs_Open_WriteMode = []string{
	CronJobs_Name_GCTransferTable,
	CronJobs_Name_GCDisk,
	CronJobs_Name_GCCheckpoint,
	CronJobs_Name_GCCatalogCache,
	CronJobs_Name_GCLogtail,
	CronJobs_Name_GCLockMerge,
	CronJobs_Name_ReportStats,
}

var CronJobs_Open_ReplayMode = []string{
	CronJobs_Name_GCTransferTable,
	CronJobs_Name_GCCatalogCache,
	CronJobs_Name_GCLogtail,
	CronJobs_Name_ReportStats,
}

// key(string): cron job name
// value(bool,bool,bool,bool):
// 1.bool,2.bool: can be in the write mode, must be in the write mode
// 3.bool,4.bool: can be in the replay mode, must be in the replay mode
var CronJobs_Spec = map[string][]bool{
	CronJobs_Name_GCTransferTable: {true, true, true, true},
	CronJobs_Name_GCDisk:          {true, true, false, false},
	CronJobs_Name_GCCheckpoint:    {true, true, false, false},
	CronJobs_Name_GCCatalogCache:  {true, true, true, true},
	CronJobs_Name_GCLogtail:       {true, true, true, true},
	CronJobs_Name_GCLockMerge:     {true, true, true, false},
	CronJobs_Name_ReportStats:     {true, true, true, true},
	CronJobs_Name_Checker:         {true, false, true, false},
}

func CanAddCronJob(name string, isWriteModeDB, skipMode bool) bool {
	if v, ok := CronJobs_Spec[name]; ok {
		if skipMode {
			return true
		}
		if isWriteModeDB {
			return v[0]
		}
		return v[2]
	}
	return false
}

func AddCronJobs(db *DB) (err error) {
	isWriteMode := db.IsWriteMode()
	if isWriteMode {
		for _, name := range CronJobs_Open_WriteMode {
			if err = AddCronJob(db, name, false); err != nil {
				return
			}
		}
	} else {
		for _, name := range CronJobs_Open_ReplayMode {
			if err = AddCronJob(db, name, false); err != nil {
				return
			}
		}
	}
	if db.Opts.CheckpointCfg.MetadataCheckInterval > 0 {
		if err = AddCronJob(db, CronJobs_Name_Checker, false); err != nil {
			return
		}
	}
	err = CheckCronJobs(db, db.GetTxnMode())
	return
}

func AddCronJob(db *DB, name string, skipMode bool) (err error) {
	if !CanAddCronJob(name, db.IsWriteMode(), skipMode) {
		return moerr.NewInternalErrorNoCtxf(
			"cannot add cron job %s in %s mode", name, db.GetTxnMode(),
		)
	}

	switch name {
	case CronJobs_Name_GCTransferTable:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCTransferTable,
			db.Opts.CheckpointCfg.TransferInterval,
			func(context.Context) {
				db.Runtime.PoolUsageReport()
				// dbutils.PrintMemStats()
				db.Runtime.TransferDelsMap.Prune(db.Opts.TransferTableTTL)
				db.Runtime.TransferTable.RunTTL()
			},
			1,
		)
		return
	case CronJobs_Name_GCDisk:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCDisk,
			db.Opts.GCCfg.ScanGCInterval,
			func(ctx context.Context) {
				db.DiskCleaner.GC(ctx)
			},
			1,
		)
		return
	case CronJobs_Name_GCCheckpoint:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCCheckpoint,
			db.Opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) {
				if db.Opts.CheckpointCfg.DisableGCCheckpoint {
					return
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetCheckpointGCWaterMark()
				if gcWaterMark == nil {
					return
				}
				if err := db.BGCheckpointRunner.GCByTS(ctx, *gcWaterMark); err != nil {
					logutil.Error(
						"GC-Checkpoint-Err",
						zap.Error(err),
					)
				}
			},
			1,
		)
		return
	case CronJobs_Name_GCCatalogCache:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCCatalogCache,
			db.Opts.CatalogCfg.GCInterval,
			func(ctx context.Context) {
				if db.Opts.CatalogCfg.DisableGC {
					return
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetScanWaterMark()
				if gcWaterMark == nil {
					return
				}
				db.Catalog.GCByTS(ctx, gcWaterMark.GetEnd())
			},
			1,
		)
		return
	case CronJobs_Name_GCLogtail:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCLogtail,
			db.Opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) {
				ckp := db.BGCheckpointRunner.MaxIncrementalCheckpoint()
				if ckp == nil {
					return
				}
				endTS := ckp.GetEnd()
				if ts := types.TSSubDuration(&endTS, time.Second*55); ts.Valid() {
					if updated := db.LogtailMgr.GCByTS(ctx, ts); updated {
						logutil.Info(db.Runtime.ExportLogtailStats())
					}
				}
			},
			1,
		)
		return
	case CronJobs_Name_GCLockMerge:
		err = db.CronJobs.AddJob(
			CronJobs_Name_GCLockMerge,
			options.DefaultLockMergePruneInterval,
			func(ctx context.Context) {
				db.Runtime.LockMergeService.Prune()
			},
			1,
		)
		return
	case CronJobs_Name_ReportStats:
		err = db.CronJobs.AddJob(
			CronJobs_Name_ReportStats,
			time.Second*10,
			func(ctx context.Context) {
				mpoolAllocatorSubTask()
			},
			1,
		)
		return
	case CronJobs_Name_Checker:
		err = db.CronJobs.AddJob(
			CronJobs_Name_Checker,
			db.Opts.CheckpointCfg.MetadataCheckInterval,
			func(ctx context.Context) {
				db.Catalog.CheckMetadata()
			},
			1,
		)
		return
	}
	err = moerr.NewInternalErrorNoCtxf(
		"unknown cron job name: %s", name,
	)
	return
}

func RemoveCronJob(db *DB, name string) {
	db.CronJobs.RemoveJob(name)
}

func CheckCronJobs(db *DB, expectMode DBTxnMode) (err error) {
	for name, spec := range CronJobs_Spec {
		if (expectMode.IsWriteMode() && spec[1]) || (expectMode.IsReplayMode() && spec[3]) {
			if job := db.CronJobs.GetJob(name); job == nil {
				err = moerr.NewInternalErrorNoCtxf("missing cron job %s in %s mode", name, expectMode)
				return
			}
		}
	}
	db.CronJobs.ForeachJob(func(name string, _ *tasks.CancelableJob) bool {
		if spec, ok := CronJobs_Spec[name]; !ok {
			err = moerr.NewInternalErrorNoCtxf("unknown cron job name: %s", name)
			return false
		} else {
			if expectMode.IsWriteMode() {
				if !spec[0] {
					err = moerr.NewInternalErrorNoCtxf("invalid cron job %s in %s mode", name, expectMode)
					return false
				}
			} else {
				if !spec[2] {
					err = moerr.NewInternalErrorNoCtxf("invalid cron job %s in %s mode", name, expectMode)
					return false
				}
			}
		}
		return true
	})
	return
}
