// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const defaultTargetObjectSize = 120 * common.Const1MBytes

type arguments struct {
	db, tbl       string
	accountId     uint64
	objs          []objectio.ObjectStats
	filter        string
	targetObjSize int
}

// Args:
//
//	"dbName.tableName[.accountId][:obj1,obj2,...:targetObjSize]"
//	"dbName.tableName[.accountId][:all:filter:targetObjSize]"
//	".tableId[.accountId][...]"
//
// filter: "overlap", "small"
// filter default: "basic"
// targetObjSize: "1G", "1M", "1K"
// targetObjSize default: "120M"
// Example: "db1.tbl1"
// Example: "db1.tbl1:all:small:100M"
// Example: "db1.tbl1:obj1,obj2:100M"
func parseArgs(arg string) (arguments, error) {
	args := strings.Split(arg, ":")
	if len(args) < 1 || len(args) > 4 {
		return arguments{}, moerr.NewInternalErrorNoCtx("handleMerge: invalid arg format")
	}

	a := arguments{
		targetObjSize: defaultTargetObjectSize,
	}

	// Parse db and table
	dbtbl := strings.Split(args[0], ".")
	if len(dbtbl) == 2 {
		a.db, a.tbl = dbtbl[0], dbtbl[1]
		a.accountId = math.MaxUint64
	} else if len(dbtbl) == 3 {
		a.db, a.tbl = dbtbl[0], dbtbl[1]
		accId, err := strconv.ParseUint(dbtbl[2], 10, 32)
		if err != nil {
			return arguments{}, moerr.NewInternalErrorNoCtx("handleMerge: invalid account id")
		}
		a.accountId = accId
	} else {
		return arguments{}, moerr.NewInternalErrorNoCtx("handleMerge: invalid db.table format")
	}

	// Parse objects
	if len(args) != 1 && args[1] != "all" {
		objs := strings.Split(args[1], ",")
		a.objs = make([]objectio.ObjectStats, 0, len(objs))
		for _, objStr := range objs {
			// Parse object
			objStr = strings.TrimSpace(objStr)
			parts := strings.Split(objStr, "_")
			if len(parts) != 2 {
				return arguments{}, moerr.NewInternalErrorNoCtxf("handleMerge: invalid obj format: %s", objStr)
			}
			uuid, err := types.ParseUuid(parts[0])
			if err != nil {
				return arguments{}, errors.Join(moerr.NewInternalErrorNoCtxf("handleMerge: invalid obj uuid format: %s", objStr), err)
			}
			num, err := strconv.Atoi(parts[1])
			if err != nil {
				return arguments{}, errors.Join(moerr.NewInternalErrorNoCtxf("handleMerge: invalid obj num format: %s", objStr), err)
			}
			objectname := objectio.BuildObjectName(&uuid, uint16(num))
			obj := objectio.NewObjectStats()
			objectio.SetObjectStatsObjectName(obj, objectname)
			a.objs = append(a.objs, *obj)
		}
	}

	if len(args) <= 2 {
		return a, nil
	}

	if args[1] == "all" {
		// Parse filter
		// only parse filter if obj is not specified
		a.filter = strings.TrimSpace(strings.ToLower(args[2]))
		if len(args) == 3 {
			return a, nil
		}
		// Parse targetObjSize
		size, err := units.RAMInBytes(args[3])
		if err != nil {
			return arguments{}, errors.Join(moerr.NewInternalErrorNoCtxf("handleMerge: invalid targetObjSize format: %s", args[3]), err)
		}
		a.targetObjSize = int(size)
	} else {
		// Parse targetObjSize
		size, err := units.RAMInBytes(args[2])
		if err != nil {
			return arguments{}, errors.Join(moerr.NewInternalErrorNoCtxf("handleMerge: invalid targetObjSize format: %s", args[3]), err)
		}
		a.targetObjSize = int(size)
	}

	return a, nil
}

func handleMerge() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpCommitMerge,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(tnShardID uint64, parameter string, proc *process.Process) (payload []byte, err error) {
			txnOp := proc.TxnOperator
			if proc.TxnOperator == nil {
				return nil, moerr.NewInternalError(proc.Ctx, "handleFlush: txn operator is nil")
			}
			a, err := parseArgs(parameter)
			if err != nil {
				return nil, err
			}
			defer func() {
				if err != nil {
					logutil.Error("mergeblocks err on cn",
						zap.String("err", err.Error()), zap.String("parameter", parameter))
					e := &api.MergeCommitEntry{
						StartTs: txnOp.SnapshotTS(),
						Err:     err.Error(),
					}
					for _, o := range a.objs {
						e.MergedObjs = append(e.MergedObjs, o.Clone().Marshal())
					}
					// No matter success or not, we should return the merge result to DN
					payload, _ = e.MarshalBinary()
					err = nil
				}
			}()

			if a.accountId != math.MaxUint64 {
				proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(a.accountId))
			}

			var rel engine.Relation
			tblId, err1 := strconv.ParseUint(a.tbl, 10, 64)
			if err1 == nil {
				_, _, rel, err = proc.SessionInfo.StorageEngine.GetRelationById(proc.Ctx, txnOp, tblId)
				if err != nil {
					logutil.Errorf("mergeblocks err on cn, tblId %d, err %s", tblId, err.Error())
					return nil, err
				}
			} else {
				var database engine.Database
				database, err = proc.SessionInfo.StorageEngine.Database(proc.Ctx, a.db, txnOp)
				if err != nil {
					logutil.Errorf("mergeblocks err on cn, db %s, err %s", a.db, err.Error())
					return nil, err
				}
				rel, err = database.Relation(proc.Ctx, a.tbl, nil)
				if err != nil {
					logutil.Errorf("mergeblocks err on cn, table %s, err %s", a.db, err.Error())
					return nil, err
				}
			}

			entry, err := rel.MergeObjects(proc.Ctx, a.objs, a.filter, uint32(a.targetObjSize))
			if err != nil {
				merge.CleanUpUselessFiles(entry, proc.FileService)
				return nil, err
			}
			payload, err = entry.MarshalBinary()
			if err != nil {
				return nil, err
			}
			return payload, nil
		},
		func(data []byte) (any, error) {
			resp := &db.InspectResp{}
			if err := types.Decode(data, resp); err != nil {
				return nil, err
			}
			return resp, nil
		})
}
