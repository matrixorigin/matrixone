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
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const defaultTargetObjectSize = 120 * common.Const1MBytes

type mergeType uint8

const (
	objectMergeType mergeType = iota
	tableMergeType
)

type arguments struct {
	mergeType     mergeType
	db, tbl       string
	accountId     uint64
	objs          []objectio.ObjectStats
	filter        string
	targetObjSize int
}

// Args:
//
// "o:tableID[.accountID]:obj1, obj2...[:targetObjSize]"
// "t:dbName.tableName[[.accountID]:filter:targetObjSize]"
//
// filter: "overlap", "small"
// filter default: "basic"
// targetObjSize: "1G", "1M", "1K"
// targetObjSize default: "120M"
// Example: "t:db1.tbl1"
// Example: "t:db1.tbl1:small:100M"
// Example: "o:tblID.accID:obj1,obj2:100M"

func parseArgsForPartitionTables(arg string) (arguments, error) {
	args := strings.Split(arg, ":")
	mergeType := args[0]
	args = args[1:]
	if len(args) > 3 {
		return arguments{}, moerr.NewInvalidArgNoCtx("mergeobjcts", arg)
	}

	a := arguments{
		targetObjSize: defaultTargetObjectSize,
		accountId:     math.MaxUint64,
	}

	switch mergeType {
	case "o":
		a.mergeType = objectMergeType
		tblAccount := strings.Split(args[0], ".")
		if len(tblAccount) > 2 {
			return arguments{}, moerr.NewInvalidArgNoCtx("tableID.accountID", args[0])
		}
		a.tbl = tblAccount[0]
		if len(tblAccount) == 2 {
			accID, err := strconv.ParseUint(tblAccount[1], 10, 32)
			if err != nil {
				return arguments{}, moerr.NewInvalidArgNoCtx("accountID", args[0])
			}
			a.accountId = accID
		}

		// Parse objects
		objs := strings.Split(args[1], ",")
		a.objs = make([]objectio.ObjectStats, 0, len(objs))
		for _, objStr := range objs {
			// Parse object
			objStr = strings.TrimSpace(objStr)
			parts := strings.Split(objStr, "_")
			if len(parts) != 2 {
				return arguments{}, moerr.NewInvalidArgNoCtx("object", objStr)
			}
			uuid, err := types.ParseUuid(parts[0])
			if err != nil {
				return arguments{}, errors.Join(moerr.NewInvalidArgNoCtx("object", objStr), err)
			}
			num, err := strconv.Atoi(parts[1])
			if err != nil {
				return arguments{}, errors.Join(moerr.NewInvalidArgNoCtx("object", objStr), err)
			}
			objectname := objectio.BuildObjectName(&uuid, uint16(num))
			obj := objectio.NewObjectStats()
			objectio.SetObjectStatsObjectName(obj, objectname)
			a.objs = append(a.objs, *obj)
		}

		if len(args) == 3 {
			size, err := units.RAMInBytes(args[2])
			if err != nil {
				return arguments{}, errors.Join(moerr.NewInvalidArgNoCtx("targetObjSize", args[3]), err)
			}
			a.targetObjSize = int(size)
		}
	case "t":
		a.mergeType = tableMergeType

		dbTblAccount := strings.Split(args[0], ".")
		if len(dbTblAccount) < 2 || len(dbTblAccount) > 3 {
			return arguments{}, moerr.NewInvalidArgNoCtx("dbName.tblName[.accountID]", args[0])
		}
		a.db, a.tbl = dbTblAccount[0], dbTblAccount[1]
		if len(dbTblAccount) == 3 {
			accID, err := strconv.ParseUint(dbTblAccount[2], 10, 32)
			if err != nil {
				return arguments{}, moerr.NewInvalidArgNoCtx("accountID", dbTblAccount[2])
			}
			a.accountId = accID
		}
		if len(args) == 1 {
			break
		}

		// Parse filter
		a.filter = strings.TrimSpace(strings.ToLower(args[1]))
		if len(args) == 2 {
			break
		}
		// Parse targetObjSize
		size, err := units.RAMInBytes(args[2])
		if err != nil {
			return arguments{}, errors.Join(moerr.NewInvalidArgNoCtx("targetObjSize", args[3]), err)
		}
		a.targetObjSize = int(size)

	default:
		return arguments{}, moerr.NewInvalidArgNoCtx("mergeobjcts", arg)
	}
	return a, nil
}

func handleCNMerge(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewNotSupported(proc.Ctx, "service %s not supported", service)
	}

	txnOp := proc.GetTxnOperator()
	if txnOp == nil {
		return Result{}, moerr.NewInternalError(proc.Ctx, "handleFlush: txn operator is nil")
	}
	a, err := parseArgsForPartitionTables(parameter)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		if err != nil {
			logutil.Error("mergeblocks err on cn",
				zap.String("err", err.Error()), zap.String("parameter", parameter))
		}
	}()

	ctx := proc.Ctx
	if a.accountId != math.MaxUint64 {
		ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(a.accountId))
	}

	switch a.mergeType {
	case objectMergeType:
		tblId, err := strconv.ParseUint(a.tbl, 10, 64)
		if err != nil {
			return Result{}, moerr.NewInvalidArgNoCtx("tableID", a.tbl)
		}
		_, _, rel, err := proc.GetSessionInfo().StorageEngine.GetRelationById(ctx, txnOp, tblId)
		if err != nil {
			logutil.Errorf("mergeblocks err on cn, tblId %d, err %s", tblId, err.Error())
			return Result{}, err
		}

		entry, err := rel.MergeObjects(ctx, a.objs, a.filter, uint32(a.targetObjSize))
		if err != nil {
			merge.CleanUpUselessFiles(entry, proc.GetFileService())
			if entry == nil {
				entry = &api.MergeCommitEntry{
					Err: err.Error(),
				}
			}
		}

		payload, err := entry.MarshalBinary()
		if err != nil {
			return Result{}, err
		}

		resp, err := txnWrite(ctx, proc.GetService(), txnOp, payload)
		if err != nil {
			return Result{}, err
		}
		defer resp.Release()
		return Result{
			Method: MergeObjectsMethod,
			Data:   resp.Responses[0].CNOpResponse.Payload,
		}, nil

	case tableMergeType:

		database, err := proc.GetSessionInfo().StorageEngine.Database(ctx, a.db, txnOp)
		if err != nil {
			logutil.Errorf("mergeblocks err on cn, db %s, err %s", a.db, err.Error())
			return Result{}, err
		}
		rel, err := database.Relation(ctx, a.tbl, nil)
		if err != nil {
			logutil.Errorf("mergeblocks err on cn, table %s, err %s", a.db, err.Error())
			return Result{}, err
		}

		var engineDefs []engine.TableDef
		engineDefs, err = rel.TableDefs(ctx)
		if err != nil {
			return Result{}, err
		}
		var partitionInfo *plan.PartitionByDef
		for _, def := range engineDefs {
			if partitionDef, ok := def.(*engine.PartitionDef); ok {
				if partitionDef.Partitioned > 0 {
					p := &plan.PartitionByDef{}
					err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
					if err != nil {
						return Result{}, err
					}
					partitionInfo = p
				}
			}
		}

		if partitionInfo == nil {
			entry, err := rel.MergeObjects(ctx, a.objs, a.filter, uint32(a.targetObjSize))
			if err != nil {
				return Result{}, err
			}

			payload, err := entry.MarshalBinary()
			if err != nil {
				return Result{}, err
			}

			resp, err := txnWrite(ctx, proc.GetService(), txnOp, payload)
			if err != nil {
				return Result{}, err
			}
			defer resp.Release()
			return Result{
				Method: MergeObjectsMethod,
				Data:   resp.Responses[0].CNOpResponse.Payload,
			}, nil
		}

		// check if the current table is partitioned
		var prel engine.Relation
		var resultBuffer bytes.Buffer
		// for partition table, run merge on each partition table separately.
		for _, partitionTable := range partitionInfo.PartitionTableNames {
			prel, err = database.Relation(ctx, partitionTable, nil)
			if err != nil {
				return Result{}, err
			}
			entry, err := prel.MergeObjects(ctx, a.objs, a.filter, uint32(a.targetObjSize))
			if err != nil {
				resultBuffer.WriteString(err.Error())
				resultBuffer.WriteString("\n")
				continue
			}

			payload, err := entry.MarshalBinary()
			if err != nil {
				return Result{}, err
			}

			resp, err := txnWrite(ctx, proc.GetService(), txnOp, payload)
			if err != nil {
				return Result{}, err
			}
			resultBuffer.WriteString(string(resp.Responses[0].CNOpResponse.Payload))
			resultBuffer.WriteString("\n")
			resp.Release()
		}
		return Result{
			Method: MergeObjectsMethod,
			Data:   resultBuffer.Bytes(),
		}, nil
	}
	return Result{}, nil
}

func txnWrite(ctx context.Context, service string, txnOp client.TxnOperator, payload []byte) (*rpc.SendResult, error) {
	var target metadata.TNShard
	cluster := clusterservice.GetMOCluster(service)
	cluster.GetTNService(clusterservice.NewSelector(),
		func(store metadata.TNService) bool {
			for _, shard := range store.Shards {
				target = metadata.TNShard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: shard.ShardID,
					},
					ReplicaID: shard.ReplicaID,
					Address:   store.TxnServiceAddress,
				}
				return true
			}
			return true
		})

	return txnOp.Write(ctx, []txn.TxnRequest{{
		CNRequest: &txn.CNOpRequest{
			OpCode:  uint32(api.OpCode_OpCommitMerge),
			Payload: payload,
			Target:  target,
		},
		Options: &txn.TxnRequestOptions{
			RetryCodes: []int32{
				// tn shard not found
				int32(moerr.ErrTNShardNotFound),
			},
			RetryInterval: int64(time.Second),
		},
	}})
}
