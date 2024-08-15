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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
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

	target := getTNShard(proc.GetService())
	fs := proc.GetFileService()
	mergeAndWrite := func(rel engine.Relation, stats []objectio.ObjectStats) (*rpc.SendResult, error) {
		entry, err := rel.MergeObjects(ctx, stats, uint32(a.targetObjSize))
		if err != nil {
			merge.CleanUpUselessFiles(entry, fs)
			if entry == nil {
				entry = &api.MergeCommitEntry{
					Err: err.Error(),
				}
			}
		}

		payload, err := entry.MarshalBinary()
		if err != nil {
			return nil, err
		}

		return txnWrite(ctx, target, txnOp, payload)
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

		resp, err := mergeAndWrite(rel, a.objs)
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
			logutil.Errorf("mergeblocks err on cn, table %s, err %s", a.tbl, err.Error())
			return Result{}, err
		}

		partitionInfo, err := getRelPartitionInfo(ctx, rel)
		if err != nil {
			logutil.Errorf("mergeblocks err on cn, table %s, err %s", a.tbl, err.Error())
			return Result{}, err
		}

		if partitionInfo == nil {
			stats, err := rel.GetNonAppendableObjectStats(ctx)
			if err != nil {
				return Result{}, err
			}
			if a.filter != "" {
				buffer := new(bytes.Buffer)
				var errOut error
				hasSuccess := false
				round := 0
				for {
					round++
					var ss []objectio.ObjectStats
					ss, stats, err = applyMergePolicy(ctx, a.filter, getSortKeyPos(ctx, rel), stats)
					if err != nil {
						errOut = errors.Join(errOut, err)
						break
					}
					resp, err := mergeAndWrite(rel, ss)
					if err != nil {
						errOut = errors.Join(errOut, err)
						break
					}
					buffer.WriteString(string(resp.Responses[0].CNOpResponse.Payload))
					buffer.WriteString("\n")
					resp.Release()
					hasSuccess = true
					logutil.Info("[CN-MERGING]",
						zap.String("table", rel.GetTableName()),
						zap.String("policy", a.filter),
						zap.Int("round", round),
						zap.Int("objects length", len(ss)),
						zap.Int("remain objects", len(stats)),
					)
				}

				if !hasSuccess {
					return Result{}, errOut
				}
				return Result{
					Method: MergeObjectsMethod,
					Data:   buffer.Bytes(),
				}, nil
			}

			slicedStats := sliceStats(stats)
			buffer := new(bytes.Buffer)
			var errOut error
			hasSuccess := false
			mergedStats := 0
			for _, ss := range slicedStats {
				mergedStats += len(ss)
				resp, err := mergeAndWrite(rel, ss)
				if err != nil {
					errOut = errors.Join(errOut, err)
					continue
				}
				buffer.WriteString(string(resp.Responses[0].CNOpResponse.Payload))
				buffer.WriteString("\n")
				resp.Release()
				hasSuccess = true
				logutil.Info("[CN-MERGING]",
					zap.String("table", rel.GetTableName()),
					zap.Int("merged objects", mergedStats),
					zap.Int("total objects", len(stats)),
				)
			}
			if !hasSuccess {
				return Result{}, errOut
			}
			return Result{
				Method: MergeObjectsMethod,
				Data:   buffer.Bytes(),
			}, nil
		}

		// check if the current table is partitioned
		var prel engine.Relation
		var buffer bytes.Buffer
		var errOut error
		hasSuccess := false
		// for partition table, run merge on each partition table separately.
		for _, partitionTable := range partitionInfo.PartitionTableNames {
			prel, err = database.Relation(ctx, partitionTable, nil)
			if err != nil {
				return Result{}, err
			}
			stats, err := prel.GetNonAppendableObjectStats(ctx)
			if err != nil {
				return Result{}, err
			}
			if a.filter != "" {
				round := 0
				for {
					round++
					var ss []objectio.ObjectStats
					ss, stats, err = applyMergePolicy(ctx, a.filter, getSortKeyPos(ctx, rel), stats)
					if err != nil {
						errOut = errors.Join(errOut, err)
						break
					}
					resp, err := mergeAndWrite(prel, ss)
					if err != nil {
						errOut = errors.Join(errOut, err)
						break
					}
					buffer.WriteString(string(resp.Responses[0].CNOpResponse.Payload))
					buffer.WriteString("\n")
					resp.Release()
					hasSuccess = true
					logutil.Info("[CN-MERGING]",
						zap.String("table", prel.GetTableName()),
						zap.String("policy", a.filter),
						zap.Int("round", round),
						zap.Int("objects length", len(ss)),
						zap.Int("remain objects", len(stats)),
					)
				}
				continue
			}

			slicedStats := sliceStats(stats)
			mergedStats := 0
			for _, ss := range slicedStats {
				mergedStats += len(ss)
				resp, err := mergeAndWrite(prel, ss)
				if err != nil {
					errOut = errors.Join(errOut, err)
					continue
				}
				buffer.WriteString(string(resp.Responses[0].CNOpResponse.Payload))
				buffer.WriteString("\n")
				resp.Release()
				hasSuccess = true
				logutil.Info("[CN-MERGING]",
					zap.String("table", rel.GetTableName()),
					zap.Int("merged objects", mergedStats),
					zap.Int("total objects", len(stats)),
				)
			}
		}
		if !hasSuccess {
			return Result{}, errOut
		}
		return Result{
			Method: MergeObjectsMethod,
			Data:   buffer.Bytes(),
		}, nil
	}
	return Result{}, nil
}

func getTNShard(service string) metadata.TNShard {
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
	return target
}

// Each merge process merges at most ~10^7 rows.
// For transfer table, 10^7 rows is 12 * 10^7 ~= 120MB size.
const slicedRowCnt = 10_000_000

func sliceStats(stats []objectio.ObjectStats) [][]objectio.ObjectStats {
	rows := uint32(0)
	slicedSize := make([]int, 1)
	i := 0
	for _, stat := range stats {
		rows += stat.Rows()
		slicedSize[i]++
		if rows > slicedRowCnt {
			i++
			slicedSize = append(slicedSize, 0)
			rows = 0
		}
	}

	slicedStats := make([][]objectio.ObjectStats, len(slicedSize))
	i = 0
	for _, stat := range stats {
		slicedStats[i] = append(slicedStats[i], stat)
		if len(slicedStats) == slicedSize[i] {
			i++
		}
	}
	return slicedStats
}

func getRelPartitionInfo(ctx context.Context, rel engine.Relation) (*plan.PartitionByDef, error) {
	engineDefs, err := rel.TableDefs(ctx)
	if err != nil {
		return nil, err
	}
	var partitionInfo *plan.PartitionByDef
	for _, def := range engineDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(util.UnsafeStringToBytes(partitionDef.Partition))
				if err != nil {
					return nil, err
				}
				partitionInfo = p
			}
		}
	}
	return partitionInfo, nil
}

func txnWrite(ctx context.Context, target metadata.TNShard, txnOp client.TxnOperator, payload []byte) (*rpc.SendResult, error) {
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

func applyMergePolicy(ctx context.Context, policyName string, sortKeyPos int, objStats []objectio.ObjectStats) ([]objectio.ObjectStats, []objectio.ObjectStats, error) {
	arg := cutBetween(policyName, "(", ")")
	if strings.HasPrefix(policyName, "small") {
		size := uint32(110 * common.Const1MBytes)
		i, err := units.RAMInBytes(arg)
		if err == nil && 10*common.Const1MBytes < i && i < 250*common.Const1MBytes {
			size = uint32(i)
		}
		selectedObjs, remainObjs := NewSmall(size).Filter(objStats)
		return selectedObjs, remainObjs, nil
	} else if strings.HasPrefix(policyName, "overlap") {
		if sortKeyPos == -1 {
			return objStats, nil, nil
		}
		maxObjects := 10
		i, err := strconv.Atoi(arg)
		if err == nil {
			maxObjects = i
		}
		selectedObjs, remainObjs := NewOverlap(maxObjects).Filter(objStats)
		return selectedObjs, remainObjs, nil
	}

	return nil, nil, moerr.NewInvalidInput(ctx, "invalid merge policy name")
}

func cutBetween(s, start, end string) string {
	i := strings.Index(s, start)
	if i >= 0 {
		j := strings.Index(s[i:], end)
		if j >= 0 {
			return s[i+len(start) : i+j]
		}
	}
	return ""
}

func getSortKeyPos(ctx context.Context, rel engine.Relation) int {
	def := rel.GetTableDef(ctx)
	cols := def.Cols
	for i, col := range cols {
		if col.Primary && col.Name != catalog.FakePrimaryKeyColName {
			if col.ClusterBy {
				panic("bad schema")
			}
			return i
		} else if col.ClusterBy {
			return i
		}
	}
	return -1
}
