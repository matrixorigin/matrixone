package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (tbl *txnTable) isShardingTable() (bool, error) {
	var err error
	tbl.shardInfo.init.Do(func() {
		shardTableID, policy, ok, e := shardservice.GetService().GetShardInfo(tbl.tableId)
		if err != nil {
			err = e
			return
		}

		tbl.shardInfo.shardTableID = shardTableID
		tbl.shardInfo.shardPolicy = policy
		tbl.shardInfo.sharding = ok
	})
	if err != nil {
		return false, err
	}
	return tbl.shardInfo.sharding, nil
}

func (tbl *txnTable) readShardingRanges(
	ctx context.Context,
	exprs []*plan.Expr,
) (engine.Ranges, error) {
	if !tbl.shardInfo.sharding {
		panic("BUG: cannot call getShardingRanges on non-sharding table")
	}

	opts := shardservice.DefaultOptions
	if tbl.shardInfo.shardPolicy == shard.Policy_Partition {
		opts = opts.Shard(tbl.tableId)
	}

	var ranges engine.Ranges
	err := shardservice.GetService().Read(
		ctx,
		shardservice.ReadRequest{
			TableID: tbl.shardInfo.shardTableID,
			Method:  shardservice.ReadRanges,
			Data:    encodeRangesPayload(tbl, exprs),
			Apply: func(value []byte) {
				ranges.Append(value)
			},
		},
		opts,
	)
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func handleReadRanges(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	payload []byte,
	ts timestamp.Timestamp,
) ([]byte, error) {
	// before handleReadRanges called, the latest log tail apply ts must > ts

	value := decodeRangesPayload(payload)
	e := engine.(*Engine)
	part, new := e.getOrCreateLatestPart(
		value.databaseID,
		value.tableID,
	)
	// no partition state, need init and subscribe
	if !new {
		if err := e.UpdateOfPush(
			ctx,
			value.databaseID,
			value.tableID,
			ts,
		); err != nil {
			return nil, err
		}

		if _, err := e.lazyLoadLatestCkp(
			ctx,
			value.databaseID,
			value.databaseName,
			value.tableID,
			value.tableName,
			value.primarySeqnum,
		); err != nil {
			return nil, err
		}
	}

	snap := part.Snapshot()
	var blocks objectio.BlockInfoSlice
	ranges := &blocks

	blocks.AppendBlockInfo(objectio.EmptyBlockInfo)

	if err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.GetTableDef(ctx),
		exprs,
		&blocks,
		tbl.proc.Load(),
	); err != nil {
		return
	}
	return nil, nil
}

func handleReadData(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	payload []byte,
	ts timestamp.Timestamp,
) ([]byte, error) {
	return nil, nil
}

func encodeRangesPayload(
	tbl *txnTable,
	exprs []*plan.Expr,
) []byte {
	return nil
}

func decodeRangesPayload(
	value []byte,
) rangesPayload {
	return rangesPayload{}
}

func encodeRanges(
	ranges engine.Ranges,
) []byte {
	return nil
}

func decodeRanges(
	value []byte,
) engine.Ranges {
	return nil
}

type rangesPayload struct {
	databaseID    uint64
	databaseName  string
	tableID       uint64
	tableName     string
	primarySeqnum int
	exprs         []*plan.Expr
}
