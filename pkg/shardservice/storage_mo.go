// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var (
	MetadataTableSQL = fmt.Sprintf(`create table %s.%s(
		table_id 		  bigint      unsigned primary key not null,
		account_id        bigint      unsigned not null,       
		policy            varchar(50)          not null,
		shard_count       int         unsigned not null,
		replica_count     int         unsigned not null,
		version           int         unsigned not null
	)`, catalog.MO_CATALOG, catalog.MOShardsMetadata)

	ShardsTableSQL = fmt.Sprintf(`create table %s.%s(
		table_id 		  bigint unsigned     not null,
		shard_id          bigint unsigned     not null,
		policy            varchar(50)         not null
	)`, catalog.MO_CATALOG, catalog.MOShards)

	InitSQLs = []string{
		MetadataTableSQL,
		ShardsTableSQL,
	}
)

type storage struct {
	clock    clock.Clock
	executor executor.SQLExecutor
	waiter   client.TimestampWaiter
	handles  map[int]ReadFunc
	engine   engine.Engine
}

func NewShardStorage(
	clock clock.Clock,
	executor executor.SQLExecutor,
	waiter client.TimestampWaiter,
	handles map[int]ReadFunc,
	engine engine.Engine,
) ShardStorage {
	return &storage{
		clock:    clock,
		executor: executor,
		waiter:   waiter,
		handles:  handles,
		engine:   engine,
	}
}

func (s *storage) Get(
	table uint64,
) (uint64, pb.ShardsMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	now, _ := s.clock.Now()
	var metadata pb.ShardsMetadata
	shardTableID := table
	err := s.executor.ExecTxn(
		ctx,
		func(
			txn executor.TxnExecutor,
		) error {
			if err := readMetadata(
				shardTableID,
				txn,
				&metadata,
			); err != nil {
				return err
			}

			// For partition table, the origin table id is the shard table id, and
			// the partition table id is the shard id.
			//
			// For normal table, the origin table id is the shard table id, and the
			// shard id is a int value that calculated by the shard policy.
			//
			// Metadata is not found by the special table id means the table is not
			// sharding table or is a partition table. We need use the table id as
			// shard id to get sharding metadata.
			if metadata.IsEmpty() {
				v, err := getTableIDByShardID(
					table,
					pb.Policy_Partition.String(),
					txn,
				)
				if err != nil || v == 0 {
					return err
				}
				shardTableID = v

				if err := readMetadata(
					shardTableID,
					txn,
					&metadata,
				); err != nil {
					return err
				}
			}

			if !metadata.IsEmpty() {
				return readShards(
					shardTableID,
					txn,
					&metadata,
				)
			}

			return nil
		},
		executor.Options{}.WithMinCommittedTS(now),
	)
	if err != nil {
		return 0, pb.ShardsMetadata{}, err
	}
	return shardTableID, metadata, nil
}

func (s *storage) GetChanged(
	tables map[uint64]uint32,
	applyDeleted func(uint64),
	applyChanged func(uint64),
) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	targets := make([]string, 0, len(tables))
	for table := range tables {
		targets = append(targets, fmt.Sprintf("%d", table))
	}

	current := make(map[uint64]uint32)
	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(
			txn executor.TxnExecutor,
		) error {
			res, err := txn.Exec(
				getCheckMetadataSQL(targets),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(
				func(
					rows int,
					cols []*vector.Vector,
				) bool {
					ids := executor.GetFixedRows[uint64](cols[0])
					versions := executor.GetFixedRows[uint32](cols[1])
					for i := 0; i < rows; i++ {
						current[ids[i]] = versions[i]
					}
					return true
				},
			)
			return nil
		},
		executor.Options{}.WithMinCommittedTS(now),
	)
	if err != nil {
		return err
	}

	for table, version := range tables {
		new, ok := current[table]
		if !ok {
			applyDeleted(table)
			continue
		}
		if new > version {
			applyChanged(table)
		}
	}
	return nil
}

func (s *storage) Create(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) (bool, error) {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}

	created := false
	err = s.executor.ExecTxn(
		ctx,
		func(
			txn executor.TxnExecutor,
		) error {
			// Currently we only support partition policy.
			// If the current table is a non partition
			// table, we should not create sharding metadata
			// for the current table.
			partitions, err := readPartitionIDs(
				table,
				txn,
			)
			if err != nil ||
				len(partitions) == 0 {
				return err
			}

			created = true
			metadata := pb.ShardsMetadata{
				Policy:          pb.Policy_Partition,
				ShardsCount:     uint32(len(partitions)),
				AccountID:       uint64(accountID),
				ShardIDs:        partitions,
				Version:         1,
				MaxReplicaCount: 1,
			}

			return execSQL(
				getCreateSQLs(table, metadata),
				txn,
			)
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithDisableIncrStatement(),
	)
	return created, err

}

func (s *storage) Delete(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) (bool, error) {
	deleted := false
	err := s.executor.ExecTxn(
		ctx,
		func(
			txn executor.TxnExecutor,
		) error {
			var metadata pb.ShardsMetadata
			if err := readMetadata(
				table,
				txn,
				&metadata,
			); err != nil {
				return err
			}
			if metadata.Policy == pb.Policy_None {
				return nil
			}

			deleted = true
			return execSQL(
				[]string{
					getDeleteMetadataSQL(table),
					getDeleteShardsSQL(table),
				},
				txn,
			)
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithDisableIncrStatement(),
	)
	return deleted, err
}

func (s *storage) WaitLogAppliedAt(
	ctx context.Context,
	ts timestamp.Timestamp,
) error {
	_, err := s.waiter.GetTimestamp(ctx, ts)
	return err
}

func (s *storage) Read(
	ctx context.Context,
	shard pb.TableShard,
	method int,
	param pb.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	fn, ok := s.handles[method]
	if !ok {
		panic(fmt.Sprintf("method not found: %d", method))
	}

	return fn(
		ctx,
		shard,
		s.engine,
		param,
		ts,
		buffer,
	)
}

func (s *storage) Unsubscribe(
	tables ...uint64,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	var err error
	for _, tid := range tables {
		if e := s.engine.UnsubscribeTable(ctx, 0, tid); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

func readMetadata(
	table uint64,
	txn executor.TxnExecutor,
	metadata *pb.ShardsMetadata,
) error {
	res, err := txn.Exec(
		getMetadataSQL(table),
		executor.StatementOption{},
	)
	if err != nil {
		return err
	}
	defer res.Close()

	res.ReadRows(
		func(
			rows int,
			cols []*vector.Vector,
		) bool {
			metadata.AccountID = executor.GetFixedRows[uint64](cols[1])[0]
			metadata.Policy = pb.Policy(pb.Policy_value[cols[2].GetStringAt(0)])
			metadata.ShardsCount = executor.GetFixedRows[uint32](cols[3])[0]
			metadata.MaxReplicaCount = executor.GetFixedRows[uint32](cols[4])[0]
			metadata.Version = executor.GetFixedRows[uint32](cols[5])[0]
			return false
		},
	)

	return nil
}

func readShards(
	table uint64,
	txn executor.TxnExecutor,
	metadata *pb.ShardsMetadata,
) error {
	res, err := txn.Exec(
		getShardsSQL(table),
		executor.StatementOption{},
	)
	if err != nil {
		return err
	}
	defer res.Close()

	var shardIDs []uint64
	res.ReadRows(
		func(
			rows int,
			cols []*vector.Vector,
		) bool {
			shardIDs = append(shardIDs, executor.GetFixedRows[uint64](cols[0])...)
			return true
		},
	)
	metadata.ShardIDs = shardIDs
	return nil
}

func getTableIDByShardID(
	shardID uint64,
	policy string,
	txn executor.TxnExecutor,
) (uint64, error) {
	res, err := txn.Exec(
		getTableIDByShardSQL(shardID, policy),
		executor.StatementOption{},
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	var tableID uint64
	res.ReadRows(
		func(
			rows int,
			cols []*vector.Vector,
		) bool {
			tableID = executor.GetFixedRows[uint64](cols[0])[0]
			return true
		},
	)
	return tableID, nil
}

func readPartitionIDs(
	table uint64,
	txn executor.TxnExecutor,
) ([]uint64, error) {
	res, err := txn.Exec(
		getPartitionsSQL(table),
		executor.StatementOption{},
	)
	if err != nil {
		return nil, err
	}
	var names []string
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			names = append(names, executor.GetStringRows(cols[0])...)
			return true
		},
	)
	res.Close()

	if len(names) == 0 {
		return nil, err
	}

	res, err = txn.Exec(
		getTableIDsSQL(names),
		executor.StatementOption{},
	)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			ids = append(ids, executor.GetFixedRows[uint64](cols[0])...)
			return true
		},
	)
	res.Close()
	return ids, nil
}

func execSQL(
	sql []string,
	txn executor.TxnExecutor,
) error {
	for _, s := range sql {
		res, err := txn.Exec(
			s,
			executor.StatementOption{},
		)
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

func getMetadataSQL(
	table uint64,
) string {
	return fmt.Sprintf(
		"select table_id, account_id, policy, shard_count, replica_count, version from %s.%s where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MOShardsMetadata,
		table,
	)
}

func getCheckMetadataSQL(
	tables []string,
) string {
	return fmt.Sprintf(
		"select table_id, version from %s.%s where table_id in (%s)",
		catalog.MO_CATALOG,
		catalog.MOShardsMetadata,
		strings.Join(tables, ","),
	)
}

func getDeleteMetadataSQL(
	table uint64,
) string {
	return fmt.Sprintf(
		"delete from %s.%s where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MOShardsMetadata,
		table,
	)
}

func getShardsSQL(
	table uint64,
) string {
	return fmt.Sprintf(
		"select shard_id from %s.%s where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MOShards,
		table,
	)
}

func getTableIDByShardSQL(
	shardID uint64,
	policy string,
) string {
	return fmt.Sprintf(
		"select table_id from %s.%s where shard_id = %d and policy = '%s'",
		catalog.MO_CATALOG,
		catalog.MOShards,
		shardID,
		policy,
	)
}

func getDeleteShardsSQL(
	table uint64,
) string {
	return fmt.Sprintf(
		"delete from %s.%s where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MOShards,
		table,
	)
}

func getPartitionsSQL(
	table uint64,
) string {
	return fmt.Sprintf("select partition_table_name from %s.%s where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MO_TABLE_PARTITIONS,
		table,
	)
}

func getTableIDsSQL(
	names []string,
) string {
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, fmt.Sprintf("'%s'", name))
	}

	return fmt.Sprintf("select rel_id from %s.%s where relname in (%s)",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		strings.Join(values, ","),
	)
}

func getCreateSQLs(
	table uint64,
	metadata pb.ShardsMetadata,
) []string {
	values := make([]string, 0, metadata.ShardsCount+1)
	values = append(values,
		fmt.Sprintf(
			`insert into %s.%s (table_id, account_id, policy, shard_count, replica_count, version) 
			 values (%d, %d, '%s', %d, %d, %d)`,
			catalog.MO_CATALOG,
			catalog.MOShardsMetadata,
			table,
			metadata.AccountID,
			metadata.Policy.String(),
			metadata.ShardsCount,
			metadata.MaxReplicaCount,
			metadata.Version,
		),
	)

	for _, id := range metadata.ShardIDs {
		values = append(values,
			fmt.Sprintf(
				"insert into %s.%s (table_id, shard_id, policy) values (%d, %d, '%s')",
				catalog.MO_CATALOG,
				catalog.MOShards,
				table,
				id,
				metadata.Policy.String(),
			),
		)
	}
	return values
}
