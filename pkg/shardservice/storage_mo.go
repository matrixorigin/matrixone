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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
		shard_id          bigint unsigned     not null  
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
}

func NewShardStorage(
	clock clock.Clock,
	executor executor.SQLExecutor,
	waiter client.TimestampWaiter,
) ShardStorage {
	return &storage{
		clock:    clock,
		executor: executor,
		waiter:   waiter,
	}
}

func (s *storage) Get(
	table uint64,
) (pb.ShardsMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	now, _ := s.clock.Now()
	var metadata pb.ShardsMetadata
	err := s.executor.ExecTxn(
		ctx,
		func(
			txn executor.TxnExecutor,
		) error {
			if err := readMetadata(
				table,
				txn,
				&metadata,
			); err != nil {
				return err
			}

			if err := readShards(
				table,
				txn,
				&metadata,
			); err != nil {
				return err
			}

			return nil
		},
		executor.Options{}.WithMinCommittedTS(now),
	)

	if err != nil {
		return pb.ShardsMetadata{}, err
	}
	return metadata, nil
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
	table uint64,
	payload []byte,
	ts timestamp.Timestamp,
) ([]byte, error) {
	// TODO: implement this
	return nil, nil
}

func (s *storage) Unsubscribe(
	tables ...uint64,
) error {
	return nil
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
				"insert into %s.%s (table_id, shard_id) values (%d, %d)",
				catalog.MO_CATALOG,
				catalog.MOShards,
				table,
				id,
			),
		)
	}
	return values
}
