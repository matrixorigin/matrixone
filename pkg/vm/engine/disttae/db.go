// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newDB(cli client.TxnClient, dnList []DNStore) *DB {
	dnMap := make(map[string]int)
	for i := range dnList {
		dnMap[dnList[i].UUID] = i
	}
	db := &DB{
		cli:    cli,
		dnMap:  dnMap,
		tables: make(map[[2]uint64]Partitions),
	}
	return db
}

func (db *DB) getPartitions(databaseId, tableId uint64) Partitions {
	parts, ok := db.tables[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = new(Partition)
		}
		db.tables[[2]uint64{databaseId, tableId}] = parts
	}
	return parts
}

func (db *DB) Update(ctx context.Context, dnList []DNStore,
	databaseId, tableId uint64, ts timestamp.Timestamp) error {
	op, err := db.cli.New()
	if err != nil {
		return err
	}
	db.Lock()
	parts, ok := db.tables[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = new(Partition)
		}
		db.tables[[2]uint64{databaseId, tableId}] = parts
	}
	db.Unlock()
	for _, dn := range dnList {
		part := parts[db.dnMap[dn.UUID]]
		part.Lock()
		if part.ts.Less(ts) {
			if err := updatePartition(ctx, op, part.data, dn,
				genSyncLogTailReq(part.ts, ts, databaseId, tableId)); err != nil {
				part.Unlock()
				return err
			}
			part.ts = ts
		}
		part.Unlock()
	}
	return nil
}

func (db *DB) BlockList(ctx context.Context, dnList []DNStore,
	databaseId, tableId uint64, ts timestamp.Timestamp,
	entries [][]Entry) []BlockMeta {
	return nil
}

func (db *DB) NewReader(ctx context.Context, readNumber int, expr *plan.Expr,
	dnList []DNStore, databaseId, tableId uint64, ts timestamp.Timestamp,
	entires [][]Entry) ([]engine.Reader, error) {
	return nil, nil
}
