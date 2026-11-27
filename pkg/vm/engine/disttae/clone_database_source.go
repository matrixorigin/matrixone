// Copyright 2025 Matrix Origin
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
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"golang.org/x/sync/errgroup"
)

// LoadDatabaseCloneSources reads the checkpoints and logtails for the tables
// of a live database clone. The database belongs to the clone's fixed source
// snapshot. Each reader still acquires and checks its partition state through
// the ordinary read path. Only the blocking I/O is scheduled in parallel here.
func (e *Engine) LoadDatabaseCloneSources(ctx context.Context, snapshot client.TxnOperator, dbName string, names []string) error {
	if len(names) < 2 {
		return nil
	}
	database, err := e.Database(ctx, dbName, snapshot)
	if err != nil {
		return err
	}
	relations := make([]*txnTable, 0, len(names))
	appendTable := func(name string) (*txnTable, error) {
		relation, err := database.Relation(ctx, name, nil)
		if err != nil {
			return nil, err
		}
		return cloneSourceTable(relation), nil
	}
	seen := make(map[uint64]struct{}, len(names))
	add := func(table *txnTable) {
		if _, found := seen[table.tableId]; !found {
			relations = append(relations, table)
			seen[table.tableId] = struct{}{}
		}
	}
	for _, name := range names {
		table, err := appendTable(name)
		if err != nil {
			return err
		}
		def := table.GetTableDef(ctx)
		if features.IsPartitioned(def.FeatureFlag) {
			// The partition service owns the physical child list.
			continue
		}
		add(table)
		for _, index := range def.Indexes {
			if !index.TableExist || index.IndexTableName == "" {
				continue
			}
			indexTable, err := appendTable(index.IndexTableName)
			if err != nil {
				return err
			}
			add(indexTable)
		}
	}

	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(16, 2*runtime.GOMAXPROCS(0)))
	for _, table := range relations {
		if readCtx.Err() != nil {
			break
		}
		group.Go(func() (err error) {
			defer func() {
				if failure := recover(); failure != nil {
					err = moerr.ConvertPanicError(readCtx, failure)
				}
			}()
			if err := readCtx.Err(); err != nil {
				return err
			}
			_, err = table.getPartitionState(readCtx)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func cloneSourceTable(relation engine.Relation) *txnTable {
	if delegate, ok := relation.(*txnTableDelegate); ok {
		return delegate.origin
	}
	return relation.(*txnTable)
}
