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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newDB() *DB {
	return &DB{
		readTs: timestamp.Timestamp{
			PhysicalTime: 0,
			LogicalTime:  0,
		},
	}
}

func (db *DB) Update(ctx context.Context, dnList []DNStore,
	databaseId, tableId uint64, ts timestamp.Timestamp) error {
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
