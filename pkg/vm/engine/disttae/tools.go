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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func genCreateDatabaseTuple(name string, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	{
		bat.Vecs[0] = vector.New(catalog.MoDatabaseTypes[0]) // dat_id
		if err := bat.Vecs[0].Append(uint64(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[1] = vector.New(catalog.MoDatabaseTypes[1]) // datname
		if err := bat.Vecs[1].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[2] = vector.New(catalog.MoDatabaseTypes[2]) // dat_catalog_name
		if err := bat.Vecs[2].Append([]byte(catalog.MO_CATALOG), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[3] = vector.New(catalog.MoDatabaseTypes[3]) // dat_createsql
		if err := bat.Vecs[3].Append([]byte(fmt.Sprintf("create database %s", name)), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[4] = vector.New(catalog.MoDatabaseTypes[4]) // owner
		if err := bat.Vecs[4].Append(uint32(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[5] = vector.New(catalog.MoDatabaseTypes[5]) // creator
		if err := bat.Vecs[5].Append(uint32(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[6] = vector.New(catalog.MoDatabaseTypes[6]) // created_time
		if err := bat.Vecs[6].Append(types.Timestamp(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[7] = vector.New(catalog.MoDatabaseTypes[7]) // account_id
		if err := bat.Vecs[7].Append(uint32(0), false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genDropDatabaseTuple(name string) *batch.Batch {
	return &batch.Batch{}
}

func genCreateTableTuple(name string) *batch.Batch {
	return &batch.Batch{}
}

func genCreateColumnTuple(def engine.TableDef) *batch.Batch {
	return &batch.Batch{}
}

func genDropTableTuple(name string) *batch.Batch {
	return &batch.Batch{}
}

func genDropColumnsTuple(name string) *batch.Batch {
	return &batch.Batch{}
}

// genDatabaseIdExpr generate an expression to find database id
// by database name
func genDatabaseIdExpr(name string) *plan.Expr {
	return nil
}

// genTableIdExpr generate an expression to find table id
// by database id and table name
func genTableIdExpr(databaseId uint64, name string) *plan.Expr {
	return nil
}
