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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Relation = new(table)

func (tbl *table) Rows() int64 {
	return 0
}

func (tbl *table) Size(name string) int64 {
	return 0
}

func (tbl *table) Ranges(ctx context.Context) ([][]byte, error) {
	return nil, nil
}

func (tbl *table) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return nil, nil
}

func (tbl *table) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (tbl *table) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (tbl *table) Write(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (tbl *table) Update(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (tbl *table) Delete(ctx context.Context, vec *vector.Vector, name string) error {
	return nil
}

func (tbl *table) Truncate(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (tbl *table) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *table) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *table) GetTableID(ctx context.Context) string {
	return strconv.FormatUint(tbl.tableId, 10)
}

func (tbl *table) NewReader(ctx context.Context, num int, expr *plan.Expr,
	ranges [][]byte) ([]engine.Reader, error) {
	return nil, nil
}
