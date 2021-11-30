// Copyright 2021 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *build) BuildShowTables(stmt *tree.ShowTables, plan *ShowTables) error {
	if len(stmt.DBName) == 0 {
		stmt.DBName = b.db
	}
	db, err := b.e.Database(stmt.DBName)
	if err != nil {
		return errors.New(errno.InvalidSchemaName, err.Error())
	}
	if stmt.Where != nil {
		return errors.New(errno.FeatureNotSupported, "not support where for show tables")
	}
	if stmt.Like != nil {
		plan.Like = []byte(stmt.Like.Right.String())
	}
	plan.Db = db
	return nil
}