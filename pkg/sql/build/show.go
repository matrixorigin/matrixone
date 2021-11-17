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

package build

import (
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showColumns"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showDatabases"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showTables"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
)

func (b *build) buildShowTables(stmt *tree.ShowTables) (op.OP, error) {
	var likeStr []byte
	if len(stmt.DBName) == 0 {
		stmt.DBName = b.db
	}
	db, err := b.e.Database(stmt.DBName)
	if err != nil {
		return nil, sqlerror.New(errno.InvalidSchemaName, err.Error())
	}
	if stmt.Where != nil {

	}
	if stmt.Like != nil {
		likeStr = []byte(stmt.Like.Right.String())
	}
	return showTables.New(db, likeStr), nil
}

func (b *build) buildShowDatabases(stmt *tree.ShowDatabases) (op.OP, error) {
	var likeStr []byte
	// semantic analysis
	if stmt.Where != nil {

	}
	if stmt.Like != nil {
		likeStr = []byte(stmt.Like.Right.String())
	}

	return showDatabases.New(b.e, likeStr), nil
}

func (b *build) buildShowColumns(stmt *tree.ShowColumns) (op.OP, error) {
	tn := stmt.Table.ToTableName()
	_, _, r, err := b.tableName(&tn)
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {

	}
	var likeStr []byte
	if stmt.Like != nil {
		likeStr = []byte(stmt.Like.Right.String())
	}
	return showColumns.New(r, likeStr), nil
}
