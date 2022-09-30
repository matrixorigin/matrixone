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

package parsers

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/postgresql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Parse(dialectType dialect.DialectType, sql string) ([]tree.Statement, error) {
	switch dialectType {
	case dialect.MYSQL:
		return mysql.Parse(sql)
	case dialect.POSTGRESQL:
		return postgresql.Parse(sql)
	default:
		return nil, moerr.NewInternalError("type of dialect error")
	}
}

func ParseOne(dialectType dialect.DialectType, sql string) (tree.Statement, error) {
	switch dialectType {
	case dialect.MYSQL:
		return mysql.ParseOne(sql)
	case dialect.POSTGRESQL:
		return postgresql.ParseOne(sql)
	default:
		return nil, moerr.NewInternalError("type of dialect error")
	}
}
