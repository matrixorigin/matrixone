// Copyright 2021 - 2023 Matrix Origin
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

package moconnector

import (
	"context"
	"fmt"
	"strings"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type Converter interface {
	Convert(context.Context, ie.InternalExecResult) (string, error)
}

type SQLConverter struct {
	dbName    string
	tableName string
}

func newSQLConverter(dbName, tableName string) Converter {
	return &SQLConverter{
		dbName:    dbName,
		tableName: tableName,
	}
}

func (c *SQLConverter) Convert(ctx context.Context, obj ie.InternalExecResult) (string, error) {

	columnCount := int(obj.ColumnCount())
	rowCount := int(obj.RowCount())

	var fields, values string
	var colNames []string

	for i := 0; i < columnCount; i++ {
		name, _, _, err := obj.Column(ctx, uint64(i))
		if err != nil {
			return "", err // Handle the error appropriately
		}

		if strings.Contains(name, ".") {
			parts := strings.SplitN(name, ".", 2)
			fields += "`" + parts[0] + "`.`" + parts[1] + "`"
		} else {
			fields += "`" + name + "`"
		}

		if i < columnCount-1 {
			fields += ", "
		}
		colNames = append(colNames, name)
	}
	for i := 0; i < rowCount; i++ {
		var rowValues string
		var err error
		for j := 0; j < columnCount; j++ {
			var val string
			val, err = obj.StringValueByName(ctx, uint64(i), colNames[j])
			// Enclose the value in single quotes if it is a string
			if err != nil {
				rowValues += "NULL"
			} else {
				rowValues += "'" + val + "'"
			}
			if j < columnCount-1 {
				rowValues += ", "
			}
		}

		if i > 0 && len(values) > 0 {
			values += ", "
		}
		values += fmt.Sprintf("(%s)", rowValues)
	}
	s := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s ",
		c.dbName, c.tableName, fields, values)
	return s, nil
}
