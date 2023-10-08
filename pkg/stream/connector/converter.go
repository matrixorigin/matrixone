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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Converter interface {
	Convert(context.Context, RawObject) (string, error)
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

func (c *SQLConverter) Convert(ctx context.Context, obj RawObject) (string, error) {
	fields, values, err := c.fieldValueSQL(ctx, obj)
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES(%s)",
		c.dbName, c.tableName, fields, values)
	return s, nil
}

func (c *SQLConverter) fieldValueSQL(ctx context.Context, obj RawObject) (string, string, error) {
	var field, value strings.Builder
	first := true
	writeField := func(s string) {
		if first {
			field.WriteString(s)
		} else {
			field.WriteString(", ")
			field.WriteString(s)
		}
	}
	writeValue := func(s string) {
		if first {
			value.WriteString(s)
		} else {
			value.WriteString(", ")
			value.WriteString(s)
		}
	}
	for k, v := range obj {
		writeField(k)
		switch vv := v.(type) {
		case int:
			writeValue(strconv.FormatInt(int64(vv), 10))
		case int32:
			writeValue(strconv.FormatInt(int64(vv), 10))
		case int64:
			writeValue(strconv.FormatInt(vv, 10))
		case float64:
			writeValue(strconv.FormatFloat(vv, 'g', -1, 64))
		case string:
			writeValue("'" + vv + "'")
		case bool:
			if vv {
				writeValue("1")
			} else {
				writeValue("0")
			}
		default:
			return "", "", moerr.NewErrUnsupportedDataType(ctx, vv)
		}
		first = false
	}
	return field.String(), value.String(), nil
}
