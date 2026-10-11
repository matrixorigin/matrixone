// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// drainResult owns every result-set path, including late errors and failed writes.
func drainResult(rows *sql.Rows, destination io.Writer, budget int64, started time.Time) (result resultDescriptor, firstRow *float64, err error) {
	defer func() { err = errors.Join(err, rows.Close()) }()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return result, firstRow, err
	}
	if len(columns) == 0 || len(columns) > 1024 {
		return result, firstRow, errors.New("invalid public column count")
	}
	schema := make([]publicColumn, len(columns))
	raw := make([]sql.RawBytes, len(columns))
	arguments := make([]any, len(columns))
	for index, column := range columns {
		entry := publicColumn{Name: column.Name(), SQLType: column.DatabaseTypeName()}
		entry.Nullable, entry.NullableKnown = column.Nullable()
		entry.Precision, entry.Scale, entry.DecimalKnown = column.DecimalSize()
		schema[index] = entry
		arguments[index] = &raw[index]
	}
	hasher := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(destination, hasher), 64<<10)
	var rowCount uint64
	var byteCount int64
	for rows.Next() {
		if firstRow == nil {
			first := time.Since(started).Seconds()
			firstRow = &first
		}
		if err = rows.Scan(arguments...); err != nil {
			return result, firstRow, err
		}
		values := make([]cell, len(columns))
		for index := range raw {
			values[index], err = encodeCell(schema[index], raw[index])
			if err != nil {
				return result, firstRow, err
			}
		}
		encoded, marshalErr := json.Marshal(values)
		if marshalErr != nil {
			return result, firstRow, marshalErr
		}
		if len(encoded) > maxRowBytes || int64(len(encoded)+1) > budget-byteCount {
			return result, firstRow, errors.New("row or spool quota exceeded")
		}
		if _, err = writer.Write(encoded); err != nil {
			return result, firstRow, err
		}
		if err = writer.WriteByte('\n'); err != nil {
			return result, firstRow, err
		}
		rowCount++
		byteCount += int64(len(encoded) + 1)
	}
	if err = rows.Err(); err != nil {
		return result, firstRow, err
	}
	if rows.NextResultSet() {
		return result, firstRow, errors.New("unexpected additional result set")
	}
	if err = rows.Err(); err != nil {
		return result, firstRow, err
	}
	if err = rows.Close(); err != nil {
		return result, firstRow, err
	}
	if err = writer.Flush(); err != nil {
		return result, firstRow, err
	}
	result = resultDescriptor{Schema: schema, Rows: rowCount, Bytes: byteCount, SHA256: hex.EncodeToString(hasher.Sum(nil))}
	return result, firstRow, nil
}
