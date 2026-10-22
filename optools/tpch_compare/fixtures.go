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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
)

func qualifyFixtures(ctx context.Context, conn *sql.Conn, root string) (retErr error) {
	query := Query{Columns: []Column{{Kind: "int"}, {Kind: "decimal"}, {Kind: "text"}, {Kind: "date"}}, Statements: []string{"SELECT 1,CAST('1234567890123.45' AS DECIMAL(15,2)),' x ',CAST('2020-02-29' AS DATE) UNION ALL SELECT 2,NULL,NULL,NULL ORDER BY 1"}, Order: []OrderKey{{Column: 0}}}
	cap, err := executeUnit(ctx, conn, query, filepath.Join(root, "boundary.rows"), captureDisk)
	if err != nil {
		return err
	}
	reader, err := openRows(filepath.Join(root, cap.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, reader.close()) }()
	want := []Row{{{Value: "1"}, {Value: "1234567890123.45"}, {Value: " x "}, {Value: "2020-02-29"}}, {{Value: "2"}, {Null: true}, {Null: true}, {Null: true}}}
	for _, expected := range want {
		got, err := reader.next()
		if err != nil {
			return err
		}
		if !slices.Equal(got, expected) {
			return fmt.Errorf("independent typed boundary fixture differs: %v", got)
		}
	}
	if _, err := reader.next(); !errors.Is(err, io.EOF) {
		return errors.New("independent fixture has extra rows")
	}
	q := Query{Columns: []Column{{Kind: "approximate"}}, Statements: []string{"SELECT CAST('0.1' AS DECIMAL(15,2))/CAST('3' AS DECIMAL(15,2))"}}
	cap, err = executeUnit(ctx, conn, q, filepath.Join(root, "division.rows"), captureDisk)
	if err != nil {
		return err
	}
	rows, err := openRows(filepath.Join(root, cap.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rows.close()) }()
	row, err := rows.next()
	if err != nil {
		return err
	}
	ok, err := equalCell(row[0], Cell{Value: "1/30"}, q.Columns[0], cap.Columns[0], PhysicalColumn{Type: "DOUBLE"})
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("division differs from independent rational reference")
	}
	return nil
}
