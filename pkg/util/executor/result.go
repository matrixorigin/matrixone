// Copyright 2023 Matrix Origin
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

package executor

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (res Result) Close() {
	for _, rows := range res.Batches {
		rows.Clean(res.mp)
	}
}

// ReadRows read all rows, apply is used to read cols data in a row. If apply return
// false, stop reading. If the query has a lot of data, apply will be called multiple times, giving
// a batch of rows for each call.
func (res Result) ReadRows(apply func(rows int, cols []*vector.Vector) bool) {
	for _, rows := range res.Batches {
		if !apply(rows.RowCount(), rows.Vecs) {
			return
		}
	}
}

// GetFixedRows get fixed rows, int, float, etc.
func GetFixedRows[T any](vec *vector.Vector) []T {
	return vector.MustFixedCol[T](vec)
}

// GetBytesRows get bytes rows, varchar, varbinary, text, json, etc.
func GetBytesRows(vec *vector.Vector) [][]byte {
	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	rows := make([][]byte, 0, n)
	for idx := range data {
		rows = append(rows, data[idx].GetByteSlice(area))
	}
	return rows
}

// GetStringRows get bytes rows, varchar, varbinary, text, json, etc.
func GetStringRows(vec *vector.Vector) []string {
	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	rows := make([]string, 0, n)
	for idx := range data {
		rows = append(rows, string(data[idx].GetByteSlice(area)))
	}
	return rows
}
