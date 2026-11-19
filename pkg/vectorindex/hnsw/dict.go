// Copyright 2026 Matrix Origin
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

package hnsw

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// DictBoundsSql writes the range of an int8/uint8 index into its dict table.
func DictBoundsSql(db, dict string, bounds QuantizeBounds) string {
	key, val := sqlquote.Ident(catalog.Hnsw_TblCol_Dict_Key), sqlquote.Ident(catalog.Hnsw_TblCol_Dict_Val)
	return fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (%s, %s), (%s, %s) ON DUPLICATE KEY UPDATE %s = VALUES(%s)",
		sqlquote.QualifiedIdent(db, dict), key, val,
		sqlquote.String(catalog.Hnsw_Dict_QuantizeMin), sqlquote.String(strconv.FormatFloat(bounds.Min, 'g', -1, 64)),
		sqlquote.String(catalog.Hnsw_Dict_QuantizeMax), sqlquote.String(strconv.FormatFloat(bounds.Max, 'g', -1, 64)),
		val, val)
}

// LoadQuantizeBounds reads the range of an int8/uint8 index from its dict table. It is unset
// when the index has no dict table or the table holds no range yet.
func LoadQuantizeBounds(sqlproc *sqlexec.SqlProcess, db, dict string) (QuantizeBounds, error) {
	if dict == "" {
		return QuantizeBounds{}, nil
	}
	key, val := sqlquote.Ident(catalog.Hnsw_TblCol_Dict_Key), sqlquote.Ident(catalog.Hnsw_TblCol_Dict_Val)
	sql := fmt.Sprintf("SELECT %s, CAST(%s AS DOUBLE) FROM %s WHERE %s IN (%s, %s)",
		key, val, sqlquote.QualifiedIdent(db, dict), key,
		sqlquote.String(catalog.Hnsw_Dict_QuantizeMin), sqlquote.String(catalog.Hnsw_Dict_QuantizeMax))
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return QuantizeBounds{}, err
	}
	defer res.Close()

	var bounds QuantizeBounds
	var hasMin, hasMax bool
	for _, bat := range res.Batches {
		for i := 0; i < bat.RowCount(); i++ {
			if bat.Vecs[1].IsNull(uint64(i)) {
				continue
			}
			v := vector.GetFixedAtWithTypeCheck[float64](bat.Vecs[1], i)
			switch bat.Vecs[0].GetStringAt(i) {
			case catalog.Hnsw_Dict_QuantizeMin:
				bounds.Min, hasMin = v, true
			case catalog.Hnsw_Dict_QuantizeMax:
				bounds.Max, hasMax = v, true
			}
		}
	}
	bounds.Set = hasMin && hasMax
	return bounds, nil
}
