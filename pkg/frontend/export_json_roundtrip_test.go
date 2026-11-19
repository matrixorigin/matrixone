// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/stretchr/testify/require"
)

func TestJSONOutfileRoundTripsThroughLoadParser(t *testing.T) {
	const document = `{"line":"a\nb\tc","quote":"\\\" and \\\",","unicode":"世界"}`
	jsonValue, err := types.ParseStringToByteJson(document)
	require.NoError(t, err)
	encoded, err := jsonValue.Marshal()
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		quote byte
	}{
		{name: "unenclosed"},
		{name: "enclosed", quote: '"'},
	} {
		t.Run(tc.name, func(t *testing.T) {
			quote := tc.quote
			var enclosure string
			if quote != 0 {
				enclosure = string(quote)
			}
			mp := mpool.MustNewZero()
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
			defer func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()) }()
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], encoded, false, mp))
			bat.SetRowCount(1)
			ep := &ExportConfig{
				userConfig: &tree.ExportParam{Fields: tree.NewFields(",", false, quote, '\\'), Lines: tree.NewLines("", "\n")},
				Symbol:     [][]byte{[]byte("\n")}, ColumnFlag: []bool{true},
			}
			ch := make(chan *BatchByte, 1)
			constructByte(context.Background(), &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
			result := <-ch
			require.NoError(t, result.err)
			parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
				FieldsTerminatedBy: ",", FieldsEnclosedBy: enclosure, FieldsEscapedBy: "\\", LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
			}, bytes.NewReader(result.writeByte), csvparser.ReadBlockSize, false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.False(t, row[0].IsNull)
			require.Equal(t, jsonValue.String(), row[0].Val)
		})
	}
}
