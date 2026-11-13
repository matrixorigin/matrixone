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

package vector

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// This bounded workload also permits test-only Go coverage counts to compare
// full-admission entries for 1/2/8 queries. No production counter is installed.
func TestJSONReaderResidentWork(t *testing.T) {
	for _, elements := range []int{16, 4096} {
		for _, queries := range []int{1, 2, 8} {
			for _, mode := range []string{"builder", "snapshot"} {
				t.Run(fmt.Sprintf("elements=%d/queries=%d/%s", elements, queries, mode), func(t *testing.T) {
					mp := mpool.MustNewZero()
					defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
					document := `{"keep":"selected","p1":2,"p2":true,"p3":null,"p4":[1],"p5":{"x":1},"p6":3.5,"p7":"tail","large":[` + strings.Repeat(`{"unused":"value","n":1},`, elements-1) + `{"unused":"value","n":1}]}`
					raw := jsonAdmissionValue(t, document)
					var reader JSONReader
					if mode == "builder" {
						builder, err := NewJSONReaderBuilder(mp, nil)
						require.NoError(t, err)
						defer builder.Close()
						require.NoError(t, builder.Append(raw, false))
						reader, err = builder.Finish()
						require.NoError(t, err)
					} else {
						source := NewVec(types.T_json.ToType())
						defer func() {
							if source != nil {
								source.Free(mp)
							}
						}()
						require.NoError(t, AppendBytes(source, raw, false, mp))
						var err error
						reader, err = NewJSONReaderSnapshot(source, mp)
						require.NoError(t, err)
						source.Free(mp)
						source = nil
					}
					defer reader.Close()
					cursor, err := reader.OpenCursor()
					require.NoError(t, err)
					defer cursor.Close()
					backing := mp.CurrNB()
					expected := []string{"selected", "2", "true", "null", "[1]", `{"x": 1}`, "3.5", "tail"}
					for index := 0; index < queries; index++ {
						key := "keep"
						if index > 0 {
							key = fmt.Sprintf("p%d", index)
						}
						selected, err := cursor.QueryText(0, []byte("$."+key), false)
						require.NoError(t, err)
						require.True(t, selected.Exists)
						require.False(t, selected.InputNull)
						require.False(t, selected.Multiple)
						require.False(t, selected.InvalidPath)
						if index == 3 {
							require.True(t, selected.ValueNull)
						} else {
							require.Equal(t, expected[index], selected.Text)
						}
						selected.Close()
						text, err := cursor.Text()
						require.NoError(t, err)
						require.Equal(t, expected[index], text.Text, "text selection must leave the raw query result unchanged")
						text.Close()
						require.Equal(t, backing, mp.CurrNB(), "resident queries must not allocate document backing")
					}
					t.Logf("document_bytes=%d reader_backing_bytes=%d queries=%d", len(raw), backing, queries)
				})
			}
		}
	}
}
