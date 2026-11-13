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
	"github.com/stretchr/testify/require"
)

// BenchmarkJSONReader separates publication from already-published reads.
// Publication includes copies and full admission. Resident is a reader-only
// component measurement, not legacy function, expression or SQL acceptance.
func BenchmarkJSONReader(b *testing.B) {
	for _, count := range []int{16, 4096} {
		raw := jsonAdmissionValue(b, `{"keep":1,"large":[`+strings.Repeat("1,", count-1)+"1]}")
		for _, mode := range []string{"publication", "resident"} {
			b.Run(fmt.Sprintf("elements=%d/%s", count, mode), func(b *testing.B) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(b, mp.CurrNB()); mpool.DeleteMPool(mp) }()
				path := jsonReaderPath(b, "$.keep")
				makeReader := func() JSONReader {
					builder, err := NewJSONReaderBuilder(mp, nil)
					require.NoError(b, err)
					defer builder.Close()
					require.NoError(b, builder.Append(raw, false))
					reader, err := builder.Finish()
					require.NoError(b, err)
					return reader
				}
				reader := makeReader()
				defer reader.Close()
				result, err := reader.Query(0, path)
				require.NoError(b, err)
				text, err := result.Text()
				result.Close()
				require.NoError(b, err)
				require.Equal(b, "1", text.Text)
				text.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					current := reader
					if mode == "publication" {
						current = makeReader()
					}
					value, queryErr := current.Query(0, path)
					if queryErr != nil {
						b.Fatal(queryErr)
					}
					output, textErr := value.Text()
					value.Close()
					if mode == "publication" {
						current.Close()
					}
					got := output.Text
					output.Close()
					if textErr != nil || got != "1" {
						b.Fatal("invalid JSON reader result")
					}
				}
				b.StopTimer()
			})
		}
	}
}
