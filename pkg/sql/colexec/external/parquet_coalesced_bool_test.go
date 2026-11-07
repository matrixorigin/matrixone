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

package external

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetCoalescedBoolStatistics(t *testing.T) {
	// These original pyarrow 24.0.0 files contain identical rows; only the
	// first has statistics. SHA256: be3c15e00c7cdc8d5a22d433617c6280b6236a85f9f3106f534064e3f8354400
	// and e10a9b8f3748268cfa546a7afc3b83cb4d1227a74d821a86a3f39be3c9e3e5a9.
	type row struct {
		A int64  `parquet:"a"`
		S string `parquet:"s"`
	}
	for _, name := range []string{"coalesced_bool_statistics.parquet", "coalesced_bool_no_statistics.parquet"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("../../../../test/distributed/resources/parquet", name))
			require.NoError(t, err)
			for _, mode := range []struct {
				name  string
				param *ExternalParam
			}{
				{"default", nil}, {"fanout", &ExternalParam{ExParamConst: ExParamConst{ParquetWholeFileFanout: true}}},
			} {
				t.Run(mode.name, func(t *testing.T) {
					f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquetLoadFileOptions(mode.param)...)
					require.NoError(t, err)
					reader := parquet.NewGenericReader[row](f)
					t.Cleanup(func() { require.NoError(t, reader.Close()) })
					rows := make([]row, 3)
					n, err := reader.Read(rows)
					require.ErrorIs(t, err, io.EOF)
					require.Equal(t, 2, n)
					require.Equal(t, []row{{1, "a"}, {2, strings.Repeat("z", 5000)}}, rows[:n])
				})
			}
			_, err = parquet.OpenFile(bytes.NewReader(data[:len(data)-8]), int64(len(data)-8))
			require.Error(t, err)
		})
	}
}
