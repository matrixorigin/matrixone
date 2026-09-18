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

package function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestParseJSONValueDateRejectsTimeComponent(t *testing.T) {
	for _, input := range []string{
		"2024-01-02 12:34:56",
		"2024-01-02T12:34:56",
	} {
		_, err := parseJSONValueDate(jsonValueExtracted{text: input}, types.T_date.ToType())
		require.Error(t, err, input)
	}

	got, err := parseJSONValueDate(jsonValueExtracted{text: "2024-01-02"}, types.T_date.ToType())
	require.NoError(t, err)
	require.Equal(t, "2024-01-02", got.String())
}

func BenchmarkJSONValueStoredAdmittedLargeDocumentSmallPath(b *testing.B) {
	var builder strings.Builder
	builder.WriteString(`{"keep":1,"large":[`)
	for i := 0; i < 4096; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('1')
	}
	builder.WriteString(`]}`)
	document, err := types.ParseStringToByteJson(builder.String())
	if err != nil {
		b.Fatal(err)
	}
	stored, err := document.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	path := []byte(`$.keep`)
	b.ReportMetric(float64(len(stored)), "bytes/document")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extracted := jsonValueExtract(stored, path, types.T_json)
		if extracted.state != jsonValueOneValue || extracted.text != "1" {
			b.Fatalf("unexpected extraction state=%v text=%q", extracted.state, extracted.text)
		}
	}
}
