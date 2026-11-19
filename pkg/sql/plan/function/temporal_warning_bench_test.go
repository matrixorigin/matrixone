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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type temporalBenchSink struct {
	count uint64
	last  string
	limit int
}

func (s *temporalBenchSink) AppendWarningDiagnostic(_ uint16, msg string) { s.count++; s.last = msg }
func (s *temporalBenchSink) AppendWarningCount(n uint64)                  { s.count += n }
func (s *temporalBenchSink) GetWarningRetentionLimit() int                { return s.limit }

func BenchmarkTemporalWarningConstruction(b *testing.B) {
	for _, size := range []int{3, 4096, 1 << 20} {
		input := []byte(strings.Repeat("x", size))
		for _, mode := range []string{"nil", "count", "retain"} {
			b.Run(mode+"/"+map[int]string{3: "short", 4096: "4k", 1 << 20: "long"}[size], func(b *testing.B) {
				proc := testutil.NewProcess(b)
				defer proc.Free()
				switch mode {
				case "count":
					proc.WarningSink = &temporalBenchSink{}
				case "retain":
					proc.WarningSink = &temporalBenchSink{limit: 1}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					appendStrToDateWarning(proc, moerr.ER_TRUNCATED_WRONG_VALUE, "Incorrect datetime value", "", input)
				}
			})
		}
	}
}

var _ process.WarningDiagnosticCountAppender = (*temporalBenchSink)(nil)
