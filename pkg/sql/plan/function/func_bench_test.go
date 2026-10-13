// Copyright 2024 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"testing"
)

func benchmarkDateFormat(b *testing.B, scenario dateFormatScenario) {
	tc := newDateFormatTestCase(b, scenario, 8192)
	proc := testutil.NewProcess(b)
	defer proc.Free()
	fc := NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
	defer fc.Free()
	fc.Benchmark(b)
}

func BenchmarkDateFormat1(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[0]) }

func BenchmarkDateFormat2(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[1]) }

func BenchmarkDateFormat3(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[2]) }

func BenchmarkDateFormat4(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[3]) }

func BenchmarkDateFormat5(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[4]) }

func BenchmarkDateFormat6(b *testing.B) { benchmarkDateFormat(b, dateFormatScenarios[5]) }
