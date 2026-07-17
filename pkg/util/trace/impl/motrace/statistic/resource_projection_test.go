// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistic

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
)

func TestFromResourceSummary(t *testing.T) {
	summary := resource.StatementResourceSummary{
		Usage: resource.Usage{
			ExclusiveActiveNS: 100,
			S3ReadBytes:       200,
			S3WriteBytes:      300,
			ClientEgressBytes: 400,
			SpillBytes:        500,
		},
		Memory:            resource.MemoryTotals{MaxDomainPeakLiveBytes: 600},
		AttemptCount:      2,
		OutputPacketCount: 3,
		ConnType:          resource.ConnExternal,
	}
	summary.Usage.WaitNS[resource.WaitLock] = 7
	summary.Usage.WaitNS[resource.WaitFilesystem] = 11
	summary.Usage.S3Requests[resource.S3Head] = 13
	summary.Usage.S3Requests[resource.S3Get] = 17
	summary.Usage.S3Requests[resource.S3Put] = 19
	summary.Usage.S3Requests[resource.S3List] = 23
	summary.Usage.S3Requests[resource.S3Delete] = 29
	summary.Usage.S3Requests[resource.S3DeleteMulti] = 31

	stats := FromResourceSummary(summary, 1.25)
	if stats.GetVersion() != StatsArrayVersion6 || len(stats) != StatsArrayLengthV6 {
		t.Fatalf("unexpected version/length: %v/%d", stats.GetVersion(), len(stats))
	}
	if stats.GetTimeConsumed() != 100 || stats.GetMemorySize() != 600 ||
		stats.GetS3IOInputCount() != 19 || stats.GetS3IOOutputCount() != 30 ||
		stats.GetOutTrafficBytes() != 400 || stats.GetOutPacketCount() != 3 ||
		stats.GetCU() != 1.25 || stats.GetS3IOListCount() != 23 ||
		stats.GetS3IODeleteCount() != 60 || stats.GetS3ReadBytes() != 200 ||
		stats.GetS3WriteBytes() != 300 || stats.GetSpillBytes() != 500 ||
		stats.GetTotalWaitNS() != 18 || stats.GetAttemptCount() != 2 {
		t.Fatalf("unexpected projection: %s", stats.ToJsonString())
	}
	if stats.GetConnType() != float64(ConnTypeExternal) || stats.GetQualityFlags() != 0 {
		t.Fatalf("unexpected metadata: %s", stats.ToJsonString())
	}
}

func TestFromResourceSummaryProjectionOverflow(t *testing.T) {
	summary := resource.StatementResourceSummary{
		Usage: resource.Usage{S3ReadBytes: maxExactFloatInteger + 1},
	}
	stats := FromResourceSummary(summary, 0)
	if stats.GetQualityFlags()&resource.QualityProjectionOverflow == 0 {
		t.Fatalf("overflow not flagged: %s", stats.ToJsonString())
	}
}

func TestHistoricalStatsArrayLengths(t *testing.T) {
	tests := []struct {
		stats *StatsArray
		want  int
	}{
		{NewStatsArrayV1(), StatsArrayLengthV1},
		{NewStatsArrayV2(), StatsArrayLengthV2},
		{NewStatsArrayV3(), StatsArrayLengthV3},
		{NewStatsArrayV4(), StatsArrayLengthV4},
		{NewStatsArrayV5(), StatsArrayLengthV5},
		{NewStatsArrayV6(), StatsArrayLengthV6},
	}
	for _, test := range tests {
		if got := len(test.stats.ToJsonString()); got == 0 {
			t.Fatal("empty JSON")
		}
		if test.stats.GetVersion() == StatsArrayVersion6 && len(*test.stats) != test.want {
			t.Fatalf("v6 backing length: got %d want %d", len(*test.stats), test.want)
		}
	}
}
