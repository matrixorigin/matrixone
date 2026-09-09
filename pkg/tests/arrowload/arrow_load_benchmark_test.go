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

package arrowload

import (
	"fmt"
	"testing"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestArrowLoadForceMaterializeFallback(t *testing.T) {
	for _, test := range []struct {
		name             string
		forceMaterialize bool
	}{
		{name: "borrow"},
		{name: "materialize", forceMaterialize: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := startArrowLoadClusterWithOptions(t, arrowLoadClusterOptions{
				cnCount: 1, enabled: true, s3Enabled: true, distributedEnabled: true,
				forceMaterialize: test.forceMaterialize,
			})
			db := openArrowLoadDB(t, c, 0)
			mustExec(t, db, "create database if not exists arrow_materialize")
			mustExec(t, db, "use arrow_materialize")
			mustExec(t, db, "create table load_target(id bigint not null, payload varchar(128) not null)")
			path, _ := fixtureLarge(t)
			borrowedBefore := promtestutil.ToFloat64(
				metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
			)
			copiedBefore := promtestutil.ToFloat64(
				metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
			)

			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table load_target", path))
			require.Equal(t, int64(largeFixtureRows), queryCount(t, db, "select count(*) from load_target"))
			borrowedDelta := promtestutil.ToFloat64(
				metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
			) - borrowedBefore
			copiedDelta := promtestutil.ToFloat64(
				metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
			) - copiedBefore
			if test.forceMaterialize {
				require.Zero(t, borrowedDelta)
				require.Greater(t, copiedDelta, float64(0))
			} else {
				require.Greater(t, borrowedDelta, float64(0))
			}
		})
	}
}

// BenchmarkArrowLoadEndToEndMaterializeAB measures the complete SQL LOAD path
// through the MySQL frontend and embedded storage stack. Table truncation is
// outside the timer; parsing, planning, Arrow I/O/conversion, transaction commit,
// and result acknowledgement remain inside it. The metric assertions prevent a
// benchmark run from silently comparing the same ownership policy twice.
func BenchmarkArrowLoadEndToEndMaterializeAB(b *testing.B) {
	path, ddl := fixtureLarge(b)
	for _, benchmark := range []struct {
		name             string
		forceMaterialize bool
	}{
		{name: "borrow"},
		{name: "materialize", forceMaterialize: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			c := startArrowLoadClusterWithOptions(b, arrowLoadClusterOptions{
				cnCount: 1, enabled: true, s3Enabled: false, distributedEnabled: false,
				forceMaterialize: benchmark.forceMaterialize,
			})
			db := openArrowLoadDB(b, c, 0)
			mustExec(b, db, "create database if not exists arrow_materialize_benchmark")
			mustExec(b, db, "use arrow_materialize_benchmark")
			mustExec(b, db, fmt.Sprintf("create table load_target(%s)", ddl))
			borrowedBefore := promtestutil.ToFloat64(
				metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
			)
			copiedBefore := promtestutil.ToFloat64(
				metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
			)
			loadSQL := fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table load_target", path)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				mustExec(b, db, "truncate table load_target")
				b.StartTimer()
				mustExec(b, db, loadSQL)
			}
			b.StopTimer()
			b.ReportMetric(float64(largeFixtureRows*b.N)/b.Elapsed().Seconds(), "rows/s")
			require.Equal(b, int64(largeFixtureRows), queryCount(b, db, "select count(*) from load_target"))
			borrowedDelta := promtestutil.ToFloat64(
				metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
			) - borrowedBefore
			copiedDelta := promtestutil.ToFloat64(
				metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
			) - copiedBefore
			if benchmark.forceMaterialize {
				require.Zero(b, borrowedDelta)
				require.Greater(b, copiedDelta, float64(0))
			} else {
				require.Greater(b, borrowedDelta, float64(0))
			}
		})
	}
}
