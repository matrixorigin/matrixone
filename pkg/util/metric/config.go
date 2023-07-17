// Copyright 2022 Matrix Origin
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

package metric

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
)

var (
	// full buffer approximately cost (56[Sample struct] + 8[pointer]) x 4096 = 256K
	configRawHistBufLimit     int32 = EnvOrDefaultInt[int32]("MO_METRIC_RAWHIST_BUF_LIMIT", 4096)
	configGatherInterval      int64 = EnvOrDefaultInt[int64]("MO_METRIC_GATHER_INTERVAL", 15000) // 15s
	configStatsGatherInterval int64 = EnvOrDefaultInt[int64]("MO_STATS_GATHER_INTERVAL", 15000)  // 15s
	configExportToProm        int32 = EnvOrDefaultBool("MO_METRIC_EXPORT_TO_PROM", 1)
	configForceReinit         int32 = EnvOrDefaultBool("MO_METRIC_DROP_AND_INIT", 0) // TODO: find a better way to init metrics and remove this one
)

func EnvOrDefaultBool(key string, defaultValue int32) int32 {
	val, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	switch strings.ToLower(val) {
	case "0", "false", "f":
		return 0
	case "1", "true", "t":
		return 1
	default:
		return defaultValue
	}
}

func EnvOrDefaultInt[T int32 | int64](key string, defaultValue T) T {
	val, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	var size int
	switch any(&defaultValue).(type) {
	case int32:
		size = 32
	case int64:
		size = 64
	}
	i, err := strconv.ParseInt(val, 10, size)
	if err != nil {
		return defaultValue
	}
	return T(i)
}
func GetRawHistBufLimit() int32 { return atomic.LoadInt32(&configRawHistBufLimit) }

func EnableExportToProm() bool { return atomic.LoadInt32(&configExportToProm) != 0 }

func GetForceInit() bool { return atomic.LoadInt32(&configForceReinit) != 0 }

func GetGatherInterval() time.Duration {
	return time.Duration(atomic.LoadInt64(&configGatherInterval)) * time.Millisecond
}

func GetStatsGatherInterval() time.Duration {
	return time.Duration(atomic.LoadInt64(&configStatsGatherInterval)) * time.Millisecond
}

// for tests

func SetRawHistBufLimit(new int32) int32 {
	return atomic.SwapInt32(&configRawHistBufLimit, new)
}

func SetExportToProm(new bool) bool {
	var val int32 = 0
	if new {
		val = 1
	}
	return atomic.SwapInt32(&configExportToProm, val) != 0
}

func SetGatherInterval(new time.Duration) time.Duration {
	return time.Duration(atomic.SwapInt64(&configGatherInterval, int64(new/time.Millisecond))) * time.Millisecond
}

func IsFullBatchRawHist(mf *pb.MetricFamily) bool {
	return mf.GetType() == pb.MetricType_RAWHIST && len(mf.Metric[0].RawHist.Samples) >= int(GetRawHistBufLimit())
}
