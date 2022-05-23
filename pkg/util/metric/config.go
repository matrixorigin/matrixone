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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/metric/pb"
)

var (
	// full buffer approximately cost (56[Sample struct] + 8[pointer]) x 4096 = 256K
	configRawHistBufLimit int32 = 4096
	configGatherInternal  int64 = 15000 // 15s
	configExportToProm    int32 = 1
)

func getRawHistBufLimit() int32 {
	return atomic.LoadInt32(&configRawHistBufLimit)
}

func getExportToProm() bool {
	return atomic.LoadInt32(&configExportToProm) != 0
}

func getGatherInternal() time.Duration {
	return time.Duration(atomic.LoadInt64(&configGatherInternal)) * time.Millisecond
}

// for tests
func setRawHistBufLimit(new int32) int32 {
	return atomic.SwapInt32(&configRawHistBufLimit, new)
}

func setExportToProm(new bool) bool {
	var val int32 = 0
	if new {
		val = 1
	}
	return atomic.SwapInt32(&configExportToProm, val) != 0
}

func setGatherInternal(new time.Duration) time.Duration {
	return time.Duration(atomic.SwapInt64(&configGatherInternal, int64(new/time.Millisecond))) * time.Millisecond
}

func isFullBatchRawHist(mf *pb.MetricFamily) bool {
	return mf.GetType() == pb.MetricType_RAWHIST && len(mf.Metric[0].RawHist.Samples) >= int(getRawHistBufLimit())
}
