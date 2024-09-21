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

package malloc

import (
	"encoding/json"
	"runtime/metrics"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestPeakInuseTrackerMarshal(t *testing.T) {
	tracker := NewPeakInuseTracker()

	// json
	_, err := json.Marshal(tracker)
	assert.Nil(t, err)

	// update
	tracker.UpdateMalloc(1)
	tracker.UpdateSession(1)
	tracker.UpdateIO(1)
	tracker.UpdateMemoryCache(1)
	tracker.UpdateHashmap(1)
	samples := []metrics.Sample{
		{
			Name: "/memory/classes/total:bytes",
		},
	}
	metrics.Read(samples)
	for _, sample := range samples {
		tracker.UpdateGoMetrics(sample)
	}

	// log
	logutil.Info("peak inuse memory", zap.Any("info", tracker))
}

func BenchmarkPeakInuseTrackerUpdate(b *testing.B) {
	tracker := NewPeakInuseTracker()
	for i, max := uint64(0), uint64(b.N); i < max; i++ {
		tracker.UpdateMalloc(i)
	}
}

func BenchmarkPeakInuseTrackerNoUpdate(b *testing.B) {
	tracker := NewPeakInuseTracker()
	for range b.N {
		tracker.UpdateMalloc(0)
	}
}
