// Copyright 2024 Matrix Origin
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

package hashtable

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func newAllocator() malloc.Allocator {
	config := malloc.GetDefaultConfig()
	softLimit := uint64(math.MaxUint)
	if config.HashmapSoftLimit != nil {
		softLimit = *config.HashmapSoftLimit
	}
	hardLimit := uint64(math.MaxUint)
	if config.HashmapHardLimit != nil {
		hardLimit = *config.HashmapHardLimit
	}

	var softLimitLoggedAt time.Time
	softLimitLogMinInterval := time.Second * 10

	return malloc.NewInuseTrackingAllocator(
		malloc.GetDefault(nil),
		func(inUse uint64) {

			// soft limit
			if inUse > softLimit && time.Since(softLimitLoggedAt) > softLimitLogMinInterval {
				logutil.Warn("hashmap memory exceed soft limit",
					zap.Any("inuse", inUse),
					zap.Any("soft limit", softLimit),
				)
				softLimitLoggedAt = time.Now()
			}

			// hard limit
			if inUse > hardLimit {
				panic(fmt.Sprintf(
					"hashmap memory exceed hard limit. hard limit %v, inuse %v",
					hardLimit,
					inUse,
				))
			}

		},
	)
}

var defaultAllocator = sync.OnceValue(func() malloc.Allocator {
	return newAllocator()
})
