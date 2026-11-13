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

// Package metadata defines the durable ISCP protocol shared by planners and workers.
package metadata

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Job states are the persisted ISCP protocol, shared by planners and workers.
const (
	StateInvalid int8 = iota
	StatePending
	StateRunning
	StateCompleted
	StateError
	StateCanceled
)

func IndexJobName(index string) string { return "index_" + index }

func StateName(state int8, errorCode int64) string {
	switch {
	case state == StateError:
		return "stopped"
	case state == StateCanceled:
		return "canceled"
	case errorCode != 0:
		return "retrying"
	default:
		return "running"
	}
}

// ParseWatermark rejects absent, invalid and uninitialized checkpoints.
func ParseWatermark(value string) (types.TS, bool) {
	physical, logical, ok := strings.Cut(value, "-")
	if !ok {
		return types.TS{}, false
	}
	p, err := strconv.ParseInt(physical, 10, 64)
	if err != nil || p <= 0 || p == math.MaxInt64 {
		return types.TS{}, false
	}
	l, err := strconv.ParseUint(logical, 10, 32)
	if err != nil {
		return types.TS{}, false
	}
	return types.BuildTS(p, uint32(l)), true
}
