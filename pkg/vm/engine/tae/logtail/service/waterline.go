// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// Waterliner maintains waterline for all subscribed tables.
type Waterliner struct {
	sync.Mutex

	initialized bool
	clock       func() (timestamp.Timestamp, timestamp.Timestamp)
	waterline   timestamp.Timestamp
}

func NewWaterliner(nowFn func() (timestamp.Timestamp, timestamp.Timestamp)) *Waterliner {
	return &Waterliner{
		initialized: false,
		clock:       nowFn,
	}
}

// Waterline returns waterline for subscribed table.
//
// it would be initialized on its first call.
func (w *Waterliner) Waterline() timestamp.Timestamp {
	w.Lock()
	defer w.Unlock()

	if !w.initialized {
		now, _ := w.clock()
		w.waterline = now
		w.initialized = true
	}

	return w.waterline
}

// Advance updates waterline.
//
// Caller should keep monotonous.
func (w *Waterliner) Advance(update timestamp.Timestamp) {
	w.Lock()
	defer w.Unlock()

	if !w.initialized {
		panic("advance on uninitailized instance")
	}

	if update.Less(w.waterline) {
		panic("timestamp rollback for waterline")
	}

	w.waterline = update
}
