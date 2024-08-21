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

package cdc

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	watermarkUpdateInterval = time.Second
)

type WatermarkUpdater struct {
	// watermarkMap saves the watermark of each table
	watermarkMap *sync.Map

	persistFunc func(watermark timestamp.Timestamp) error
}

func NewWatermarkUpdater(persistFunc func(watermark timestamp.Timestamp) error) *WatermarkUpdater {
	return &WatermarkUpdater{
		watermarkMap: &sync.Map{},
		persistFunc:  persistFunc,
	}
}

func (u *WatermarkUpdater) Run(ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ WatermarkUpdater.Run: start\n")
	defer func() {
		u.updateWatermark()
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ WatermarkUpdater.Run: end\n")
	}()

	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		case <-time.After(watermarkUpdateInterval):
			u.updateWatermark()
		}
	}
}

func (u *WatermarkUpdater) UpdateTableWatermark(tableId uint64, watermark timestamp.Timestamp) {
	u.watermarkMap.Store(tableId, watermark)
}

func (u *WatermarkUpdater) RemoveTable(tableId uint64) {
	u.watermarkMap.Delete(tableId)
}

func (u *WatermarkUpdater) updateWatermark() {
	// get min ts of all table
	var watermark timestamp.Timestamp
	u.watermarkMap.Range(func(k, v any) bool {
		ts := v.(timestamp.Timestamp)
		if watermark.IsEmpty() || ts.Less(watermark) {
			watermark = ts
		}
		return true
	})

	// TODO handle error
	_ = u.persistFunc(watermark)
	//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ updateWatermark persisted new watermark: %s\n", watermark.DebugString())
}
