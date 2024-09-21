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
	tracker.Update("a", 1)
	tracker.Update("b", 2)

	// log
	logutil.Info("peak inuse memory", zap.Any("info", tracker))
}
