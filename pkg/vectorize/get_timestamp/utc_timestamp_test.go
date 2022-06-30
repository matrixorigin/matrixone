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
package get_timestamp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestUTCTimeStamp(t *testing.T) {
	timestampStr := GetUTCTimestamp().String()
	if _, err := types.ParseDatetime(timestampStr, 6); err != nil {
		t.Errorf("GetUTCTimestamp() error = %v\n", err)
	}
}
