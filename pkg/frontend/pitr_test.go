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

package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_checkPitrInValidDurtion(t *testing.T) {
	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "h",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is d", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "d",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is m", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "mo",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is y", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "y",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "h",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(-2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})
}
