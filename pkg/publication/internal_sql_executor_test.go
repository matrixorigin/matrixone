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

package publication

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestDefChangedBackoff_NormalRetry(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return nil },
		getAttempt:   func() int { return 1 },
	}
	assert.Equal(t, time.Second, b.Next(1))
}

func TestDefChangedBackoff_DefChangedFirstAttempt(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return moerr.NewTxnNeedRetryWithDefChangedNoCtx() },
		getAttempt:   func() int { return 1 },
	}
	assert.Equal(t, 2*time.Second, b.Next(1))
}

func TestDefChangedBackoff_DefChangedLaterAttempt(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return moerr.NewTxnNeedRetryWithDefChangedNoCtx() },
		getAttempt:   func() int { return 2 },
	}
	// attempt > 1, so regular interval
	assert.Equal(t, time.Second, b.Next(2))
}

func TestDefChangedBackoff_AttemptLessThanOne(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return nil },
		getAttempt:   func() int { return 0 },
	}
	// attempt < 1 gets clamped to 1
	assert.Equal(t, time.Second, b.Next(0))
}
