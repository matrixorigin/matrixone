// Copyright 2023 Matrix Origin
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

package perfcounter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithCounterSetFrom(t *testing.T) {
	var c1, c2 CounterSet
	ctx := WithCounterSet(context.Background(), &c1, &c2)
	ctx2 := WithCounterSetFrom(context.Background(), ctx)
	Update(ctx2, func(set *CounterSet) {
		set.DistTAE.Logtail.Entries.Add(1)
	})
	assert.Equal(t, int64(1), c1.DistTAE.Logtail.Entries.Load())
}
