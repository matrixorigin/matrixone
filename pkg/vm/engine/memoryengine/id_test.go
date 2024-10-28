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

package memoryengine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestIDGenerator(t *testing.T) {
	ctx := context.Background()
	var last ID
	t0 := time.Now()
	for time.Since(t0) < time.Second*2 {
		for i := 0; i < 100_0000; i++ {
			id, err := RandomIDGenerator.NewID(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// monotonic
			if id <= last {
				t.Fatal()
			}
			last = id
		}
	}
}

var _ hakeeperIDGenerator = new(testHKClient)

type testHKClient struct {
}

func (hkClient *testHKClient) AllocateID(ctx context.Context) (uint64, error) {
	return 0, moerr.NewInternalErrorNoCtx("return error")
}

func (hkClient *testHKClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, moerr.NewInternalErrorNoCtx("return error")
}

func Test_id(t *testing.T) {
	idGen := NewHakeeperIDGenerator(&testHKClient{})
	_, err := idGen.NewID(context.Background())
	assert.Error(t, err)

	_, err = idGen.NewIDByKey(context.Background(), "abc")
	assert.Error(t, err)
}
