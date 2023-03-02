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

package lockservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestKeeper(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			n := 0
			cc := make(chan struct{})
			s.RegisterMethodHandler(
				pb.Method_Keepalive,
				func(ctx context.Context, r1 *lock.Request, r2 *lock.Response) error {
					n++
					if n == 10 {
						close(cc)
					}
					return nil
				})
			k := NewLockTableKeeper("s1", c, time.Millisecond*10)
			defer func() {
				assert.NoError(t, k.Close())
			}()
			<-cc
		},
	)
}
