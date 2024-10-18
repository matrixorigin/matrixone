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

package bootstrap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_asyncUpgradeTenantTask(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Expected no panic")
				}
			}()

			var cnt atomic.Int32
			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if cnt.Load() > 0 {
					return executor.Result{}, nil
				}
				cnt.Add(1)
				return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
			})

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					h1 := newTestVersionHandler("1.2.0", "1.1.0", versions.Yes, versions.No, 10)
					h2 := newTestVersionHandler("2.0.0", "1.2.0", versions.Yes, versions.No, 2)
					s.handles = append(s.handles, h1)
					s.handles = append(s.handles, h2)
				},
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
			defer cancel()

			b.asyncUpgradeTenantTask(ctx)
		},
	)
}
