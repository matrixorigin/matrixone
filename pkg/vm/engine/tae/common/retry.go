// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type RetryOp = func() error
type WaitOp = func() (ok bool)

func DoRetry(op RetryOp, ctx context.Context) (err error) {
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		err = op()
		if err == nil {
			break
		}
		runtime.Gosched()
	}
	return
}

func WaitUtil(op WaitOp, ctx context.Context) (err error) {
	var ok bool
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		ok = op()
		if ok {
			break
		}
		runtime.Gosched()
	}
	if !ok {
		err = moerr.NewInternalError(ctx, "timeout")
	}
	return
}
