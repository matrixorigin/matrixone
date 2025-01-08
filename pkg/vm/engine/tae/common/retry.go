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
	"time"
)

type RetryOp = func() error
type WaitOp = func() (ok bool, err error)

func RetryWithInterval(
	ctx context.Context,
	op WaitOp,
	interval time.Duration,
) (err error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var ok bool
	ok, err = op()
	if ok {
		return
	}

	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case <-ticker.C:
			ok, err = op()
			if ok {
				return
			}
		}
	}
}
