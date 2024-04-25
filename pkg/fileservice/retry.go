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

package fileservice

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const maxRetryAttemps = 128

func DoWithRetry[T any](
	what string,
	fn func() (T, error),
	maxAttemps int,
	isRetryable func(error) bool,
) (res T, err error) {
	defer catch(&err)
	numRetries := 0
	for {
		res, err = fn()
		if err != nil {
			if isRetryable(err) {
				maxAttemps--

				numRetries++
				if numRetries%5 == 0 {
					logutil.Info("file service retry",
						zap.Any("times", numRetries),
						zap.Any("what", what),
					)
				}

				if maxAttemps <= 0 {
					return
				}
				continue
			}
			return
		}
		return
	}
}
