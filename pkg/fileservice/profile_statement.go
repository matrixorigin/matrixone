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

package fileservice

import (
	"os"
	"strconv"
	"time"
)

type ctxKeyStatementProfiler struct{}

var CtxKeyStatementProfiler ctxKeyStatementProfiler

var PerStatementProfileDir = os.Getenv("PER_STMT_PROFILE_DIR")

var PerStatementProfileThreshold = func() time.Duration {
	str := os.Getenv("PER_STMT_PROFILE_THRESHOLD_MSEC")
	if str == "" {
		return time.Millisecond * 500
	}
	n, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return time.Millisecond * time.Duration(n)
}()
