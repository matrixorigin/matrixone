// Copyright 2026 Matrix Origin
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

package sidecarflight

import (
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func internalErrorf(format string, args ...any) error {
	var cause error
	if strings.Contains(format, "%w") {
		format = strings.ReplaceAll(format, "%w", "%v")
		for _, arg := range args {
			if err, ok := arg.(error); ok {
				cause = err
				break
			}
		}
	}
	err := error(moerr.NewInternalErrorNoCtxf(format, args...))
	if cause != nil {
		err = errors.Join(err, cause)
	}
	return err
}
