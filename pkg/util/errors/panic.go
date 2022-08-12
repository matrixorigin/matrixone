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

package errors

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util"
)

// ReportPanic reports a panic has occurred on the real stderr.
// return error, which already reported.
func ReportPanic(ctx context.Context, r any) error {
	return ReportPanicWithDepth(ctx, r, 3)
}

func ReportPanicWithDepth(ctx context.Context, r any, depth int) error {
	panicErr := PanicAsError(r, depth+1)
	return WithContext(ctx, panicErr)
}

// PanicAsError turns r into an error if it is not one already.
func PanicAsError(r any, depth int) error {
	if err, ok := r.(error); ok {
		return &withStack{err, util.Callers(depth + 1)}
	}
	return &withStack{fmt.Errorf("panic: %v", r), util.Callers(depth + 1)}
}
