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

package external

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// convertReaderError converts an error from a file-format library into a mo
// error, except that a cancellation or deadline is returned unchanged even
// when the library wrapped it (parquet-go reports a canceled footer read as
// "reading magic footer of parquet file: context canceled").
//
// When one shard of a parallel load fails, it cancels its siblings, and the
// scheduler reports the failing shard's error by recognising the siblings'
// errors as that cancellation with errors.Is.  moerr.ConvertGoError formats
// the error into an internal error, which loses that identity: whichever
// shard's result was collected first then won, and the load sometimes
// reported a sibling's "context canceled" instead of the real failure.
func convertReaderError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return moerr.ConvertGoError(ctx, err)
}
