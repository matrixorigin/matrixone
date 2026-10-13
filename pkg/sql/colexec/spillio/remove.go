// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spillio

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// RemoveFileWithRetry uses an uncancelled context because query cancellation
// must not prevent removal of an already-owned spill file. A bounded retry
// handles transient local file-service errors without delaying cleanup forever.
func RemoveFileWithRetry(fs fileservice.MutableFileService, name string) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(5 * time.Millisecond)
		}
		err = fs.RemoveFile(context.Background(), name)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}
	return err
}
