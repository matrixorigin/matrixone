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

package spillio

import (
	"io"
	"math"
	"os"
)

// WritePreextended appends an already admitted payload without writing beyond
// EOF. Buffered writes that extend EOF can make XFS speculatively allocate a
// large unwritten tail, consuming disk space that the logical spill reservation
// does not cover. Extending the length first avoids that allocation trigger.
//
// The caller must exclusively own the file, position it at EOF, and open it
// without O_APPEND. Disk admission must precede this call. On error the file may
// contain an extended or partially written tail; the caller must retain its
// reservation until removing the unpublished file, rather than retrying it.
func WritePreextended(file *os.File, payload []byte) (int, error) {
	if file == nil {
		return 0, os.ErrInvalid
	}
	if len(payload) == 0 {
		return 0, nil
	}
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if offset < 0 || int64(len(payload)) > math.MaxInt64-offset {
		return 0, os.ErrInvalid
	}
	if err := file.Truncate(offset + int64(len(payload))); err != nil {
		return 0, err
	}
	return file.Write(payload)
}
