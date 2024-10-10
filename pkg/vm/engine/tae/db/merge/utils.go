// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func estimateMergeConsume(mobjs []*catalog.ObjectEntry) (origSize, estSize int) {
	if len(mobjs) == 0 {
		return
	}
	rows := 0
	for _, m := range mobjs {
		rows += int(m.Rows())
		origSize += int(m.OriginSize())
	}
	// the main memory consumption is transfer table.
	// each row uses 12B, so estimate size is 12 * rows.
	estSize = rows * 12
	return
}

func entryOutdated(entry *catalog.ObjectEntry, lifetime time.Duration) bool {
	createdAt := entry.CreatedAt.Physical()
	return time.Unix(0, createdAt).Add(lifetime).Before(time.Now())
}
