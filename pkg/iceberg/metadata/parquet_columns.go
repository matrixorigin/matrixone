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

package metadata

import (
	"strings"

	"github.com/parquet-go/parquet-go"
)

// ParquetColumnIndexByName returns the first root column matching any name,
// ignoring case and surrounding whitespace.
func ParquetColumnIndexByName(file *parquet.File, names ...string) (int, bool) {
	if file == nil || file.Root() == nil {
		return 0, false
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	for _, col := range file.Root().Columns() {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(col.Name()))]; ok {
			return int(col.Index()), true
		}
	}
	return 0, false
}
