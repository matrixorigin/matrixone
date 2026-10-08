// Copyright 2026 Matrix Origin
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

package tree

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[TableOptionAutoIDCache](
		func() *TableOptionAutoIDCache { return &TableOptionAutoIDCache{} },
		func(t *TableOptionAutoIDCache) { *t = TableOptionAutoIDCache{} },
		reuse.DefaultOptions[TableOptionAutoIDCache](),
	)
}

// TableOptionAutoIDCache controls raw AUTO_INCREMENT reservation size, not the
// session's increment/offset or a global ordering guarantee.
type TableOptionAutoIDCache struct {
	tableOptionImpl
	Value uint64
}

func NewTableOptionAutoIDCache(value uint64) *TableOptionAutoIDCache {
	t := reuse.Alloc[TableOptionAutoIDCache](nil)
	t.Value = value
	return t
}

func (node *TableOptionAutoIDCache) Format(ctx *FmtCtx) {
	ctx.WriteString("auto_id_cache = ")
	ctx.WriteString(strconv.FormatUint(node.Value, 10))
}

func (node TableOptionAutoIDCache) TypeName() string { return "tree.TableOptionAutoIDCache" }

func (node *TableOptionAutoIDCache) Free() {
	reuse.Free[TableOptionAutoIDCache](node, nil)
}
