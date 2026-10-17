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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type collationBindingRevisionKey struct{}

// persistedCollationBindingRevision 校验SQL重绑定来源，不能根据当前会话、
// RequiredProtocolVersion或结果列类型猜测历史对象的绑定代际。
func persistedCollationBindingRevision(ctx context.Context, revision uint32) error {
	// 新代际的持久化reader/authoring尚未启用；不把未知marker降级成legacy。
	if revision != uint32(collation.RevisionLegacy) {
		return moerr.NewNotSupportedf(ctx, "persisted collation binding revision %d is disabled", revision)
	}
	return nil
}

// enterPersistedCollationBinding 只作用于当前同步绑定栈。调用方在任何可能
// 返回的操作之前defer restore；嵌套View退出时恢复精确的外层context。
func enterPersistedCollationBinding(ctx CompilerContext, revision uint32) (restore func(), err error) {
	previous := ctx.GetContext()
	if err = persistedCollationBindingRevision(previous, revision); err != nil {
		return nil, err
	}
	parent := previous
	if parent == nil {
		parent = context.Background()
	}
	ctx.SetContext(context.WithValue(parent, collationBindingRevisionKey{}, revision))
	return func() { ctx.SetContext(previous) }, nil
}
