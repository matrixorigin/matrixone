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
	"github.com/matrixorigin/matrixone/pkg/defines"
)

type collationBindingRevisionKey struct{}

// WithCollationBindingRevision由已校验marker/floor的内部SQL replay owner使用。
// 不设置session变量，不修改借用的父context；非法revision仍由binder拒绝。
func WithCollationBindingRevision(ctx context.Context, revision uint32) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, collationBindingRevisionKey{}, revision)
}

// explicitCollationBindingRevision 使用当前绑定来源，不从session默认或
// protocol推断。公开入口仍保持legacy，直到原生SQL验证与持久化隔离闭合。
func explicitCollationBindingRevision(ctx context.Context) (uint32, error) {
	if ctx != nil {
		if value := ctx.Value(collationBindingRevisionKey{}); value != nil {
			if revision, ok := value.(uint32); ok && revision <= uint32(collation.RevisionV1) {
				return revision, nil
			}
			return 0, moerr.NewNotSupportedf(ctx, "unsupported collation binding revision %v", value)
		}
	}
	return uint32(collation.RevisionLegacy), nil
}

// authoredCollationRevision仅升级本批支持的canonical/utf8/utf8mb3拼写。
// 0900及DDL-only兼容别名仍是历史映射，不得借用新general-ci语义。
func authoredCollationRevision(ctx context.Context, name string, identity uint32) (uint32, error) {
	revision, err := explicitCollationBindingRevision(ctx)
	if err != nil {
		return 0, err
	}
	definition, known := collation.Lookup(name)
	if known && definition.Semantics == collation.UCA400 {
		return uint32(collation.RevisionV1), nil
	}
	if known && (definition.Semantics == collation.GeneralCI || definition.Semantics == collation.BinPadSpace) &&
		(identity == uint32(collation.UTF8MB4GeneralCIIdentity) || identity == uint32(collation.UTF8MB4BinIdentity)) {
		return revision, nil
	}
	return uint32(collation.RevisionLegacy), nil
}

// persistedCollationBindingRevision 校验SQL重绑定来源，不能根据当前会话、
// RequiredProtocolVersion或结果列类型猜测历史对象的绑定代际。
func persistedCollationBindingRevision(ctx context.Context, revision uint32) error {
	// 未知marker不得降级成legacy；revision1还须通过调用方的read floor。
	if revision > uint32(collation.RevisionV1) {
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
	if revision == uint32(collation.RevisionV1) {
		if err = RequirePersistedProtocolVersion(previous, ctx.GetProcess(), defines.MORPCVersion119); err != nil {
			return nil, err
		}
	}
	parent := previous
	if parent == nil {
		parent = context.Background()
	}
	ctx.SetContext(context.WithValue(parent, collationBindingRevisionKey{}, revision))
	return func() { ctx.SetContext(previous) }, nil
}
