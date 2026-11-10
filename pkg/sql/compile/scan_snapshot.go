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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// scanSnapshotContext 只派生当前扫描的来源上下文，不修改调用方身份。
// 已经打开的 relation 自带读取事务，构建 reader / tombstone 时无需再克隆。
func scanSnapshotContext(ctx context.Context, op client.TxnOperator, snapshot *plan.Snapshot) (context.Context, bool) {
	if snapshot == nil || snapshot.TS == nil || snapshot.TS.IsEmpty() || !snapshot.TS.Less(op.Txn().SnapshotTS) {
		return ctx, false
	}
	if snapshot.Tenant != nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
	}
	return ctx, true
}

// scanSnapshotTxn 将读取点绑定到本次打开的 relation。不同扫描不能复用
// Process 的单槽 clone，也不能修改已交给另一 relation 的事务时间戳。
func scanSnapshotTxn(ctx context.Context, op client.TxnOperator, snapshot *plan.Snapshot) (client.TxnOperator, context.Context) {
	ctx, historical := scanSnapshotContext(ctx, op, snapshot)
	if historical {
		op = op.CloneSnapshotOp(*snapshot.TS)
	}
	return op, ctx
}
