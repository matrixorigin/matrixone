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

package m0read

import "context"

// StatementReadView owns an immutable workspace prefix. Its concrete owner is
// the transaction, not a context marker or a remote authorization certificate.
// Each reader holds a separate reference; the compile releases its owner after
// stopping its scopes. This interface never crosses the pipeline wire.
type StatementReadView interface{ Release() }

type statementReadViewKey struct{}
type statementReadViewValue struct{ view StatementReadView }

func WithStatementReadView(ctx context.Context, view StatementReadView) context.Context {
	// A nested/non-M0 compile must mask a parent's view rather than inherit it.
	return context.WithValue(ctx, statementReadViewKey{}, statementReadViewValue{view: view})
}

func GetStatementReadView(ctx context.Context) StatementReadView {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(statementReadViewKey{}).(statementReadViewValue)
	return value.view
}
