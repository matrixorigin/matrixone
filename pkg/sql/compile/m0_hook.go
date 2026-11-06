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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	activeM0CaptureStatementView       = func(*Compile, *plan.Plan) (m0read.StatementReadView, error) { return nil, nil }
	activeM0CloneCreateContext         = func(c *Compile, _ *plan.CloneTable) (context.Context, error) { return c.proc.Ctx, nil }
	activeM0Enabled                    bool
	activeM0DisableCountNotNullRewrite bool
	activeM0ValidateRemoteReadBinding  = func(context.Context, *Source, *plan.TableDef, timestamp.Timestamp, plan2.LegacyTinyTextTableResolver) error {
		return nil
	}
	activeM0CaptureRemoteReadGeneration = func(client.TxnOperator) (func() error, error) { return func() error { return nil }, nil }
	activeM0WaitRemoteReadSnapshot      = func(context.Context, client.TxnClient, timestamp.Timestamp) error { return nil }
)
