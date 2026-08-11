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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

type lineagePublicationLockExec struct {
	backgroundExecTest
	accountID uint32
}

func (e *lineagePublicationLockExec) Exec(ctx context.Context, sql string) error {
	e.accountID, _ = defines.GetAccountId(ctx)
	return e.backgroundExecTest.Exec(ctx, sql)
}

func TestLockDataBranchLineageOwnerPublicationUsesSystemAccount(t *testing.T) {
	bh := &lineagePublicationLockExec{}
	bh.init()
	ctx := defines.AttachAccountId(context.Background(), 42)

	require.NoError(t, lockDataBranchLineageOwnerPublication(ctx, bh))
	require.Equal(t, uint32(catalog.System_Account), bh.accountID)
	require.Equal(t,
		[]string{databranchutils.LineageOwnerPublicationLockSQL()},
		bh.executedSQLs,
	)
}
