// Copyright 2024 Matrix Origin
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

package logtail

import (
	"testing"

	"github.com/stretchr/testify/require"

	catalogpkg "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func TestAnnotateLazyCatalogEntryAccountSummaryUsesEntryAccountNotCurrentAccount(t *testing.T) {
	bat := containers.BuildBatch(
		[]string{catalogpkg.SystemDBAttr_AccID},
		[]types.Type{types.T_uint32.ToType()},
		containers.Options{},
	)
	t.Cleanup(func() { bat.Close() })
	bat.Vecs[0].Append(uint32(10001), false)

	entry := api.Entry{
		EntryType:  api.Entry_Insert,
		TableId:    catalogpkg.MO_DATABASE_ID,
		DatabaseId: catalogpkg.MO_CATALOG_ID,
	}

	builder := &TxnLogtailRespBuilder{currentAccID: 0}
	require.NotPanics(t, func() {
		builder.annotateLazyCatalogEntryAccountSummary(&entry, bat)
	})

	accountID, ok := catalogpkg.LazyCatalogEntryAccountSummary(entry)
	require.True(t, ok)
	require.Equal(t, uint32(10001), accountID)
}
