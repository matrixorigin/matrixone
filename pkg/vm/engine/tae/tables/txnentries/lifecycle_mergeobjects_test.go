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

package txnentries

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestNewLifecycleRewriteEntryOwnsTransferTableOnValidationFailure(
	t *testing.T,
) {
	transferTable := mergesort.NewTransferTableFromMaps(api.TransferMaps{
		{{ObjIdx: api.NoTransfer}},
	})
	_, err := NewLifecycleRewriteObjectsEntry(
		context.Background(),
		nil,
		"expired-lifecycle-rewrite",
		nil,
		[]*catalog.ObjectEntry{nil},
		[]*catalog.ObjectEntry{nil},
		transferTable,
		types.BuildTS(1, 0),
		time.Now().Add(-time.Second),
		1,
		1,
		1,
		nil,
	)
	require.Error(t, err)
	require.Nil(t, transferTable.Maps)
	require.Nil(t, transferTable.Slab)
}
