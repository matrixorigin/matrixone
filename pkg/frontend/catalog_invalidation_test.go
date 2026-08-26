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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func TestCheckTableDefChangeUsesExactCatalogLookup(t *testing.T) {
	catalogCache := cache.NewCatalog()
	query := &cache.TableChangeQuery{
		AccountId:    7,
		DatabaseId:   11,
		DatabaseName: "db",
		Name:         "t",
		TableId:      13,
		Version:      1,
		Ts:           timestamp.Timestamp{PhysicalTime: 100},
	}

	require.Equal(t,
		catalogCache.HasNewerVersion(query),
		CheckTableDefChange(catalogCache, query),
	)
	require.False(t, CheckTableDefChange(nil, query))
}
