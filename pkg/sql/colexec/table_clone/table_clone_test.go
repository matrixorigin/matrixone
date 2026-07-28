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

package table_clone

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestTableCloneOperatorMetadata(t *testing.T) {
	tc := &TableClone{}
	var buf bytes.Buffer

	require.NotPanics(t, func() {
		vm.String(tc, &buf)
	})
	require.Equal(t, "TableClone", buf.String())
	require.NotEqual(t, vm.Top, tc.OpType())
	require.Equal(t, "TableClone", tc.OpType().String())
}
