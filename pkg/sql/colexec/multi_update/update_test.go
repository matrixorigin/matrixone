// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// test simple interface
func TestSimpleInterface(t *testing.T) {
	op := NewArgument()

	_ = op.TypeName()
	require.Equal(t, op.OpType(), vm.MultiUpdate)
	buf := new(bytes.Buffer)
	op.String(buf)

	op.Release()
}

// update single table
func TestUpdateSingleTable(t *testing.T) {
}

// update table with unique key

// update table s3
