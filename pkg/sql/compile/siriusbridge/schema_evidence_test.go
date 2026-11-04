// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package siriusbridge

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvidenceRetainsPhysicalWidthAndNamesOnlyInDigest(t *testing.T) {
	column := Column{OID: uint32(types.T_decimal256), Width: 8, Scale: 2, Nullable: true, Name: "private heading"}
	evidence, digest, err := SchemaEvidence([]Column{column})
	require.NoError(t, err)
	require.Equal(t, []ColumnEvidence{{OID: int32(types.T_decimal256), Precision: 8, Scale: 2, Nullable: true}}, evidence)
	column.OID = uint32(types.T_decimal64)
	_, narrowDigest, err := SchemaEvidence([]Column{column})
	require.NoError(t, err)
	require.NotEqual(t, digest, narrowDigest)
	column.Name = "different heading"
	_, renamedDigest, err := SchemaEvidence([]Column{column})
	require.NoError(t, err)
	require.NotEqual(t, narrowDigest, renamedDigest)
	_, _, err = SchemaEvidence(nil)
	require.Error(t, err)
	_, _, err = SchemaEvidence(make([]Column, 1025))
	require.Error(t, err)
	column.Name = strings.Repeat("x", (1<<20)+1)
	_, _, err = SchemaEvidence([]Column{column})
	require.Error(t, err)
}
