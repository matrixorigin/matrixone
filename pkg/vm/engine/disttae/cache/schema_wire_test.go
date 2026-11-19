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

package cache

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSchemaConstraintPrivateHistoricalStream(t *testing.T) {
	original := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: "relkind", Value: "old"}, {Key: "topic", Value: "first"}}},
		&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: "topic", Value: "second"}}},
	}}
	wire, err := original.MarshalBinary()
	require.NoError(t, err)
	canonical := newSchemaConstraint(wire)
	wire[0] ^= 0xff
	first, err := canonical.decode()
	require.NoError(t, err)
	second, err := canonical.decode()
	require.NoError(t, err)
	first.Cts[0].(*engine.StreamConfigsDef).Configs[0].Value = "changed"
	got, err := second.MarshalBinary()
	require.NoError(t, err)
	want, err := original.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, want, got)
	third, err := canonical.decode()
	require.NoError(t, err)
	got, err = third.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, want, got)
}
