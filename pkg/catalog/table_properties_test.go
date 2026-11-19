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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAppendStreamConfigProperties(t *testing.T) {
	current := &plan.Property{Key: SystemRelAttr_Comment, Value: "current"}
	first := &plan.Property{Key: "topic", Value: "first"}
	second := &plan.Property{Key: "topic", Value: "second"}
	configs := []*plan.Property{
		{Key: SystemRelAttr_Kind, Value: "old"}, first,
		{Key: "REL_COMMENT", Value: "old"}, second,
		{Key: "REL_CREATESQL", Value: "old"},
	}
	before := make([]*plan.Property, len(configs))
	for i, config := range configs {
		before[i] = &plan.Property{Key: config.Key, Value: config.Value}
	}
	got := AppendStreamConfigProperties([]*plan.Property{current}, configs)
	require.Equal(t, []*plan.Property{current, first, second}, got)
	require.Same(t, first, got[1])
	require.Same(t, second, got[2])
	require.Equal(t, before, configs, "shared persisted configuration must not be rewritten")
	require.Equal(t, []*plan.Property{current}, AppendStreamConfigProperties([]*plan.Property{current}, nil))
	require.Nil(t, AppendStreamConfigProperties(nil, nil))
}
