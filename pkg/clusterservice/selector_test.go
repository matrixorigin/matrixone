// Copyright 2023 Matrix Origin
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

package clusterservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestSelectByServiceID(t *testing.T) {
	assert.Equal(t,
		Selector{byServiceID: true, serviceID: "s1"},
		NewServiceIDSelector("s1"))

	assert.Equal(t,
		Selector{byServiceID: true, serviceID: "s2"},
		NewSelector().SelectByServiceID("s2"))

}

func TestSelectByLabel(t *testing.T) {
	assert.Equal(t,
		Selector{byLabel: true, labels: map[string]string{"l1": "v1"}, labelOp: EQ},
		NewSelector().SelectByLabel(map[string]string{"l1": "v1"}, EQ))
}

func TestFilterWithServiceID(t *testing.T) {
	assert.True(t,
		NewSelector().filter("", nil))

	assert.True(t,
		NewServiceIDSelector("s1").filter("s1", nil))
	assert.False(t,
		NewServiceIDSelector("s2").filter("s1", nil))
}

func TestFilterWithLabel(t *testing.T) {
	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, EQ).
			filter("", nil))

	assert.False(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, EQ).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1"}, EQ).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	// Test the nil cases.
	assert.True(t, NewSelector().SelectByLabel(nil, EQ).filter("", map[string]metadata.LabelList{}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{}, EQ).filter("", nil))
	assert.True(t, NewSelector().SelectByLabel(nil, EQ).filter("", nil))
	assert.False(t, NewSelector().SelectByLabel(nil, EQ).filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{"li": "v1"}, EQ).filter("", map[string]metadata.LabelList{}))
}
