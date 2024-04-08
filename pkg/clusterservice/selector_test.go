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

func TestFilterWithLabel_EQ(t *testing.T) {
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

	assert.False(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1"}, EQ).
			filter("", map[string]metadata.LabelList{
				"l1": {Labels: []string{"v1"}},
				"l2": {Labels: []string{"v2"}}},
			))

	// Test the nil cases.
	assert.True(t, NewSelector().SelectByLabel(nil, EQ).filter("", map[string]metadata.LabelList{}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{}, EQ).filter("", nil))
	assert.True(t, NewSelector().SelectByLabel(nil, EQ).filter("", nil))
	assert.False(t, NewSelector().SelectByLabel(nil, EQ).filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{"li": "v1"}, EQ).filter("", map[string]metadata.LabelList{}))
}

func TestFilterWithLabel_Contain(t *testing.T) {
	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, Contain).
			filter("", nil))

	assert.False(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, Contain).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1"}, Contain).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1"}, Contain).
			filter("", map[string]metadata.LabelList{
				"l1": {Labels: []string{"v1"}},
				"l2": {Labels: []string{"v2"}}},
			))

	// Test the nil cases.
	assert.True(t, NewSelector().SelectByLabel(nil, Contain).filter("", map[string]metadata.LabelList{}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{}, Contain).filter("", nil))
	assert.True(t, NewSelector().SelectByLabel(nil, Contain).filter("", nil))
	assert.False(t, NewSelector().SelectByLabel(nil, Contain).filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{"li": "v1"}, Contain).filter("", map[string]metadata.LabelList{}))
}

func TestFilterWithLabel_EQ_Globbing(t *testing.T) {
	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, EQ_Globbing).
			filter("", nil))

	assert.False(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v2"}, EQ_Globbing).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1"}, EQ_Globbing).
			filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))

	src := "abcdefg"
	yDst := []string{"*", "ab*", "*defg", "a*c*efg"}
	for _, dst := range yDst {
		assert.True(t,
			NewSelector().
				SelectByLabel(map[string]string{"l1": src}, EQ_Globbing).
				filter("", map[string]metadata.LabelList{
					"l1": {Labels: []string{dst}}},
				))
	}

	// Test the nil cases.
	assert.True(t, NewSelector().SelectByLabel(nil, EQ_Globbing).filter("", map[string]metadata.LabelList{}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{}, EQ_Globbing).filter("", nil))
	assert.True(t, NewSelector().SelectByLabel(nil, EQ_Globbing).filter("", nil))
	assert.False(t, NewSelector().SelectByLabel(nil, EQ_Globbing).filter("", map[string]metadata.LabelList{"l1": {Labels: []string{"v1"}}}))
	assert.True(t, NewSelector().SelectByLabel(map[string]string{"li": "v1"}, EQ_Globbing).filter("", map[string]metadata.LabelList{}))
}

func TestSelector_SelectWithoutLabel(t *testing.T) {
	assert.False(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1", "l2": "v2"}, EQ).
			filter("", map[string]metadata.LabelList{
				"l2": {
					Labels: []string{"v2"},
				},
			}))

	assert.True(t,
		NewSelector().
			SelectByLabel(map[string]string{"l1": "v1", "l2": "v2"}, EQ).
			SelectWithoutLabel(map[string]string{"l1": "v1"}).
			filter("", map[string]metadata.LabelList{
				"l2": {
					Labels: []string{"v2"},
				},
			}))
}

func TestLabelNum(t *testing.T) {
	assert.Equal(t, 2, NewSelector().SelectByLabel(map[string]string{
		"a": "a",
		"b": "b",
	}, EQ).LabelNum())
}

func TestGlobbing(t *testing.T) {
	src := "abcdefg"
	yDst := []string{"*", "ab*", "*defg", "a*c*efg"}
	nDst := []string{"*def", "aab*", "*f"}
	for _, dst := range yDst {
		assert.True(t, globbing(src, dst))
	}
	for _, dst := range nDst {
		assert.False(t, globbing(src, dst))
	}
}
