package clusterservice

import (
	"testing"

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
		Selector{byLabel: true, labelName: "l1", labelOp: EQ, labelValues: []string{"v1"}},
		NewSelector().SelectByLabel("l1", EQ, []string{"v1"}))
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
	assert.False(t,
		NewSelector().
			SelectByLabel("l1", EQ, []string{"v2"}).
			filter("", nil))

	assert.False(t,
		NewSelector().
			SelectByLabel("l1", EQ, []string{"v2"}).
			filter("", map[string]string{"l1": "v1"}))

	assert.True(t,
		NewSelector().
			SelectByLabel("l1", EQ, []string{"v1"}).
			filter("", map[string]string{"l1": "v1"}))
}
