package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExcludedFilter(t *testing.T) {
	cases := []struct {
		inputs   []*Store
		excluded []string
		expected []*Store
	}{
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a"},
			expected: []*Store{{ID: "b"}, {ID: "c"}},
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b"},
			expected: []*Store{{ID: "c"}},
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b", "c"},
			expected: nil,
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"A", "B", "C"},
			expected: []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
		},
	}

	for _, c := range cases {
		outputs := FilterStore(c.inputs, []IFilter{NewExcludedFilter(c.excluded...)})
		assert.Equal(t, c.expected, outputs)
	}
}
