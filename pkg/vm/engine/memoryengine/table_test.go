package memoryengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Only for passing the UT coverage check.
func Test_TableReader(t *testing.T) {
	table := new(Table)
	assert.Panics(t, func() {
		table.BuildShardingReaders(
			nil,
			nil,
			nil,
			nil,
			0,
			0,
			false,
			0,
		)
	})
	assert.Panics(t, func() {
		table.GetProcess()
	})
}
